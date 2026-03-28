# -*- coding: utf-8 -*-
"""Generate mismatch CSVs for Layer 1: Wiom DB <-> Juspay (Jan 2026)."""
import duckdb

con = duckdb.connect('data.duckdb', read_only=True)

# ============================================================
# CSV 1: WIOM DB -> JUSPAY MISMATCHES (Jan 2026)
# ============================================================

df1 = con.execute("""
    -- A) Booking: status NULL (Juspay SUCCESS but Wiom NULL)
    SELECT
        'wiom_booking_transactions' as wiom_table,
        w.BOOKING_TXN_ID as wiom_txn_id,
        w.CREATED_ON as wiom_date,
        w.BOOKING_FEE as wiom_amount,
        w.RESULTSTATUS as wiom_status,
        w.PAYMENT_FLAG as wiom_payment_flag,
        w.PAYMENT_GATEWAY_NAME as wiom_gateway,
        j.order_id as juspay_order_id,
        j.payment_status as juspay_status,
        j.amount as juspay_amount,
        j.payment_gateway as juspay_gateway,
        j.order_date_created as juspay_date,
        'STATUS_MISMATCH' as mismatch_type,
        'Wiom NULL status but Juspay SUCCESS -- callback failure' as mismatch_reason,
        COALESCE(j.amount, 0) - COALESCE(w.BOOKING_FEE, 0) as amount_diff
    FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE w.CREATED_ON LIKE 'Jan%2026' AND w.RESULTSTATUS IS NULL

    UNION ALL

    -- B) Booking: amount mismatch
    SELECT
        'wiom_booking_transactions',
        w.BOOKING_TXN_ID, w.CREATED_ON, w.BOOKING_FEE, w.RESULTSTATUS,
        w.PAYMENT_FLAG, w.PAYMENT_GATEWAY_NAME,
        j.order_id, j.payment_status, j.amount, j.payment_gateway, j.order_date_created,
        'AMOUNT_MISMATCH',
        'Wiom Rs' || CAST(w.BOOKING_FEE AS VARCHAR) || ' vs Juspay Rs' || CAST(j.amount AS VARCHAR),
        j.amount - w.BOOKING_FEE
    FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE w.CREATED_ON LIKE 'Jan%2026'
    AND w.RESULTSTATUS = 'TXN_SUCCESS' AND j.payment_status = 'SUCCESS'
    AND w.BOOKING_FEE != j.amount

    UNION ALL

    -- C) Primary revenue: amount mismatch
    SELECT
        'wiom_primary_revenue',
        pr.TRANSACTION_ID, pr.RECHARGE_DT, pr.TOTALPAID, NULL,
        pr.MODE, NULL,
        j.order_id, j.payment_status, j.amount, j.payment_gateway, j.order_date_created,
        'AMOUNT_MISMATCH',
        'Wiom TOTALPAID=' || CAST(pr.TOTALPAID AS VARCHAR) || ' vs Juspay Rs' || CAST(j.amount AS VARCHAR),
        j.amount - pr.TOTALPAID
    FROM wiom_primary_revenue pr
    INNER JOIN juspay_transactions j ON pr.TRANSACTION_ID = j.order_id
    WHERE pr.RECHARGE_DT LIKE 'Jan%2026' AND pr.MODE='online'
    AND j.payment_status='SUCCESS' AND pr.TOTALPAID != j.amount

    UNION ALL

    -- D) Primary revenue: unmatched online (not in Juspay)
    SELECT
        'wiom_primary_revenue',
        pr.TRANSACTION_ID, pr.RECHARGE_DT, pr.TOTALPAID, NULL,
        pr.MODE, NULL,
        NULL, NULL, NULL, NULL, NULL,
        'WIOM_ONLY',
        CASE
            WHEN pr.TRANSACTION_ID LIKE 'WIFI_SRVC_%' THEN 'WIFI_SRVC -- routes outside Juspay (partner wallet)'
            WHEN pr.TRANSACTION_ID LIKE 'custGen_%' AND pr.TOTALPAID = 0 THEN 'custGen with TOTALPAID=0 -- failed/cancelled'
            WHEN pr.TRANSACTION_ID LIKE 'BOOKING_PAYMENT%' THEN 'BOOKING_PAYMENT -- booking fee reversal'
            ELSE 'Unmatched online txn -- not found in Juspay'
        END,
        NULL
    FROM wiom_primary_revenue pr
    LEFT JOIN juspay_transactions j ON pr.TRANSACTION_ID = j.order_id
    WHERE pr.RECHARGE_DT LIKE 'Jan%2026' AND pr.MODE='online' AND j.order_id IS NULL

    ORDER BY mismatch_type, wiom_table
""").fetchdf()

df1.to_csv('docs/mismatch_wiom_to_juspay_jan26_v2.csv', index=False)
print(f"CSV 1: mismatch_wiom_to_juspay_jan26.csv")
print(f"  Total rows: {len(df1):,}")
print(f"  By mismatch_type:")
for t, cnt in df1['mismatch_type'].value_counts().items():
    print(f"    {t:25s} {cnt:>6,}")


# ============================================================
# CSV 2: JUSPAY -> WIOM DB MISMATCHES (Jan 2026)
# Now includes wiom_refunded_transactions
# ============================================================

df2 = con.execute("""
    WITH all_wiom_ids AS (
        SELECT BOOKING_TXN_ID as id, 'wiom_booking_transactions' as tbl FROM wiom_booking_transactions
        UNION ALL SELECT TRANSACTION_ID, 'wiom_primary_revenue' FROM wiom_primary_revenue
        UNION ALL SELECT TXN_ID, 'wiom_net_income' FROM wiom_net_income
        UNION ALL SELECT TRANSACTION_ID, 'wiom_topup_income' FROM wiom_topup_income
        UNION ALL SELECT SD_TXN_ID, 'wiom_customer_security_deposit' FROM wiom_customer_security_deposit
        UNION ALL SELECT TRANSACTION_ID, 'wiom_ott_transactions' FROM wiom_ott_transactions
        UNION ALL SELECT TRANSACTION_ID, 'wiom_mobile_recharge_transactions' FROM wiom_mobile_recharge_transactions
        UNION ALL SELECT TRANSACTION_ID, 'wiom_refunded_transactions' FROM wiom_refunded_transactions
    )
    SELECT
        j.order_id as juspay_order_id,
        j.payment_status as juspay_status,
        j.amount as juspay_amount,
        j.payment_gateway as juspay_gateway,
        j.order_date_created as juspay_date,
        j.order_status as juspay_order_status,
        j.customer_id as juspay_customer_id,
        j.description as juspay_description,
        CASE
            WHEN j.order_id LIKE 'custGen_%' THEN 'custGen_*'
            WHEN j.order_id LIKE 'custWgSubs_%' THEN 'custWgSubs_*'
            WHEN j.order_id LIKE 'w_%' THEN 'w_*'
            WHEN j.order_id LIKE 'cusSubs_%' THEN 'cusSubs_*'
            WHEN j.order_id LIKE 'mr_%' THEN 'mr_*'
            WHEN j.order_id LIKE 'cxTeam_%' THEN 'cxTeam_*'
            ELSE 'other'
        END as order_id_pattern,
        'JUSPAY_ONLY' as mismatch_type,
        'Juspay SUCCESS txn not found in any Wiom DB table (incl. refunded)' as mismatch_reason
    FROM juspay_transactions j
    WHERE j.source_month='Jan26' AND j.payment_status='SUCCESS'
    AND j.order_id NOT IN (SELECT id FROM all_wiom_ids)
    ORDER BY order_id_pattern, j.amount DESC
""").fetchdf()

df2.to_csv('docs/mismatch_juspay_to_wiom_jan26_v2.csv', index=False)
print(f"\nCSV 2: mismatch_juspay_to_wiom_jan26.csv")
print(f"  Total rows: {len(df2):,}")
print(f"  By order_id_pattern:")
for t, cnt in df2['order_id_pattern'].value_counts().items():
    amt = df2[df2['order_id_pattern']==t]['juspay_amount'].sum()
    print(f"    {t:20s} {cnt:>6,} txns  Rs {amt:>12,.0f}")
print(f"  By gateway:")
for t, cnt in df2['juspay_gateway'].value_counts().items():
    print(f"    {t:15s} {cnt:>6,}")
print(f"  Total orphan amount: Rs {df2['juspay_amount'].sum():,.0f}")

con.close()
