# -*- coding: utf-8 -*-
"""Total money inflow comparison: Wiom DB vs Juspay — Jan 2026."""
import duckdb

con = duckdb.connect('data.duckdb', read_only=True)

print("=" * 85)
print("TOTAL MONEY INFLOW: WIOM DB vs JUSPAY - JAN 2026")
print("=" * 85)

# --- JUSPAY ---
j = con.execute("""
    SELECT COUNT(*) as txns, SUM(amount) as total,
        SUM(CASE WHEN payment_gateway='PAYTM_V2' THEN amount ELSE 0 END) as paytm,
        SUM(CASE WHEN payment_gateway='PHONEPE' THEN amount ELSE 0 END) as phonepe,
        SUM(CASE WHEN payment_gateway='PAYU' THEN amount ELSE 0 END) as payu,
        SUM(CASE WHEN payment_gateway='RAZORPAY' THEN amount ELSE 0 END) as razorpay
    FROM juspay_transactions WHERE source_month='Jan26' AND payment_status='SUCCESS'
""").fetchone()
print(f"\nJUSPAY (SUCCESS):  {j[0]:>10,} txns   Rs {j[1]:>14,.0f}")
print(f"  Paytm:   Rs {j[2]:>14,.0f}")
print(f"  PhonePe: Rs {j[3]:>14,.0f}")
print(f"  PayU:    Rs {j[4]:>14,.0f}")
print(f"  Razorpay:Rs {j[5]:>14,.0f}")

# --- WIOM DB ---
print(f"\nWIOM DB - TABLE BY TABLE (Jan 2026):")

booking = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(BOOKING_FEE),0)
    FROM wiom_booking_transactions WHERE CREATED_ON LIKE 'Jan%2026' AND RESULTSTATUS='TXN_SUCCESS'
""").fetchone()
print(f"  booking_transactions (SUCCESS):       {booking[0]:>8,} txns  Rs {booking[1]:>14,.0f}")

pr_online = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(TOTALPAID),0)
    FROM wiom_primary_revenue WHERE RECHARGE_DT LIKE 'Jan%2026' AND MODE='online' AND TOTALPAID > 0
""").fetchone()
print(f"  primary_revenue (online, >0):         {pr_online[0]:>8,} txns  Rs {pr_online[1]:>14,.0f}")

pr_cash = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(TOTALPAID),0)
    FROM wiom_primary_revenue WHERE RECHARGE_DT LIKE 'Jan%2026' AND MODE='cash' AND TOTALPAID > 0
""").fetchone()
print(f"  primary_revenue (cash, >0):           {pr_cash[0]:>8,} txns  Rs {pr_cash[1]:>14,.0f}")

ni = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(AMOUNT),0)
    FROM wiom_net_income WHERE YR_MNTH='2026-01' AND MODE='online'
""").fetchone()
print(f"  net_income (online):                  {ni[0]:>8,} txns  Rs {ni[1]:>14,.0f}")

sd = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(SD_AMOUNT),0)
    FROM wiom_customer_security_deposit WHERE CREATED_ON LIKE 'Jan%2026'
""").fetchone()
print(f"  customer_security_deposit:            {sd[0]:>8,} txns  Rs {sd[1]:>14,.0f}")

mr = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(CAST(PAY_AMMOUNT AS DOUBLE)),0)
    FROM wiom_mobile_recharge_transactions
    WHERE CAST(CREATEDATE AS VARCHAR) LIKE '2026-01%'
""").fetchone()
print(f"  mobile_recharge_transactions:         {mr[0]:>8,} txns  Rs {mr[1]:>14,.0f}")

ott = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(PAY_AMMOUNT),0)
    FROM wiom_ott_transactions WHERE CAST(CREATEDATE AS VARCHAR) LIKE '2026-01%'
""").fetchone()
print(f"  ott_transactions:                     {ott[0]:>8,} txns  Rs {ott[1]:>14,.0f}")

topup = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(AMOUNT),0)
    FROM wiom_topup_income WHERE DATETIME LIKE '%Jan%2026%'
""").fetchone()
print(f"  topup_income:                         {topup[0]:>8,} txns  Rs {topup[1]:>14,.0f}")

# === OVERLAP CHECK ===
print(f"\n{'='*85}")
print("OVERLAP CHECK - which tables share the same transaction IDs?")
print("=" * 85)

o1 = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(pr.TOTALPAID),0)
    FROM wiom_primary_revenue pr
    INNER JOIN wiom_net_income ni ON pr.TRANSACTION_ID = ni.TXN_ID
    WHERE pr.RECHARGE_DT LIKE 'Jan%2026' AND pr.MODE='online'
""").fetchone()
print(f"\nprimary_revenue ^ net_income:    {o1[0]:>8,} overlapping txns  Rs {o1[1]:>12,.0f} (in PR)")

ni_only = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(ni.AMOUNT),0)
    FROM wiom_net_income ni
    LEFT JOIN wiom_primary_revenue pr ON ni.TXN_ID = pr.TRANSACTION_ID
    WHERE ni.YR_MNTH='2026-01' AND ni.MODE='online' AND pr.TRANSACTION_ID IS NULL
""").fetchone()
print(f"net_income ONLY (not in PR):    {ni_only[0]:>8,} txns              Rs {ni_only[1]:>12,.0f}")

print("\n  net_income-only ID patterns:")
r = con.execute("""
    SELECT
        CASE
            WHEN ni.TXN_ID LIKE 'w_%' THEN 'w_*'
            WHEN ni.TXN_ID LIKE 'custGen_%' THEN 'custGen_*'
            WHEN ni.TXN_ID LIKE 'custWgSubs_%' THEN 'custWgSubs_*'
            ELSE 'other'
        END as pattern, COUNT(*) as cnt, SUM(ni.AMOUNT) as amt
    FROM wiom_net_income ni
    LEFT JOIN wiom_primary_revenue pr ON ni.TXN_ID = pr.TRANSACTION_ID
    WHERE ni.YR_MNTH='2026-01' AND ni.MODE='online' AND pr.TRANSACTION_ID IS NULL
    GROUP BY pattern ORDER BY cnt DESC
""").fetchall()
for x in r:
    print(f"    {x[0]:20s} {x[1]:>8,} txns  Rs {x[2]:>12,.0f}")

pr_only = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(pr.TOTALPAID),0)
    FROM wiom_primary_revenue pr
    LEFT JOIN wiom_net_income ni ON pr.TRANSACTION_ID = ni.TXN_ID
    WHERE pr.RECHARGE_DT LIKE 'Jan%2026' AND pr.MODE='online' AND pr.TOTALPAID > 0 AND ni.TXN_ID IS NULL
""").fetchone()
print(f"\nprimary_revenue ONLY (not in NI):{pr_only[0]:>8,} txns              Rs {pr_only[1]:>12,.0f}")

print("\n  primary_revenue-only ID patterns:")
r = con.execute("""
    SELECT
        CASE
            WHEN pr.TRANSACTION_ID LIKE 'BILL_PAID_%' THEN 'BILL_PAID_*'
            WHEN pr.TRANSACTION_ID LIKE 'custGen_%' THEN 'custGen_*'
            WHEN pr.TRANSACTION_ID LIKE 'custWgSubs_%' THEN 'custWgSubs_*'
            WHEN pr.TRANSACTION_ID LIKE 'WIFI_SRVC_%' THEN 'WIFI_SRVC_*'
            WHEN pr.TRANSACTION_ID LIKE 'BOOKING_PAYMENT_%' THEN 'BOOKING_PAYMENT_*'
            ELSE 'other'
        END as pattern, COUNT(*) as cnt, SUM(pr.TOTALPAID) as amt
    FROM wiom_primary_revenue pr
    LEFT JOIN wiom_net_income ni ON pr.TRANSACTION_ID = ni.TXN_ID
    WHERE pr.RECHARGE_DT LIKE 'Jan%2026' AND pr.MODE='online' AND pr.TOTALPAID > 0 AND ni.TXN_ID IS NULL
    GROUP BY pattern ORDER BY cnt DESC
""").fetchall()
for x in r:
    print(f"    {x[0]:30s} {x[1]:>8,} txns  Rs {x[2]:>12,.0f}")

# === DEDUPLICATED TOTAL ===
print(f"\n{'='*85}")
print("DEDUPLICATED WIOM ONLINE TOTAL (Jan 2026)")
print("=" * 85)

dedup = con.execute("""
    WITH all_online_txns AS (
        SELECT TRANSACTION_ID as id, TOTALPAID as amt, 'primary_revenue' as src
        FROM wiom_primary_revenue
        WHERE RECHARGE_DT LIKE 'Jan%2026' AND MODE='online' AND TOTALPAID > 0

        UNION ALL

        SELECT TXN_ID, AMOUNT, 'net_income'
        FROM wiom_net_income
        WHERE YR_MNTH='2026-01' AND MODE='online'
        AND TXN_ID NOT IN (
            SELECT TRANSACTION_ID FROM wiom_primary_revenue
            WHERE RECHARGE_DT LIKE 'Jan%2026' AND MODE='online'
        )
    )
    SELECT COUNT(*) as txns, SUM(amt) as total FROM all_online_txns
""").fetchone()

print(f"\nOnline recharges + topups (deduplicated): {dedup[0]:>8,} txns  Rs {dedup[1]:>14,.0f}")
print(f"  + Security deposits:                    {sd[0]:>8,} txns  Rs {sd[1]:>14,.0f}")
print(f"  + Mobile recharges:                     {mr[0]:>8,} txns  Rs {mr[1]:>14,.0f}")
print(f"  + OTT transactions:                     {ott[0]:>8,} txns  Rs {ott[1]:>14,.0f}")
wiom_total = dedup[1] + sd[1] + mr[1] + ott[1]
print(f"  ---------------------------------------------------------------")
print(f"  WIOM TOTAL (online, Jan 2026):                    Rs {wiom_total:>14,.0f}")
print(f"  JUSPAY TOTAL (SUCCESS, Jan 2026):                 Rs {j[1]:>14,.0f}")
print(f"  DIFFERENCE (Juspay - Wiom):                       Rs {j[1] - wiom_total:>14,.0f}")

print(f"\n  + Cash transactions (outside Juspay):  {pr_cash[0]:>8,} txns  Rs {pr_cash[1]:>14,.0f}")
print(f"  GRAND TOTAL (online + cash):                      Rs {wiom_total + pr_cash[1]:>14,.0f}")

# === JUSPAY vs WIOM MATCHED AMOUNTS ===
print(f"\n{'='*85}")
print("JUSPAY vs WIOM MATCHED RECORDS - AMOUNT COMPARISON")
print("=" * 85)

# Match juspay to primary_revenue (largest overlap)
r = con.execute("""
    SELECT
        COUNT(*) as matched,
        SUM(j.amount) as juspay_amt,
        SUM(pr.TOTALPAID) as wiom_amt,
        SUM(j.amount) - SUM(pr.TOTALPAID) as diff,
        SUM(CASE WHEN j.amount != pr.TOTALPAID THEN 1 ELSE 0 END) as mismatches
    FROM juspay_transactions j
    INNER JOIN wiom_primary_revenue pr ON j.order_id = pr.TRANSACTION_ID
    WHERE j.source_month='Jan26' AND j.payment_status='SUCCESS'
    AND pr.RECHARGE_DT LIKE 'Jan%2026' AND pr.MODE='online'
""").fetchone()
print(f"\nJuspay <-> primary_revenue (matched):")
print(f"  Records matched:  {r[0]:>10,}")
print(f"  Juspay amount:    Rs {r[1]:>14,.0f}")
print(f"  Wiom PR amount:   Rs {r[2]:>14,.0f}")
print(f"  Difference:       Rs {r[3]:>14,.0f}")
print(f"  Amount mismatches:{r[4]:>10,}")

# Match juspay to net_income
r2 = con.execute("""
    SELECT
        COUNT(*) as matched,
        SUM(j.amount) as juspay_amt,
        SUM(ni.AMOUNT) as wiom_amt,
        SUM(j.amount) - SUM(ni.AMOUNT) as diff,
        SUM(CASE WHEN j.amount != ni.AMOUNT THEN 1 ELSE 0 END) as mismatches
    FROM juspay_transactions j
    INNER JOIN wiom_net_income ni ON j.order_id = ni.TXN_ID
    WHERE j.source_month='Jan26' AND j.payment_status='SUCCESS'
    AND ni.YR_MNTH='2026-01' AND ni.MODE='online'
""").fetchone()
print(f"\nJuspay <-> net_income (matched):")
print(f"  Records matched:  {r2[0]:>10,}")
print(f"  Juspay amount:    Rs {r2[1]:>14,.0f}")
print(f"  Wiom NI amount:   Rs {r2[2]:>14,.0f}")
print(f"  Difference:       Rs {r2[3]:>14,.0f}")
print(f"  Amount mismatches:{r2[4]:>10,}")

con.close()
