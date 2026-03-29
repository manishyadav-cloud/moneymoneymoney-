# -*- coding: utf-8 -*-
"""Complete Layer 1 Reconciliation Report - January 2026."""
import duckdb

con = duckdb.connect('data.duckdb', read_only=True)

print('=' * 90)
print('COMPLETE LAYER 1 RECONCILIATION REPORT - JANUARY 2026')
print('=' * 90)

# ============================================================
# TABLE 1: UNIQUE TXNS SUMMARY
# ============================================================
print('\n\n### TABLE 1: UNIQUE TXNS SUMMARY ###\n')

wiom_split = [
    ('wiom_booking_transactions', 'BOOKING_TXN_ID', 'BOOKING_FEE', "RESULTSTATUS='TXN_SUCCESS' AND CREATED_ON LIKE 'Jan%2026'"),
    ('wiom_primary_revenue', 'TRANSACTION_ID', 'TOTALPAID', "MODE='online' AND TOTALPAID > 0 AND RECHARGE_DT LIKE 'Jan%2026'"),
    ('wiom_net_income', 'TXN_ID', 'AMOUNT', "MODE='online' AND YR_MNTH='2026-01'"),
    ('wiom_customer_security_deposit', 'SD_TXN_ID', 'SD_AMOUNT', "CREATED_ON LIKE 'Jan%2026'"),
    ('wiom_mobile_recharge_transactions', 'TRANSACTION_ID', 'CAST(PAY_AMMOUNT AS DOUBLE)', "CAST(CREATEDATE AS VARCHAR) LIKE '2026-01%'"),
    ('wiom_ott_transactions', 'TRANSACTION_ID', 'PAY_AMMOUNT', "CAST(CREATEDATE AS VARCHAR) LIKE '2026-01%'"),
]

wiom_total_txns = 0
wiom_total_amt = 0
for tbl, id_col, amt_col, where in wiom_split:
    r = con.execute(f'SELECT COUNT(DISTINCT "{id_col}"), COALESCE(SUM({amt_col}),0) FROM {tbl} WHERE {where}').fetchone()
    wiom_total_txns += r[0]
    wiom_total_amt += r[1]

juspay = con.execute("""
    SELECT COUNT(DISTINCT order_id), COALESCE(SUM(amount),0)
    FROM juspay_transactions WHERE payment_status='SUCCESS' AND source_month='Jan26'
""").fetchone()

both = con.execute("""
    WITH wiom_ids AS (
        SELECT BOOKING_TXN_ID as id FROM wiom_booking_transactions WHERE RESULTSTATUS='TXN_SUCCESS' AND CREATED_ON LIKE 'Jan%2026'
        UNION SELECT TRANSACTION_ID FROM wiom_primary_revenue WHERE MODE='online' AND TOTALPAID > 0 AND RECHARGE_DT LIKE 'Jan%2026'
        UNION SELECT TXN_ID FROM wiom_net_income WHERE MODE='online' AND YR_MNTH='2026-01'
        UNION SELECT SD_TXN_ID FROM wiom_customer_security_deposit WHERE CREATED_ON LIKE 'Jan%2026'
        UNION SELECT TRANSACTION_ID FROM wiom_mobile_recharge_transactions WHERE CAST(CREATEDATE AS VARCHAR) LIKE '2026-01%'
        UNION SELECT TRANSACTION_ID FROM wiom_ott_transactions WHERE CAST(CREATEDATE AS VARCHAR) LIKE '2026-01%'
    )
    SELECT COUNT(DISTINCT j.order_id)
    FROM juspay_transactions j INNER JOIN wiom_ids w ON j.order_id = w.id
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
""").fetchone()[0]

wiom_only = wiom_total_txns - both

juspay_only = con.execute("""
    WITH all_wiom_ids AS (
        SELECT BOOKING_TXN_ID as id FROM wiom_booking_transactions WHERE CREATED_ON LIKE 'Jan%2026'
        UNION SELECT TRANSACTION_ID FROM wiom_primary_revenue WHERE RECHARGE_DT LIKE 'Jan%2026'
        UNION SELECT TXN_ID FROM wiom_net_income WHERE YR_MNTH='2026-01'
        UNION SELECT SD_TXN_ID FROM wiom_customer_security_deposit WHERE CREATED_ON LIKE 'Jan%2026'
        UNION SELECT TRANSACTION_ID FROM wiom_mobile_recharge_transactions WHERE CAST(CREATEDATE AS VARCHAR) LIKE '2026-01%'
        UNION SELECT TRANSACTION_ID FROM wiom_ott_transactions WHERE CAST(CREATEDATE AS VARCHAR) LIKE '2026-01%'
        UNION SELECT TRANSACTION_ID FROM wiom_refunded_transactions WHERE DT >= '2026-01-01' AND DT < '2026-02-01'
    )
    SELECT COUNT(DISTINCT j.order_id)
    FROM juspay_transactions j
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.order_id NOT IN (SELECT id FROM all_wiom_ids)
""").fetchone()[0]

wiom_refund = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(PAY_AMMOUNT),0)
    FROM wiom_refunded_transactions WHERE DT >= '2026-01-01' AND DT < '2026-02-01'
""").fetchone()

juspay_refund = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(refund_amount),0)
    FROM juspay_refunds jr
    WHERE jr.refund_status='SUCCESS'
    AND jr.order_id IN (SELECT order_id FROM juspay_transactions WHERE source_month='Jan26')
""").fetchone()

print(f'{"Metric":50s} {"Value":>15s}')
print('-' * 70)
print(f'{"Unique Txns - Wiom DB":50s} {wiom_total_txns:>15,}')
print(f'{"Unique Txns - Juspay":50s} {juspay[0]:>15,}')
print(f'{"Unique Txns in BOTH":50s} {both:>15,}')
print(f'{"Unique Txns in Wiom DB only (not in Juspay)":50s} {wiom_only:>15,}')
print(f'{"Unique Txns in Juspay only (not in Wiom)":50s} {juspay_only:>15,}')
print(f'{"Total Refunded Txns - Wiom DB":50s} {wiom_refund[0]:>15,}')
print(f'{"Total Refunded Txns - Juspay":50s} {juspay_refund[0]:>15,}')
print(f'{"Total Amount - Wiom DB":50s} Rs {wiom_total_amt:>12,.0f}')
print(f'{"Total Amount - Juspay":50s} Rs {juspay[1]:>12,.0f}')
print(f'{"Total Refund Amount - Wiom DB":50s} Rs {wiom_refund[1]:>12,.0f}')
print(f'{"Total Refund Amount - Juspay":50s} Rs {juspay_refund[1]:>12,.0f}')


# ============================================================
# TABLE 2: WIOM DB SPLIT
# ============================================================
print('\n\n### TABLE 2: UNIQUE TXNS - WIOM DB SPLIT (Jan 2026) ###\n')
print(f'{"Table":45s} {"ID Column":>20s} {"Unique Txns":>12s}  {"Total Amount":>16s}')
print('=' * 100)
for tbl, id_col, amt_col, where in wiom_split:
    r = con.execute(f'SELECT COUNT(DISTINCT "{id_col}"), COALESCE(SUM({amt_col}),0) FROM {tbl} WHERE {where}').fetchone()
    print(f'{tbl:45s} {id_col:>20s} {r[0]:>12,}  Rs {r[1]:>14,.0f}')
print('=' * 100)
print(f'{"TOTAL (zero overlap between tables)":45s} {"":>20s} {wiom_total_txns:>12,}  Rs {wiom_total_amt:>14,.0f}')


# ============================================================
# TABLE 3: JUSPAY SPLIT BY GATEWAY
# ============================================================
print('\n\n### TABLE 3: UNIQUE TXNS - JUSPAY BY GATEWAY (Jan 2026) ###\n')
print(f'{"Gateway":20s} {"Unique Txns":>12s}  {"Total Amount":>16s}  {"Avg Txn":>10s}  {"Share":>8s}')
print('=' * 75)
r = con.execute("""
    SELECT payment_gateway, COUNT(DISTINCT order_id), COALESCE(SUM(amount),0)
    FROM juspay_transactions WHERE payment_status='SUCCESS' AND source_month='Jan26'
    GROUP BY payment_gateway ORDER BY COUNT(DISTINCT order_id) DESC
""").fetchall()
for x in r:
    avg = x[2]/x[1] if x[1]>0 else 0
    share = 100*x[1]/juspay[0] if juspay[0]>0 else 0
    print(f'{str(x[0]):20s} {x[1]:>12,}  Rs {x[2]:>14,.0f}  Rs {avg:>8,.0f}  {share:>6.1f}%')
print('=' * 75)
print(f'{"TOTAL":20s} {juspay[0]:>12,}  Rs {juspay[1]:>14,.0f}')


# ============================================================
# TABLE 4: JUSPAY SPLIT BY ORDER ID PATTERN
# ============================================================
print('\n\n### TABLE 4: UNIQUE TXNS - JUSPAY BY ORDER ID PATTERN (Jan 2026) ###\n')
print(f'{"Pattern":20s} {"Unique Txns":>12s}  {"Total Amount":>16s}  {"Share":>8s}  {"Maps to Wiom Table":>35s}')
print('=' * 100)
r = con.execute("""
    SELECT
        CASE
            WHEN order_id LIKE 'custGen_%' THEN 'custGen_*'
            WHEN order_id LIKE 'w_%' THEN 'w_*'
            WHEN order_id LIKE 'custWgSubs_%' THEN 'custWgSubs_*'
            WHEN order_id LIKE 'cusSubs_%' THEN 'cusSubs_*'
            WHEN order_id LIKE 'mr_%' THEN 'mr_*'
            WHEN order_id LIKE 'sd_%' THEN 'sd_*'
            WHEN order_id LIKE 'cxTeam_%' THEN 'cxTeam_*'
            ELSE 'other'
        END as pattern,
        COUNT(DISTINCT order_id),
        COALESCE(SUM(amount),0)
    FROM juspay_transactions WHERE payment_status='SUCCESS' AND source_month='Jan26'
    GROUP BY pattern ORDER BY COUNT(DISTINCT order_id) DESC
""").fetchall()
pattern_map = {
    'w_*': 'wiom_net_income',
    'custGen_*': 'primary_revenue / booking',
    'custWgSubs_*': 'wiom_primary_revenue',
    'cusSubs_*': 'wiom_primary_revenue',
    'mr_*': 'mobile_recharge_transactions',
    'sd_*': 'customer_security_deposit',
    'cxTeam_*': '(internal/support)',
    'other': '(mixed)',
}
for x in r:
    share = 100*x[1]/juspay[0] if juspay[0]>0 else 0
    mapped = pattern_map.get(x[0], '?')
    print(f'{x[0]:20s} {x[1]:>12,}  Rs {x[2]:>14,.0f}  {share:>6.1f}%  {mapped:>35s}')

con.close()
