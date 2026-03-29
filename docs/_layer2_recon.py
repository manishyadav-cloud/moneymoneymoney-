# -*- coding: utf-8 -*-
"""Layer 2 Reconciliation: Juspay <-> PG Gateways (Jan 2026)."""
import duckdb

con = duckdb.connect('data.duckdb', read_only=True)

print('=' * 100)
print('LAYER 2 RECONCILIATION REPORT — JUSPAY vs PG GATEWAYS (JANUARY 2026)')
print('=' * 100)

# ============================================================
# OVERVIEW: Juspay Jan 2026 SUCCESS txns by gateway
# ============================================================
print('\n### OVERVIEW: JUSPAY Jan 2026 SUCCESS txns by gateway ###\n')
juspay_by_gw = con.execute("""
    SELECT payment_gateway, COUNT(DISTINCT order_id) as txns, COALESCE(SUM(amount),0) as amount
    FROM juspay_transactions
    WHERE payment_status='SUCCESS' AND source_month='Jan26'
    GROUP BY payment_gateway ORDER BY txns DESC
""").fetchall()
# Build lookup dict
gw_map = {gw: (txns, amt) for gw, txns, amt in juspay_by_gw}

print(f'{"Gateway":15s} {"Juspay Txns":>12s}  {"Juspay Amount":>16s}')
print('-' * 50)
for gw, txns, amt in juspay_by_gw:
    print(f'{str(gw):15s} {txns:>12,}  Rs {amt:>12,.0f}')

# ============================================================
# TABLE 1: PAYTM MATCH (gateway=PAYTM_V2)
# NOTE: Paytm embeds single quotes in all string columns
#       Order_ID in paytm = 'wiom-...-1'  (quotes included in string)
#       Join: TRIM(juspay_txn_id, chr(39)) = TRIM(Order_ID, chr(39))
# ============================================================
print('\n\n### TABLE 1: PAYTM — Juspay vs paytm_transactions ###\n')
print('  Note: Paytm Order_ID has embedded single quotes — using TRIM(col, chr(39)) for join')
print(f'  Juspay gateway name: PAYTM_V2\n')

j_paytm_count, j_paytm_amt = gw_map.get('PAYTM_V2', (0, 0))

paytm_matched = con.execute("""
    SELECT COUNT(DISTINCT j.order_id),
           COALESCE(SUM(j.amount), 0),
           COALESCE(SUM(p.Amount), 0)
    FROM juspay_transactions j
    INNER JOIN paytm_transactions p
        ON TRIM(j.juspay_txn_id, chr(39)) = TRIM(p.Order_ID, chr(39))
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='PAYTM_V2'
""").fetchone()

paytm_juspay_only = con.execute("""
    SELECT COUNT(DISTINCT j.order_id), COALESCE(SUM(j.amount),0)
    FROM juspay_transactions j
    LEFT JOIN paytm_transactions p
        ON TRIM(j.juspay_txn_id, chr(39)) = TRIM(p.Order_ID, chr(39))
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='PAYTM_V2'
    AND p.Order_ID IS NULL
""").fetchone()

paytm_pg_jan = con.execute("""
    SELECT COUNT(DISTINCT Order_ID), COALESCE(SUM(Amount),0)
    FROM paytm_transactions
    WHERE TRIM(Transaction_Date, chr(39)) LIKE '2026-01%'
""").fetchone()

paytm_pg_only = con.execute("""
    SELECT COUNT(DISTINCT p.Order_ID), COALESCE(SUM(p.Amount),0)
    FROM paytm_transactions p
    LEFT JOIN juspay_transactions j
        ON TRIM(p.Order_ID, chr(39)) = TRIM(j.juspay_txn_id, chr(39))
        AND j.payment_status='SUCCESS' AND j.source_month='Jan26'
    WHERE TRIM(p.Transaction_Date, chr(39)) LIKE '2026-01%'
    AND j.order_id IS NULL
""").fetchone()

match_pct_ptm = 100 * paytm_matched[0] / j_paytm_count if j_paytm_count else 0
amt_diff_ptm = paytm_matched[1] - paytm_matched[2]

print(f'{"Metric":55s} {"Txns":>10s}  {"Amount":>16s}')
print('-' * 85)
print(f'{"Juspay SUCCESS (PAYTM_V2 gateway, Jan26)":55s} {j_paytm_count:>10,}  Rs {j_paytm_amt:>12,.0f}')
print(f'{"Paytm PG table (Jan26, all = SUCCESS)":55s} {paytm_pg_jan[0]:>10,}  Rs {paytm_pg_jan[1]:>12,.0f}')
print(f'{"Matched (in both)":55s} {paytm_matched[0]:>10,}  Juspay Rs {paytm_matched[1]:>10,.0f}')
print(f'{"  Paytm PG amount for matched txns":55s} {"":>10s}  PG Rs     {paytm_matched[2]:>10,.0f}')
print(f'{"  Amount diff (Juspay amt - PG amt)":55s} {"":>10s}  Rs {amt_diff_ptm:>12,.0f}')
print(f'{"Match rate (Juspay matched / Juspay total)":55s} {match_pct_ptm:>9.1f}%')
print(f'{"Juspay only (no match in Paytm PG)":55s} {paytm_juspay_only[0]:>10,}  Rs {paytm_juspay_only[1]:>12,.0f}')
print(f'{"Paytm PG only (Jan26, not in Juspay Jan26)":55s} {paytm_pg_only[0]:>10,}  Rs {paytm_pg_only[1]:>12,.0f}')


# ============================================================
# TABLE 2: PHONEPE MATCH (gateway=PHONEPE)
#   Join: juspay.juspay_txn_id = phonepe."Merchant Order Id"
# ============================================================
print('\n\n### TABLE 2: PHONEPE — Juspay vs phonepe_transactions ###\n')
print(f'  Join key: juspay.juspay_txn_id = phonepe_transactions."Merchant Order Id"\n')

j_phonepe_count, j_phonepe_amt = gw_map.get('PHONEPE', (0, 0))

phonepe_matched = con.execute("""
    SELECT COUNT(DISTINCT j.order_id),
           COALESCE(SUM(j.amount), 0),
           COALESCE(SUM(CAST(p."Transaction Amount" AS DOUBLE)), 0)
    FROM juspay_transactions j
    INNER JOIN phonepe_transactions p
        ON j.juspay_txn_id = p."Merchant Order Id"
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='PHONEPE'
""").fetchone()

phonepe_juspay_only = con.execute("""
    SELECT COUNT(DISTINCT j.order_id), COALESCE(SUM(j.amount),0)
    FROM juspay_transactions j
    LEFT JOIN phonepe_transactions p
        ON j.juspay_txn_id = p."Merchant Order Id"
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='PHONEPE'
    AND p."Merchant Order Id" IS NULL
""").fetchone()

phonepe_pg_jan = con.execute("""
    SELECT COUNT(DISTINCT "Merchant Order Id"),
           COALESCE(SUM(CAST("Transaction Amount" AS DOUBLE)),0)
    FROM phonepe_transactions
    WHERE CAST("Transaction Date" AS VARCHAR) LIKE '2026-01%'
    AND "Transaction Status" = 'SUCCESS'
""").fetchone()

phonepe_pg_only = con.execute("""
    SELECT COUNT(DISTINCT p."Merchant Order Id"),
           COALESCE(SUM(CAST(p."Transaction Amount" AS DOUBLE)),0)
    FROM phonepe_transactions p
    LEFT JOIN juspay_transactions j
        ON p."Merchant Order Id" = j.juspay_txn_id
        AND j.payment_status='SUCCESS' AND j.source_month='Jan26'
    WHERE CAST(p."Transaction Date" AS VARCHAR) LIKE '2026-01%'
    AND p."Transaction Status" = 'SUCCESS'
    AND j.order_id IS NULL
""").fetchone()

match_pct_pp = 100 * phonepe_matched[0] / j_phonepe_count if j_phonepe_count else 0
amt_diff_pp = phonepe_matched[1] - phonepe_matched[2]

print(f'{"Metric":55s} {"Txns":>10s}  {"Amount":>16s}')
print('-' * 85)
print(f'{"Juspay SUCCESS (PHONEPE gateway, Jan26)":55s} {j_phonepe_count:>10,}  Rs {j_phonepe_amt:>12,.0f}')
print(f'{"PhonePe PG table (Jan26, SUCCESS)":55s} {phonepe_pg_jan[0]:>10,}  Rs {phonepe_pg_jan[1]:>12,.0f}')
print(f'{"Matched (in both)":55s} {phonepe_matched[0]:>10,}  Juspay Rs {phonepe_matched[1]:>10,.0f}')
print(f'{"  PhonePe PG amount for matched txns":55s} {"":>10s}  PG Rs     {phonepe_matched[2]:>10,.0f}')
print(f'{"  Amount diff (Juspay amt - PG amt)":55s} {"":>10s}  Rs {amt_diff_pp:>12,.0f}')
print(f'{"Match rate":55s} {match_pct_pp:>9.1f}%')
print(f'{"Juspay only (no match in PhonePe PG)":55s} {phonepe_juspay_only[0]:>10,}  Rs {phonepe_juspay_only[1]:>12,.0f}')
print(f'{"PhonePe PG only (Jan26, not in Juspay Jan26)":55s} {phonepe_pg_only[0]:>10,}  Rs {phonepe_pg_only[1]:>12,.0f}')


# ============================================================
# TABLE 3: PAYU MATCH (gateway=PAYU)
#   Join: juspay.juspay_txn_id = payu.txnid
#   PayU addedon format: 'DD-MM-YYYY HH:MM'  →  Jan 2026 = '%-01-2026%'
#   PayU success status: 'captured'
# ============================================================
print('\n\n### TABLE 3: PAYU — Juspay vs payu_transactions ###\n')
print(f'  Join key: juspay.juspay_txn_id = payu_transactions.txnid')
print(f'  PayU date format: DD-MM-YYYY. Success status = "captured"\n')

j_payu_count, j_payu_amt = gw_map.get('PAYU', (0, 0))

payu_matched = con.execute("""
    SELECT COUNT(DISTINCT j.order_id),
           COALESCE(SUM(j.amount), 0),
           COALESCE(SUM(CAST(p.amount AS DOUBLE)), 0)
    FROM juspay_transactions j
    INNER JOIN payu_transactions p
        ON j.juspay_txn_id = p.txnid
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='PAYU'
""").fetchone()

payu_juspay_only = con.execute("""
    SELECT COUNT(DISTINCT j.order_id), COALESCE(SUM(j.amount),0)
    FROM juspay_transactions j
    LEFT JOIN payu_transactions p
        ON j.juspay_txn_id = p.txnid
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='PAYU'
    AND p.txnid IS NULL
""").fetchone()

payu_pg_jan = con.execute("""
    SELECT COUNT(DISTINCT txnid), COALESCE(SUM(CAST(amount AS DOUBLE)),0)
    FROM payu_transactions
    WHERE addedon LIKE '%-01-2026%'
    AND status = 'captured'
""").fetchone()

payu_pg_only = con.execute("""
    SELECT COUNT(DISTINCT p.txnid), COALESCE(SUM(CAST(p.amount AS DOUBLE)),0)
    FROM payu_transactions p
    LEFT JOIN juspay_transactions j
        ON p.txnid = j.juspay_txn_id
        AND j.payment_status='SUCCESS' AND j.source_month='Jan26'
    WHERE p.addedon LIKE '%-01-2026%'
    AND p.status = 'captured'
    AND j.order_id IS NULL
""").fetchone()

match_pct_payu = 100 * payu_matched[0] / j_payu_count if j_payu_count else 0
amt_diff_payu = payu_matched[1] - payu_matched[2]

print(f'{"Metric":55s} {"Txns":>10s}  {"Amount":>16s}')
print('-' * 85)
print(f'{"Juspay SUCCESS (PAYU gateway, Jan26)":55s} {j_payu_count:>10,}  Rs {j_payu_amt:>12,.0f}')
print(f'{"PayU PG table (Jan26, captured)":55s} {payu_pg_jan[0]:>10,}  Rs {payu_pg_jan[1]:>12,.0f}')
print(f'{"Matched (in both)":55s} {payu_matched[0]:>10,}  Juspay Rs {payu_matched[1]:>10,.0f}')
print(f'{"  PayU PG amount for matched txns":55s} {"":>10s}  PG Rs     {payu_matched[2]:>10,.0f}')
print(f'{"  Amount diff (Juspay amt - PG amt)":55s} {"":>10s}  Rs {amt_diff_payu:>12,.0f}')
print(f'{"Match rate":55s} {match_pct_payu:>9.1f}%')
print(f'{"Juspay only (no match in PayU PG)":55s} {payu_juspay_only[0]:>10,}  Rs {payu_juspay_only[1]:>12,.0f}')
print(f'{"PayU PG only (Jan26, not in Juspay Jan26)":55s} {payu_pg_only[0]:>10,}  Rs {payu_pg_only[1]:>12,.0f}')


# ============================================================
# TABLE 4: RAZORPAY MATCH (gateway=RAZORPAY)
#   Join: juspay.order_id = razorpay.order_receipt  (different key!)
#   Razorpay amount is already in rupees
#   Filter for payments only: type='payment'
# ============================================================
print('\n\n### TABLE 4: RAZORPAY — Juspay vs razorpay_transactions ###\n')
print(f'  Join key: juspay.order_id = razorpay_transactions.order_receipt  (NOTE: different from other PGs!)')
print(f'  Razorpay filter: type=\'payment\', amount in rupees (not paisa)\n')

j_rzp_count, j_rzp_amt = gw_map.get('RAZORPAY', (0, 0))

razorpay_matched = con.execute("""
    SELECT COUNT(DISTINCT j.order_id),
           COALESCE(SUM(j.amount), 0),
           COALESCE(SUM(r.amount), 0)
    FROM juspay_transactions j
    INNER JOIN razorpay_transactions r
        ON j.order_id = r.order_receipt
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='RAZORPAY'
    AND r.type='payment'
""").fetchone()

razorpay_juspay_only = con.execute("""
    SELECT COUNT(DISTINCT j.order_id), COALESCE(SUM(j.amount),0)
    FROM juspay_transactions j
    LEFT JOIN razorpay_transactions r
        ON j.order_id = r.order_receipt AND r.type='payment'
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='RAZORPAY'
    AND r.order_receipt IS NULL
""").fetchone()

razorpay_pg_jan = con.execute("""
    SELECT COUNT(DISTINCT order_receipt), COALESCE(SUM(amount),0)
    FROM razorpay_transactions
    WHERE type='payment'
    AND created_at >= '2026-01-01' AND created_at < '2026-02-01'
""").fetchone()

razorpay_pg_only = con.execute("""
    SELECT COUNT(DISTINCT r.order_receipt), COALESCE(SUM(r.amount),0)
    FROM razorpay_transactions r
    LEFT JOIN juspay_transactions j
        ON r.order_receipt = j.order_id
        AND j.payment_status='SUCCESS' AND j.source_month='Jan26'
    WHERE r.type='payment'
    AND r.created_at >= '2026-01-01' AND r.created_at < '2026-02-01'
    AND j.order_id IS NULL
""").fetchone()

match_pct_rzp = 100 * razorpay_matched[0] / j_rzp_count if j_rzp_count else 0
amt_diff_rzp = razorpay_matched[1] - razorpay_matched[2]

print(f'{"Metric":55s} {"Txns":>10s}  {"Amount":>16s}')
print('-' * 85)
print(f'{"Juspay SUCCESS (RAZORPAY gateway, Jan26)":55s} {j_rzp_count:>10,}  Rs {j_rzp_amt:>12,.0f}')
print(f'{"Razorpay PG table (Jan26, type=payment)":55s} {razorpay_pg_jan[0]:>10,}  Rs {razorpay_pg_jan[1]:>12,.0f}')
print(f'{"Matched (in both)":55s} {razorpay_matched[0]:>10,}  Juspay Rs {razorpay_matched[1]:>10,.0f}')
print(f'{"  Razorpay PG amount for matched txns":55s} {"":>10s}  PG Rs     {razorpay_matched[2]:>10,.0f}')
print(f'{"  Amount diff (Juspay amt - PG amt)":55s} {"":>10s}  Rs {amt_diff_rzp:>12,.0f}')
print(f'{"Match rate":55s} {match_pct_rzp:>9.1f}%')
print(f'{"Juspay only (no match in Razorpay PG)":55s} {razorpay_juspay_only[0]:>10,}  Rs {razorpay_juspay_only[1]:>12,.0f}')
print(f'{"Razorpay PG only (Jan26, not in Juspay Jan26)":55s} {razorpay_pg_only[0]:>10,}  Rs {razorpay_pg_only[1]:>12,.0f}')


# ============================================================
# TABLE 5: SUMMARY ACROSS ALL GATEWAYS
# ============================================================
print('\n\n### TABLE 5: LAYER 2 SUMMARY — All Gateways (Jan 2026) ###\n')
print(f'{"Gateway":12s} {"Juspay":>10s} {"PG Txns":>9s} {"Matched":>9s} {"Match%":>7s} {"Juspay Amt":>14s} {"PG Amt":>14s} {"Diff":>12s} {"J-Only":>8s} {"J-Only Amt":>14s}')
print('=' * 125)

summary_rows = [
    ('PAYTM_V2',  j_paytm_count,   paytm_pg_jan[0],    paytm_matched[0],    j_paytm_amt,   paytm_matched[2],    paytm_juspay_only[0],    paytm_juspay_only[1]),
    ('PHONEPE',   j_phonepe_count, phonepe_pg_jan[0],  phonepe_matched[0],  j_phonepe_amt, phonepe_matched[2],  phonepe_juspay_only[0],  phonepe_juspay_only[1]),
    ('PAYU',      j_payu_count,    payu_pg_jan[0],     payu_matched[0],     j_payu_amt,    payu_matched[2],     payu_juspay_only[0],     payu_juspay_only[1]),
    ('RAZORPAY',  j_rzp_count,     razorpay_pg_jan[0], razorpay_matched[0], j_rzp_amt,     razorpay_matched[2], razorpay_juspay_only[0], razorpay_juspay_only[1]),
]

t_j = t_pg = t_m = t_j_amt = t_pg_amt = t_jo = t_jo_amt = 0
for gw, j_cnt, pg_cnt, m_cnt, j_amt, pg_amt, jo_cnt, jo_amt in summary_rows:
    mp = 100 * m_cnt / j_cnt if j_cnt else 0
    diff = j_amt - pg_amt
    print(f'{gw:12s} {j_cnt:>10,} {pg_cnt:>9,} {m_cnt:>9,} {mp:>6.1f}% Rs {j_amt:>10,.0f} Rs {pg_amt:>10,.0f} Rs {diff:>8,.0f} {jo_cnt:>8,} Rs {jo_amt:>10,.0f}')
    t_j += j_cnt; t_pg += pg_cnt; t_m += m_cnt
    t_j_amt += j_amt; t_pg_amt += pg_amt; t_jo += jo_cnt; t_jo_amt += jo_amt

print('=' * 125)
t_mp = 100 * t_m / t_j if t_j else 0
t_diff = t_j_amt - t_pg_amt
print(f'{"TOTAL":12s} {t_j:>10,} {t_pg:>9,} {t_m:>9,} {t_mp:>6.1f}% Rs {t_j_amt:>10,.0f} Rs {t_pg_amt:>10,.0f} Rs {t_diff:>8,.0f} {t_jo:>8,} Rs {t_jo_amt:>10,.0f}')


# ============================================================
# TABLE 6: REFUND RECONCILIATION
# ============================================================
print('\n\n### TABLE 6: REFUND RECONCILIATION — Juspay vs PG Refunds (Jan 2026 orders) ###\n')

jr_by_gw = con.execute("""
    SELECT payment_gateway, COUNT(*), COALESCE(SUM(refund_amount),0)
    FROM juspay_refunds jr
    WHERE jr.refund_status='SUCCESS'
    AND jr.order_id IN (SELECT order_id FROM juspay_transactions WHERE source_month='Jan26')
    GROUP BY payment_gateway ORDER BY COUNT(*) DESC
""").fetchall()

jr_total = sum(x[1] for x in jr_by_gw)
jr_amt_total = sum(x[2] for x in jr_by_gw)

print(f'{"Juspay Refunds by Gateway (Jan26 orders)":40s} {"Count":>8s}  {"Amount":>16s}')
print('-' * 70)
for gw, cnt, amt in jr_by_gw:
    print(f'  {str(gw):38s} {cnt:>8,}  Rs {amt:>12,.0f}')
print(f'  {"TOTAL":38s} {jr_total:>8,}  Rs {jr_amt_total:>12,.0f}')

# PhonePe refunds — column: "Transaction Date", "Total Refund Amount"
phonepe_ref_jan = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(CAST("Total Refund Amount" AS DOUBLE)),0)
    FROM phonepe_refunds
    WHERE CAST("Transaction Date" AS VARCHAR) LIKE '2026-01%'
""").fetchone()

# Paytm refunds for Jan 2026 — Transaction_Date has embedded quotes
paytm_ref_jan = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(Amount),0)
    FROM paytm_refunds
    WHERE TRIM(Transaction_Date, chr(39)) LIKE '2026-01%'
""").fetchone()

# Matched: Juspay refunds vs PhonePe refunds (Jan26 orders)
# juspay_refunds.refund_txn_id = 'wiom-...-1' = phonepe_refunds."Merchant Order Id"
pp_ref_matched = con.execute("""
    SELECT COUNT(DISTINCT jr.refund_unique_id), COALESCE(SUM(jr.refund_amount),0)
    FROM juspay_refunds jr
    INNER JOIN phonepe_refunds pr
        ON jr.refund_txn_id = pr."Merchant Order Id"
    WHERE jr.refund_status='SUCCESS'
    AND jr.order_id IN (SELECT order_id FROM juspay_transactions WHERE source_month='Jan26')
    AND jr.payment_gateway='PHONEPE'
""").fetchone()

# Matched: Juspay refunds vs Paytm refunds (Jan26 orders)
ptm_ref_matched = con.execute("""
    SELECT COUNT(DISTINCT jr.refund_unique_id), COALESCE(SUM(jr.refund_amount),0)
    FROM juspay_refunds jr
    INNER JOIN paytm_refunds pr
        ON TRIM(jr.refund_txn_id, chr(39)) = TRIM(pr.Order_ID, chr(39))
    WHERE jr.refund_status='SUCCESS'
    AND jr.order_id IN (SELECT order_id FROM juspay_transactions WHERE source_month='Jan26')
    AND jr.payment_gateway='PAYTM_V2'
""").fetchone()

print(f'\n{"Source":55s} {"Count":>8s}  {"Amount":>16s}')
print('-' * 85)
print(f'{"Juspay refunds (Jan26 orders, SUCCESS, all gateways)":55s} {jr_total:>8,}  Rs {jr_amt_total:>12,.0f}')
print(f'{"PhonePe refunds table (Jan26 txn date)":55s} {phonepe_ref_jan[0]:>8,}  Rs {phonepe_ref_jan[1]:>12,.0f}')
print(f'{"Paytm refunds table (Jan26 txn date)":55s} {paytm_ref_jan[0]:>8,}  Rs {paytm_ref_jan[1]:>12,.0f}')
print(f'{"PayU refunds (no separate table, in juspay_refunds)":55s} {"N/A":>8s}  {"N/A":>16s}')
print(f'{"Razorpay refunds (no separate table, in juspay_refunds)":55s} {"N/A":>8s}  {"N/A":>16s}')
print(f'\n{"PhonePe refunds matched (Juspay vs PG table)":55s} {pp_ref_matched[0]:>8,}  Rs {pp_ref_matched[1]:>12,.0f}')
print(f'{"Paytm refunds matched (Juspay vs PG table)":55s} {ptm_ref_matched[0]:>8,}  Rs {ptm_ref_matched[1]:>12,.0f}')

con.close()
print('\n' + '=' * 100)
print('END OF LAYER 2 REPORT')
print('=' * 100)
