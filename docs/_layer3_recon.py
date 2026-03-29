# -*- coding: utf-8 -*-
"""Layer 3 Reconciliation: PG Transactions <-> PG Settlements (Jan 2026)."""
import duckdb

con = duckdb.connect('data.duckdb', read_only=True)

SEP  = '=' * 110
SEP2 = '-' * 110

print(SEP)
print('LAYER 3 RECONCILIATION REPORT -- PG TRANSACTIONS vs PG SETTLEMENTS (JAN 2026)')
print('Scope: Juspay SUCCESS txns in Jan26 -> PG transaction records -> PG settlement records')
print(SEP)

# ================================================================
# Helper: Jan26 Juspay order_ids per gateway (our universe)
# ================================================================
juspay_gw = con.execute("""
    SELECT payment_gateway, COUNT(DISTINCT order_id) as txns, SUM(amount) as amt
    FROM juspay_transactions
    WHERE payment_status='SUCCESS' AND source_month='Jan26'
    GROUP BY payment_gateway
""").fetchall()
gw_map = {g[0]: (g[1], g[2]) for g in juspay_gw}

print('\n### SCOPE: Juspay Jan26 SUCCESS txns by gateway ###\n')
print(f'{"Gateway":15s} {"Juspay Txns":>12s}  {"Juspay Amount":>16s}')
print('-' * 48)
for gw, txns, amt in juspay_gw:
    print(f'{str(gw):15s} {txns:>12,}  Rs {amt:>12,.0f}')

# ================================================================
# TABLE 1: PAYTM
# Join: juspay -> paytm_transactions (juspay_txn_id = TRIM(Order_ID,chr(39)))
#       paytm_transactions -> paytm_settlements (TRIM(Order_ID) = TRIM(order_id))
# Amount: paytm_settlements.amount (gross), settled_amount (net), commission+gst (fees)
# ================================================================
print('\n\n### TABLE 1: PAYTM -- Transactions vs Settlements ###\n')

paytm_settled = con.execute("""
    SELECT
        COUNT(DISTINCT j.order_id)                              AS juspay_txns,
        COALESCE(SUM(j.amount), 0)                             AS juspay_amt,
        COUNT(DISTINCT t.Order_ID)                             AS pg_txns,
        COALESCE(SUM(t.Amount), 0)                             AS pg_txn_amt,
        COUNT(DISTINCT s.order_id)                             AS settled_txns,
        COALESCE(SUM(s.amount), 0)                             AS gross_settled,
        COALESCE(SUM(s.settled_amount), 0)                     AS net_settled,
        COALESCE(SUM(s.commission), 0)                         AS total_commission,
        COALESCE(SUM(s.gst), 0)                                AS total_gst,
        MIN(s.settled_date)                                    AS earliest_settle,
        MAX(s.settled_date)                                    AS latest_settle,
        COUNT(DISTINCT s.utr_no)                               AS settlement_batches
    FROM juspay_transactions j
    INNER JOIN paytm_transactions t
        ON TRIM(j.juspay_txn_id, chr(39)) = TRIM(t.Order_ID, chr(39))
    LEFT JOIN paytm_settlements s
        ON TRIM(t.Order_ID, chr(39)) = TRIM(s.order_id, chr(39))
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='PAYTM_V2'
""").fetchone()

paytm_unsettled = con.execute("""
    SELECT COUNT(DISTINCT j.order_id), COALESCE(SUM(j.amount),0)
    FROM juspay_transactions j
    INNER JOIN paytm_transactions t
        ON TRIM(j.juspay_txn_id, chr(39)) = TRIM(t.Order_ID, chr(39))
    LEFT JOIN paytm_settlements s
        ON TRIM(t.Order_ID, chr(39)) = TRIM(s.order_id, chr(39))
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='PAYTM_V2'
    AND s.order_id IS NULL
""").fetchone()

ptm_j = gw_map.get('PAYTM_V2', (0, 0))
settle_rate_ptm = 100 * paytm_settled[4] / ptm_j[0] if ptm_j[0] else 0
amt_diff_ptm    = paytm_settled[3] - paytm_settled[5]

print(f'{"Metric":55s} {"Count":>10s}  {"Amount (Rs)":>16s}')
print(SEP2)
print(f'{"Juspay SUCCESS txns (PAYTM_V2, Jan26)":55s} {ptm_j[0]:>10,}  Rs {ptm_j[1]:>12,.0f}')
print(f'{"Matched in paytm_transactions":55s} {paytm_settled[2]:>10,}  Rs {paytm_settled[3]:>12,.0f}')
print(f'{"Matched in paytm_settlements":55s} {paytm_settled[4]:>10,}  Rs {paytm_settled[5]:>12,.0f}  (gross)')
print(f'{"  Net settled (after commission+GST)":55s} {"":>10s}  Rs {paytm_settled[6]:>12,.0f}')
print(f'{"  Commission":55s} {"":>10s}  Rs {paytm_settled[7]:>12,.0f}')
print(f'{"  GST on commission":55s} {"":>10s}  Rs {paytm_settled[8]:>12,.0f}')
print(f'{"  Total fees":55s} {"":>10s}  Rs {paytm_settled[7]+paytm_settled[8]:>12,.0f}')
print(f'{"PG txn amt vs gross settled diff":55s} {"":>10s}  Rs {amt_diff_ptm:>12,.0f}')
print(f'{"Settlement rate (settled/juspay txns)":55s} {settle_rate_ptm:>9.2f}%')
print(f'{"Unsettled (Jan26 txns, no settlement)":55s} {paytm_unsettled[0]:>10,}  Rs {paytm_unsettled[1]:>12,.0f}')
print(f'{"Settlement date range":55s} {str(paytm_settled[9])} to {str(paytm_settled[10])}')
print(f'{"Distinct settlement batches (UTRs)":55s} {paytm_settled[11]:>10,}')


# ================================================================
# TABLE 2: PHONEPE
# Join: juspay -> phonepe_transactions (juspay_txn_id = "Merchant Order Id")
#       phonepe_transactions -> phonepe_settlements ("Merchant Order Id" = "Merchant Order Id")
# Amount: "Transaction Amount" (gross), "Net Amount" (net), "Total Fees" (fees)
# ================================================================
print('\n\n### TABLE 2: PHONEPE -- Transactions vs Settlements ###\n')

phonepe_settled = con.execute("""
    SELECT
        COUNT(DISTINCT j.order_id)                              AS juspay_txns,
        COALESCE(SUM(j.amount), 0)                             AS juspay_amt,
        COUNT(DISTINCT t."Merchant Order Id")                  AS pg_txns,
        COALESCE(SUM(CAST(t."Transaction Amount" AS DOUBLE)), 0) AS pg_txn_amt,
        COUNT(DISTINCT s."Merchant Order Id")                  AS settled_txns,
        COALESCE(SUM(CAST(s."Transaction Amount" AS DOUBLE)), 0) AS gross_settled,
        COALESCE(SUM(CAST(s."Net Amount" AS DOUBLE)), 0)       AS net_settled,
        COALESCE(SUM(CAST(s."Total Fees" AS DOUBLE)), 0)       AS total_fees,
        MIN(s."Settlement Date")                               AS earliest_settle,
        MAX(s."Settlement Date")                               AS latest_settle,
        COUNT(DISTINCT s."Settlement UTR")                     AS settlement_batches
    FROM juspay_transactions j
    INNER JOIN phonepe_transactions t ON j.juspay_txn_id = t."Merchant Order Id"
    LEFT JOIN phonepe_settlements s ON t."Merchant Order Id" = s."Merchant Order Id"
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='PHONEPE'
""").fetchone()

phonepe_unsettled = con.execute("""
    SELECT COUNT(DISTINCT j.order_id), COALESCE(SUM(j.amount),0)
    FROM juspay_transactions j
    INNER JOIN phonepe_transactions t ON j.juspay_txn_id = t."Merchant Order Id"
    LEFT JOIN phonepe_settlements s ON t."Merchant Order Id" = s."Merchant Order Id"
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='PHONEPE' AND s."Merchant Order Id" IS NULL
""").fetchone()

pp_j = gw_map.get('PHONEPE', (0, 0))
settle_rate_pp = 100 * phonepe_settled[4] / pp_j[0] if pp_j[0] else 0
amt_diff_pp    = phonepe_settled[3] - phonepe_settled[5]

print(f'{"Metric":55s} {"Count":>10s}  {"Amount (Rs)":>16s}')
print(SEP2)
print(f'{"Juspay SUCCESS txns (PHONEPE, Jan26)":55s} {pp_j[0]:>10,}  Rs {pp_j[1]:>12,.0f}')
print(f'{"Matched in phonepe_transactions":55s} {phonepe_settled[2]:>10,}  Rs {phonepe_settled[3]:>12,.0f}')
print(f'{"Matched in phonepe_settlements":55s} {phonepe_settled[4]:>10,}  Rs {phonepe_settled[5]:>12,.0f}  (gross)')
print(f'{"  Net settled (after fees)":55s} {"":>10s}  Rs {phonepe_settled[6]:>12,.0f}')
print(f'{"  Total fees":55s} {"":>10s}  Rs {phonepe_settled[7]:>12,.0f}')
print(f'{"PG txn amt vs gross settled diff":55s} {"":>10s}  Rs {amt_diff_pp:>12,.0f}')
print(f'{"Settlement rate":55s} {settle_rate_pp:>9.2f}%')
print(f'{"Unsettled (Jan26 txns, no settlement)":55s} {phonepe_unsettled[0]:>10,}  Rs {phonepe_unsettled[1]:>12,.0f}')
print(f'{"Settlement date range":55s} {str(phonepe_settled[8])} to {str(phonepe_settled[9])}')
print(f'{"Distinct settlement batches (UTRs)":55s} {phonepe_settled[10]:>10,}')


# ================================================================
# TABLE 3: PAYU
# Join: juspay -> payu_transactions (juspay_txn_id = txnid)
#       payu_transactions -> payu_settlements ("Merchant Txn ID" = txnid)
# Amount: "Amount" (gross), "Net Amount" (net)
# ================================================================
print('\n\n### TABLE 3: PAYU -- Transactions vs Settlements ###\n')

payu_settled = con.execute("""
    SELECT
        COUNT(DISTINCT j.order_id)                              AS juspay_txns,
        COALESCE(SUM(j.amount), 0)                             AS juspay_amt,
        COUNT(DISTINCT t.txnid)                                AS pg_txns,
        COALESCE(SUM(CAST(t.amount AS DOUBLE)), 0)             AS pg_txn_amt,
        COUNT(DISTINCT s."Merchant Txn ID")                    AS settled_txns,
        COALESCE(SUM(CAST(s."Amount" AS DOUBLE)), 0)           AS gross_settled,
        COALESCE(SUM(CAST(s."Net Amount" AS DOUBLE)), 0)       AS net_settled,
        COALESCE(SUM(CAST(s."Amount" AS DOUBLE) - CAST(s."Net Amount" AS DOUBLE)), 0) AS total_fees,
        MIN(s."AddedOn")                                       AS earliest_settle,
        MAX(s."AddedOn")                                       AS latest_settle,
        COUNT(DISTINCT s."Merchant UTR")                       AS settlement_batches
    FROM juspay_transactions j
    INNER JOIN payu_transactions t ON j.juspay_txn_id = t.txnid
    LEFT JOIN payu_settlements s ON t.txnid = s."Merchant Txn ID"
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='PAYU'
""").fetchone()

payu_unsettled = con.execute("""
    SELECT COUNT(DISTINCT j.order_id), COALESCE(SUM(j.amount),0)
    FROM juspay_transactions j
    INNER JOIN payu_transactions t ON j.juspay_txn_id = t.txnid
    LEFT JOIN payu_settlements s ON t.txnid = s."Merchant Txn ID"
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='PAYU' AND s."Merchant Txn ID" IS NULL
""").fetchone()

payu_j = gw_map.get('PAYU', (0, 0))
settle_rate_payu = 100 * payu_settled[4] / payu_j[0] if payu_j[0] else 0
amt_diff_payu    = payu_settled[3] - payu_settled[5]

print(f'{"Metric":55s} {"Count":>10s}  {"Amount (Rs)":>16s}')
print(SEP2)
print(f'{"Juspay SUCCESS txns (PAYU, Jan26)":55s} {payu_j[0]:>10,}  Rs {payu_j[1]:>12,.0f}')
print(f'{"Matched in payu_transactions":55s} {payu_settled[2]:>10,}  Rs {payu_settled[3]:>12,.0f}')
print(f'{"Matched in payu_settlements":55s} {payu_settled[4]:>10,}  Rs {payu_settled[5]:>12,.0f}  (gross)')
print(f'{"  Net settled (after fees)":55s} {"":>10s}  Rs {payu_settled[6]:>12,.0f}')
print(f'{"  Total fees (Amount - Net Amount)":55s} {"":>10s}  Rs {payu_settled[7]:>12,.0f}')
print(f'{"PG txn amt vs gross settled diff":55s} {"":>10s}  Rs {amt_diff_payu:>12,.0f}')
print(f'{"Settlement rate":55s} {settle_rate_payu:>9.2f}%')
print(f'{"Unsettled (Jan26 txns, no settlement)":55s} {payu_unsettled[0]:>10,}  Rs {payu_unsettled[1]:>12,.0f}')
print(f'{"Settlement date range":55s} {str(payu_settled[8])} to {str(payu_settled[9])}')
print(f'{"Distinct settlement batches (UTRs)":55s} {payu_settled[10]:>10,}')


# ================================================================
# TABLE 4: RAZORPAY (settlement embedded in transactions)
# No separate settlement table — settlement_id and settled_at are in razorpay_transactions
# ================================================================
print('\n\n### TABLE 4: RAZORPAY -- Embedded Settlement Analysis ###\n')
print('  Note: Razorpay settlement data is embedded in razorpay_transactions')
print('  Columns: settlement_id, settled_at, settled (flag), settlement_utr\n')

rzp_settled = con.execute("""
    SELECT
        COUNT(DISTINCT j.order_id)          AS juspay_txns,
        COALESCE(SUM(j.amount), 0)         AS juspay_amt,
        COUNT(DISTINCT r.order_receipt)    AS pg_txns,
        COALESCE(SUM(r.amount), 0)         AS pg_amt,
        SUM(CASE WHEN r.settled=1 THEN 1 ELSE 0 END) AS settled_txns,
        COALESCE(SUM(CASE WHEN r.settled=1 THEN r.amount ELSE 0 END), 0) AS settled_amt,
        COUNT(DISTINCT r.settlement_id)    AS settlement_batches,
        MIN(r.settled_at)                  AS earliest_settle,
        MAX(r.settled_at)                  AS latest_settle
    FROM juspay_transactions j
    INNER JOIN razorpay_transactions r
        ON j.order_id = r.order_receipt AND r.type='payment'
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='RAZORPAY'
""").fetchone()

rzp_j = gw_map.get('RAZORPAY', (0, 0))
settle_rate_rzp = 100 * rzp_settled[4] / rzp_j[0] if rzp_j[0] else 0

print(f'{"Metric":55s} {"Count":>10s}  {"Amount (Rs)":>16s}')
print(SEP2)
print(f'{"Juspay SUCCESS txns (RAZORPAY, Jan26)":55s} {rzp_j[0]:>10,}  Rs {rzp_j[1]:>12,.0f}')
print(f'{"Matched in razorpay_transactions (type=payment)":55s} {rzp_settled[2]:>10,}  Rs {rzp_settled[3]:>12,.0f}')
print(f'{"Settled (settled=1)":55s} {rzp_settled[4]:>10,}  Rs {rzp_settled[5]:>12,.0f}')
print(f'{"Settlement rate":55s} {settle_rate_rzp:>9.2f}%')
print(f'{"Distinct settlement batches (settlement_id)":55s} {rzp_settled[6]:>10,}')
print(f'{"Settlement date range (settled_at)":55s} {str(rzp_settled[7])} to {str(rzp_settled[8])}')
print(f'{"Note: fees not available in Razorpay transactions table":55s}')


# ================================================================
# TABLE 5: CONSOLIDATED SUMMARY
# ================================================================
print('\n\n### TABLE 5: LAYER 3 CONSOLIDATED SUMMARY (Jan 2026) ###\n')
print(f'{"Gateway":12s} {"Juspay":>10s} {"Settled":>10s} {"Rate%":>7s} {"Gross Settled":>16s} {"Net Settled":>14s} {"Fees":>12s} {"Unsettled Txns":>15s} {"Unsettled Amt":>14s}')
print('=' * 120)

rows = [
    ('PAYTM_V2',  ptm_j[0],  paytm_settled[4],   paytm_settled[5],   paytm_settled[6],   paytm_settled[7]+paytm_settled[8], paytm_unsettled[0],  paytm_unsettled[1]),
    ('PHONEPE',   pp_j[0],   phonepe_settled[4],  phonepe_settled[5], phonepe_settled[6], phonepe_settled[7],                phonepe_unsettled[0],phonepe_unsettled[1]),
    ('PAYU',      payu_j[0], payu_settled[4],     payu_settled[5],    payu_settled[6],    payu_settled[7],                   payu_unsettled[0],   payu_unsettled[1]),
    ('RAZORPAY',  rzp_j[0],  rzp_settled[4],      rzp_settled[5],     rzp_settled[5],     0,                                 rzp_j[0]-rzp_settled[4], rzp_j[1]-rzp_settled[5]),
]

t_j=t_s=t_g=t_n=t_f=t_ut=t_ua = 0
for gw, j_cnt, s_cnt, g_amt, n_amt, fees, ut_cnt, ut_amt in rows:
    rate = 100*s_cnt/j_cnt if j_cnt else 0
    note = ' *' if gw=='RAZORPAY' else ''
    print(f'{gw:12s} {j_cnt:>10,} {s_cnt:>10,} {rate:>6.1f}% Rs {g_amt:>12,.0f} Rs {n_amt:>10,.0f} Rs {fees:>8,.0f} {ut_cnt:>15,} Rs {ut_amt:>10,.0f}{note}')
    t_j+=j_cnt; t_s+=s_cnt; t_g+=g_amt; t_n+=n_amt; t_f+=fees; t_ut+=ut_cnt; t_ua+=ut_amt

print('=' * 120)
t_rate = 100*t_s/t_j if t_j else 0
print(f'{"TOTAL":12s} {t_j:>10,} {t_s:>10,} {t_rate:>6.1f}% Rs {t_g:>12,.0f} Rs {t_n:>10,.0f} Rs {t_f:>8,.0f} {t_ut:>15,} Rs {t_ua:>10,.0f}')
print('\n  * Razorpay: fees not available in transactions table (deducted at settlement_id level)')


# ================================================================
# TABLE 6: UNSETTLED BREAKDOWN — why are txns not yet settled?
# ================================================================
print('\n\n### TABLE 6: UNSETTLED TRANSACTIONS BREAKDOWN ###\n')

# Paytm unsettled - are they in the settlements table at all (maybe under different month)?
ptm_unsettled_detail = con.execute("""
    SELECT
        CASE
            WHEN s2.order_id IS NOT NULL THEN 'In settlement (other period)'
            ELSE 'Not in settlement table at all'
        END AS reason,
        COUNT(*) as cnt,
        SUM(j.amount) as amt
    FROM juspay_transactions j
    INNER JOIN paytm_transactions t
        ON TRIM(j.juspay_txn_id, chr(39)) = TRIM(t.Order_ID, chr(39))
    LEFT JOIN paytm_settlements s
        ON TRIM(t.Order_ID, chr(39)) = TRIM(s.order_id, chr(39))
    LEFT JOIN paytm_settlements s2
        ON TRIM(t.Order_ID, chr(39)) = TRIM(s2.order_id, chr(39))
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='PAYTM_V2'
    AND s.order_id IS NULL
    GROUP BY 1
""").fetchall()
print('Paytm unsettled detail:')
for x in ptm_unsettled_detail: print(f'  {str(x[0]):45s}  {x[1]:,} txns  Rs {x[2]:,.0f}')

# PayU unsettled
payu_unsettled_detail = con.execute("""
    SELECT
        CASE
            WHEN s2."Merchant Txn ID" IS NOT NULL THEN 'In settlement (other period)'
            ELSE 'Not in settlement table at all'
        END AS reason,
        COUNT(*) as cnt,
        SUM(j.amount) as amt
    FROM juspay_transactions j
    INNER JOIN payu_transactions t ON j.juspay_txn_id = t.txnid
    LEFT JOIN payu_settlements s ON t.txnid = s."Merchant Txn ID"
    LEFT JOIN payu_settlements s2 ON t.txnid = s2."Merchant Txn ID"
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='PAYU' AND s."Merchant Txn ID" IS NULL
    GROUP BY 1
""").fetchall()
print('\nPayU unsettled detail:')
for x in payu_unsettled_detail: print(f'  {str(x[0]):45s}  {x[1]:,} txns  Rs {x[2]:,.0f}')

# PhonePe unsettled
pp_unsettled_detail = con.execute("""
    SELECT
        CASE
            WHEN s2."Merchant Order Id" IS NOT NULL THEN 'In settlement (other period)'
            ELSE 'Not in settlement table at all'
        END AS reason,
        COUNT(*) as cnt,
        SUM(j.amount) as amt
    FROM juspay_transactions j
    INNER JOIN phonepe_transactions t ON j.juspay_txn_id = t."Merchant Order Id"
    LEFT JOIN phonepe_settlements s ON t."Merchant Order Id" = s."Merchant Order Id"
    LEFT JOIN phonepe_settlements s2 ON t."Merchant Order Id" = s2."Merchant Order Id"
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='PHONEPE' AND s."Merchant Order Id" IS NULL
    GROUP BY 1
""").fetchall()
print('\nPhonePe unsettled detail:')
for x in pp_unsettled_detail: print(f'  {str(x[0]):45s}  {x[1]:,} txns  Rs {x[2]:,.0f}')


# ================================================================
# TABLE 7: SETTLEMENT BATCHES SUMMARY (UTRs by date)
# ================================================================
print('\n\n### TABLE 7: SETTLEMENT BATCHES -- Jan26 txns by gateway ###\n')

print('--- Paytm: settlement batches for Jan26 txns ---')
ptm_batches = con.execute("""
    SELECT s.utr_no, s.settled_date, COUNT(DISTINCT s.order_id) as txns,
           SUM(s.amount) as gross, SUM(s.settled_amount) as net,
           SUM(s.commission)+SUM(s.gst) as fees
    FROM juspay_transactions j
    INNER JOIN paytm_transactions t ON TRIM(j.juspay_txn_id, chr(39)) = TRIM(t.Order_ID, chr(39))
    INNER JOIN paytm_settlements s ON TRIM(t.Order_ID, chr(39)) = TRIM(s.order_id, chr(39))
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26' AND j.payment_gateway='PAYTM_V2'
    GROUP BY s.utr_no, s.settled_date
    ORDER BY s.settled_date, s.utr_no
""").fetchall()
print(f'  {"UTR":30s} {"Settled Date":>14s} {"Txns":>8s} {"Gross":>14s} {"Net":>14s} {"Fees":>10s}')
print('  ' + '-'*95)
for x in ptm_batches:
    print(f'  {str(x[0]):30s} {str(x[1]):>14s} {x[2]:>8,} Rs {x[3]:>10,.0f} Rs {x[4]:>10,.0f} Rs {x[5]:>6,.0f}')

print()
print('--- PhonePe: settlement batches for Jan26 txns ---')
pp_batches = con.execute("""
    SELECT s."Settlement UTR", CAST(s."Settlement Date" AS VARCHAR) as settle_dt,
           COUNT(DISTINCT s."Merchant Order Id") as txns,
           SUM(CAST(s."Transaction Amount" AS DOUBLE)) as gross,
           SUM(CAST(s."Net Amount" AS DOUBLE)) as net,
           SUM(CAST(s."Total Fees" AS DOUBLE)) as fees
    FROM juspay_transactions j
    INNER JOIN phonepe_transactions t ON j.juspay_txn_id = t."Merchant Order Id"
    INNER JOIN phonepe_settlements s ON t."Merchant Order Id" = s."Merchant Order Id"
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26' AND j.payment_gateway='PHONEPE'
    GROUP BY s."Settlement UTR", s."Settlement Date"
    ORDER BY s."Settlement Date", s."Settlement UTR"
""").fetchall()
print(f'  {"UTR":25s} {"Settled Date":>25s} {"Txns":>8s} {"Gross":>14s} {"Net":>14s} {"Fees":>10s}')
print('  ' + '-'*100)
for x in pp_batches:
    print(f'  {str(x[0]):25s} {str(x[1]):>25s} {x[2]:>8,} Rs {x[3]:>10,.0f} Rs {x[4]:>10,.0f} Rs {x[5]:>6,.0f}')

print()
print('--- PayU: settlement batches for Jan26 txns (top 20 by amount) ---')
payu_batches = con.execute("""
    SELECT s."Merchant UTR", MIN(s."AddedOn") as settle_dt,
           COUNT(DISTINCT s."Merchant Txn ID") as txns,
           SUM(CAST(s."Amount" AS DOUBLE)) as gross,
           SUM(CAST(s."Net Amount" AS DOUBLE)) as net,
           SUM(CAST(s."Amount" AS DOUBLE) - CAST(s."Net Amount" AS DOUBLE)) as fees
    FROM juspay_transactions j
    INNER JOIN payu_transactions t ON j.juspay_txn_id = t.txnid
    INNER JOIN payu_settlements s ON t.txnid = s."Merchant Txn ID"
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26' AND j.payment_gateway='PAYU'
    GROUP BY s."Merchant UTR"
    ORDER BY gross DESC
    LIMIT 20
""").fetchall()
print(f'  {"UTR":30s} {"Settled Date":>25s} {"Txns":>8s} {"Gross":>14s} {"Net":>14s} {"Fees":>10s}')
print('  ' + '-'*105)
for x in payu_batches:
    print(f'  {str(x[0]):30s} {str(x[1]):>25s} {x[2]:>8,} Rs {x[3]:>10,.0f} Rs {x[4]:>10,.0f} Rs {x[5]:>6,.2f}')

con.close()
print('\n' + SEP)
print('END OF LAYER 3 REPORT')
print(SEP)
