# -*- coding: utf-8 -*-
"""
January 2026 Settlement Reconciliation
========================================
Scope: Only settlement records whose SETTLEMENT DATE falls in Jan 2026.
       (settlement date = the day money actually moved / was batched)

For each PG, traces Jan-26 settled rows back to:
  - Juspay transactions (by source_month: Dec25, Jan26, Feb26, etc.)
  - Juspay refunds
  - PG refund tables

Parts:
  A. Settlement universe (Jan26 settled date) by Juspay source_month
  B. 4-way amount bridge  (settled gross -> net, per gateway)
  C. Source-month waterfall: which months' txns were settled in Jan26?
  D. Refund rows settled in Jan26
  E. Bank receipt reconciliation (same period = should now match closely)

Date filters used:
  Paytm    : settled_date between 2026-01-01 and 2026-01-31
  PhonePe  : "Settlement Date" between 2026-01-01 and 2026-01-31
  PayU     : "AddedOn" LIKE '2026-01%'
  Razorpay : settled_at LIKE '2026-01%'
"""
import duckdb

con = duckdb.connect('data.duckdb', read_only=True)

SEP  = '=' * 120
SEP2 = '-' * 120

print(SEP)
print('JANUARY 2026 SETTLEMENT RECONCILIATION  --  SETTLEMENT DATE IN JAN 2026 -> JUSPAY')
print(SEP)

# ======================================================================
# PART A: SETTLEMENT UNIVERSE (Jan26 settlement date, all gateways)
#         Category = Juspay source_month + status
# ======================================================================
print('\n\n' + SEP)
print('PART A: ALL JAN-26 SETTLED ROWS  ->  JUSPAY (by source_month + status)')
print(SEP)

# -----------------------------------------------------------------------
# A1: PAYTM
# -----------------------------------------------------------------------
print('\n### A1. PAYTM -- settled in Jan 2026 ###\n')

ptm_a = con.execute("""
    SELECT
        COALESCE(j.source_month, 'NOT IN JUSPAY')   AS juspay_month,
        COALESCE(j.payment_status, 'N/A')            AS juspay_status,
        COUNT(DISTINCT s.order_id)                   AS settle_rows,
        SUM(s.amount)                                AS gross,
        SUM(s.settled_amount)                        AS net,
        SUM(s.commission) + SUM(s.gst)               AS fees
    FROM paytm_settlements s
    LEFT JOIN paytm_transactions t
        ON TRIM(s.order_id, chr(39)) = TRIM(t.Order_ID, chr(39))
    LEFT JOIN juspay_transactions j
        ON TRIM(t.Order_ID, chr(39)) = TRIM(j.juspay_txn_id, chr(39))
    WHERE YEAR(s.settled_date) = 2026 AND MONTH(s.settled_date) = 1
    GROUP BY 1, 2
    ORDER BY settle_rows DESC
""").fetchall()

ptm_tot = sum(r[2] for r in ptm_a)
ptm_gross_tot = sum(r[3] for r in ptm_a)
ptm_net_tot   = sum(r[4] for r in ptm_a)
ptm_fees_tot  = sum(r[5] for r in ptm_a)

print(f'{"Juspay Month":15s} {"Juspay Status":18s} {"Rows":>9s} {"Match%":>8s} {"Gross (Rs)":>16s} {"Net (Rs)":>14s} {"Fees (Rs)":>12s}')
print(SEP2)
for r in ptm_a:
    pct = 100 * r[2] / ptm_tot if ptm_tot else 0
    print(f'{r[0]:15s} {r[1]:18s} {r[2]:>9,} {pct:>7.3f}% Rs {r[3]:>12,.0f} Rs {r[4]:>10,.0f} Rs {r[5]:>8,.0f}')
print(SEP2)
print(f'{"TOTAL":35s} {ptm_tot:>9,} {"100.000%":>8s} Rs {ptm_gross_tot:>12,.0f} Rs {ptm_net_tot:>10,.0f} Rs {ptm_fees_tot:>8,.0f}')

# -----------------------------------------------------------------------
# A2: PHONEPE
# -----------------------------------------------------------------------
print('\n\n### A2. PHONEPE -- settled in Jan 2026 (forward payments only) ###\n')

pp_a = con.execute("""
    SELECT
        COALESCE(j.source_month, 'NOT IN JUSPAY')   AS juspay_month,
        COALESCE(j.payment_status, 'N/A')            AS juspay_status,
        COUNT(DISTINCT s."Merchant Order Id")         AS settle_rows,
        SUM(CAST(s."Transaction Amount" AS DOUBLE))  AS gross,
        SUM(CAST(s."Net Amount" AS DOUBLE))           AS net,
        ABS(SUM(CAST(s."Total Fees" AS DOUBLE)))      AS fees
    FROM phonepe_settlements s
    LEFT JOIN phonepe_transactions t ON s."Merchant Order Id" = t."Merchant Order Id"
    LEFT JOIN juspay_transactions j  ON t."Merchant Order Id" = j.juspay_txn_id
    WHERE CAST(s."Settlement Date" AS DATE) >= '2026-01-01'
      AND CAST(s."Settlement Date" AS DATE) < '2026-02-01'
      AND COALESCE(s."Transaction Type", '') != ''
    GROUP BY 1, 2
    ORDER BY settle_rows DESC
""").fetchall()

pp_tot = sum(r[2] for r in pp_a)
pp_gross = sum(r[3] for r in pp_a)
pp_net   = sum(r[4] for r in pp_a)
pp_fees  = sum(r[5] for r in pp_a)

print(f'{"Juspay Month":15s} {"Juspay Status":18s} {"Rows":>9s} {"Match%":>8s} {"Gross (Rs)":>16s} {"Net (Rs)":>14s} {"Fees (Rs)":>12s}')
print(SEP2)
for r in pp_a:
    pct = 100 * r[2] / pp_tot if pp_tot else 0
    print(f'{r[0]:15s} {r[1]:18s} {r[2]:>9,} {pct:>7.3f}% Rs {r[3]:>12,.0f} Rs {r[4]:>10,.0f} Rs {r[5]:>8,.2f}')
print(SEP2)
print(f'{"TOTAL":35s} {pp_tot:>9,} {"100.000%":>8s} Rs {pp_gross:>12,.0f} Rs {pp_net:>10,.0f} Rs {pp_fees:>8,.2f}')

# -----------------------------------------------------------------------
# A3: PAYU
# -----------------------------------------------------------------------
print('\n\n### A3. PAYU -- settled in Jan 2026 ###\n')

pu_a = con.execute("""
    SELECT
        COALESCE(j.source_month, 'NOT IN JUSPAY')   AS juspay_month,
        COALESCE(j.payment_status, 'N/A')            AS juspay_status,
        COUNT(DISTINCT s."Merchant Txn ID")           AS settle_rows,
        SUM(CAST(s."Amount" AS DOUBLE))               AS gross,
        SUM(CAST(s."Net Amount" AS DOUBLE))           AS net,
        SUM(CAST(s."Amount" AS DOUBLE) - CAST(s."Net Amount" AS DOUBLE)) AS fees
    FROM payu_settlements s
    LEFT JOIN payu_transactions t ON s."Merchant Txn ID" = t.txnid
    LEFT JOIN juspay_transactions j ON t.txnid = j.juspay_txn_id
    WHERE CAST(s."AddedOn" AS VARCHAR) LIKE '2026-01%'
    GROUP BY 1, 2
    ORDER BY settle_rows DESC
""").fetchall()

pu_tot = sum(r[2] for r in pu_a)
pu_gross = sum(r[3] for r in pu_a)
pu_net   = sum(r[4] for r in pu_a)
pu_fees  = sum(r[5] for r in pu_a)

print(f'{"Juspay Month":15s} {"Juspay Status":18s} {"Rows":>9s} {"Match%":>8s} {"Gross (Rs)":>16s} {"Net (Rs)":>14s} {"Fees (Rs)":>12s}')
print(SEP2)
for r in pu_a:
    pct = 100 * r[2] / pu_tot if pu_tot else 0
    print(f'{r[0]:15s} {r[1]:18s} {r[2]:>9,} {pct:>7.3f}% Rs {r[3]:>12,.0f} Rs {r[4]:>10,.0f} Rs {r[5]:>8,.2f}')
print(SEP2)
print(f'{"TOTAL":35s} {pu_tot:>9,} {"100.000%":>8s} Rs {pu_gross:>12,.0f} Rs {pu_net:>10,.0f} Rs {pu_fees:>8,.2f}')

# -----------------------------------------------------------------------
# A4: RAZORPAY
# -----------------------------------------------------------------------
print('\n\n### A4. RAZORPAY -- settled in Jan 2026 (type=payment) ###\n')

rzp_a = con.execute("""
    SELECT
        COALESCE(j.source_month, 'NOT IN JUSPAY')   AS juspay_month,
        COALESCE(j.payment_status, 'N/A')            AS juspay_status,
        COUNT(DISTINCT r.order_receipt)              AS settle_rows,
        SUM(r.amount)                                AS gross,
        SUM(r.amount) - SUM(r.fee) - SUM(r.tax)     AS net,
        SUM(r.fee) + SUM(r.tax)                      AS fees
    FROM razorpay_transactions r
    LEFT JOIN juspay_transactions j ON r.order_receipt = j.order_id
    WHERE r.type = 'payment' AND r.settled_at >= '2026-01-01' AND r.settled_at < '2026-02-01'
    GROUP BY 1, 2
    ORDER BY settle_rows DESC
""").fetchall()

rzp_tot = sum(r[2] for r in rzp_a)
rzp_gross = sum(r[3] for r in rzp_a)
rzp_net   = sum(r[4] for r in rzp_a)
rzp_fees  = sum(r[5] for r in rzp_a)

print(f'{"Juspay Month":15s} {"Juspay Status":18s} {"Rows":>9s} {"Match%":>8s} {"Gross (Rs)":>16s} {"Net (Rs)":>14s} {"Fees (Rs)":>12s}')
print(SEP2)
for r in rzp_a:
    pct = 100 * r[2] / rzp_tot if rzp_tot else 0
    print(f'{r[0]:15s} {r[1]:18s} {r[2]:>9,} {pct:>7.3f}% Rs {r[3]:>12,.0f} Rs {r[4]:>10,.0f} Rs {r[5]:>8,.2f}')
print(SEP2)
print(f'{"TOTAL":35s} {rzp_tot:>9,} {"100.000%":>8s} Rs {rzp_gross:>12,.0f} Rs {rzp_net:>10,.0f} Rs {rzp_fees:>8,.2f}')

# ======================================================================
# PART B: 4-WAY AMOUNT BRIDGE  (Jan26 settlement date scope)
# ======================================================================
print('\n\n' + SEP)
print('PART B: 4-WAY AMOUNT BRIDGE  (rows settled in Jan 2026)')
print('  [1] Juspay original  ->  [2] PG txn  ->  [3] Settlement gross  ->  [4] Net settled')
print(SEP)

print('\n### B1. PAYTM ###\n')
ptm_b = con.execute("""
    SELECT
        COUNT(DISTINCT j.order_id)              AS juspay_txns,
        SUM(j.amount)                           AS juspay_amt,
        SUM(t.Amount)                           AS pg_amt,
        SUM(s.amount)                           AS sett_gross,
        SUM(s.settled_amount)                   AS sett_net,
        SUM(s.commission) + SUM(s.gst)          AS fees,
        COUNT(DISTINCT j.source_month)          AS source_months
    FROM paytm_settlements s
    INNER JOIN paytm_transactions t   ON TRIM(s.order_id, chr(39)) = TRIM(t.Order_ID, chr(39))
    INNER JOIN juspay_transactions j  ON TRIM(t.Order_ID, chr(39)) = TRIM(j.juspay_txn_id, chr(39))
    WHERE YEAR(s.settled_date) = 2026 AND MONTH(s.settled_date) = 1
""").fetchone()

print(f'  Juspay txns matched    : {ptm_b[0]:,}  (spanning {ptm_b[6]} source month(s))')
print(f'  [1] Juspay orig amount : Rs {ptm_b[1]:>14,.2f}')
print(f'  [2] PG txn amount      : Rs {ptm_b[2]:>14,.2f}   diff [1]-[2]: Rs {ptm_b[1]-ptm_b[2]:>10,.2f}')
print(f'  [3] Settlement gross   : Rs {ptm_b[3]:>14,.2f}   diff [2]-[3]: Rs {ptm_b[2]-ptm_b[3]:>10,.2f}')
print(f'  [4] Settlement net     : Rs {ptm_b[4]:>14,.2f}   diff [3]-[4]: Rs {ptm_b[3]-ptm_b[4]:>10,.2f}  (fees)')
print(f'  Total fees             : Rs {ptm_b[5]:>14,.2f}   MDR: {ptm_b[5]/ptm_b[3]*100:.4f}%')
print(f'  Unmatched (orphan) gross : Rs {ptm_gross_tot - ptm_b[3]:>12,.2f}   orphan rows: {ptm_tot - ptm_b[0]:,}')

print('\n### B2. PHONEPE ###\n')
pp_b = con.execute("""
    SELECT
        COUNT(DISTINCT j.order_id)                             AS juspay_txns,
        SUM(j.amount)                                          AS juspay_amt,
        SUM(CAST(t."Transaction Amount" AS DOUBLE))            AS pg_amt,
        SUM(CAST(s."Transaction Amount" AS DOUBLE))            AS sett_gross,
        SUM(CAST(s."Net Amount" AS DOUBLE))                    AS sett_net,
        ABS(SUM(CAST(s."Total Fees" AS DOUBLE)))               AS fees,
        COUNT(DISTINCT j.source_month)                         AS source_months
    FROM phonepe_settlements s
    INNER JOIN phonepe_transactions t ON s."Merchant Order Id" = t."Merchant Order Id"
    INNER JOIN juspay_transactions j  ON t."Merchant Order Id" = j.juspay_txn_id
    WHERE CAST(s."Settlement Date" AS DATE) >= '2026-01-01'
      AND CAST(s."Settlement Date" AS DATE) < '2026-02-01'
      AND COALESCE(s."Transaction Type", '') != ''
""").fetchone()

print(f'  Juspay txns matched    : {pp_b[0]:,}  (spanning {pp_b[6]} source month(s))')
print(f'  [1] Juspay orig amount : Rs {pp_b[1]:>14,.2f}')
print(f'  [2] PG txn amount      : Rs {pp_b[2]:>14,.2f}   diff [1]-[2]: Rs {pp_b[1]-pp_b[2]:>10,.2f}')
print(f'  [3] Settlement gross   : Rs {pp_b[3]:>14,.2f}   diff [2]-[3]: Rs {pp_b[2]-pp_b[3]:>10,.2f}')
print(f'  [4] Settlement net     : Rs {pp_b[4]:>14,.2f}   diff [3]-[4]: Rs {pp_b[3]-pp_b[4]:>10,.2f}  (fees)')
print(f'  Total fees             : Rs {pp_b[5]:>14,.2f}   MDR: {pp_b[5]/pp_b[3]*100:.4f}%')
print(f'  Unmatched (orphan) gross : Rs {pp_gross - pp_b[3]:>12,.2f}   orphan rows: {pp_tot - pp_b[0]:,}')

print('\n### B3. PAYU ###\n')
pu_b = con.execute("""
    SELECT
        COUNT(DISTINCT j.order_id)                     AS juspay_txns,
        SUM(j.amount)                                  AS juspay_amt,
        SUM(CAST(t.amount AS DOUBLE))                  AS pg_amt,
        SUM(CAST(s."Amount" AS DOUBLE))                AS sett_gross,
        SUM(CAST(s."Net Amount" AS DOUBLE))            AS sett_net,
        SUM(CAST(s."Amount" AS DOUBLE) - CAST(s."Net Amount" AS DOUBLE)) AS fees,
        COUNT(DISTINCT j.source_month)                 AS source_months
    FROM payu_settlements s
    INNER JOIN payu_transactions t  ON s."Merchant Txn ID" = t.txnid
    INNER JOIN juspay_transactions j ON t.txnid = j.juspay_txn_id
    WHERE CAST(s."AddedOn" AS VARCHAR) LIKE '2026-01%'
""").fetchone()

print(f'  Juspay txns matched    : {pu_b[0]:,}  (spanning {pu_b[6]} source month(s))')
print(f'  [1] Juspay orig amount : Rs {pu_b[1]:>14,.2f}')
print(f'  [2] PG txn amount      : Rs {pu_b[2]:>14,.2f}   diff [1]-[2]: Rs {pu_b[1]-pu_b[2]:>10,.2f}')
print(f'  [3] Settlement gross   : Rs {pu_b[3]:>14,.2f}   diff [2]-[3]: Rs {pu_b[2]-pu_b[3]:>10,.2f}')
print(f'  [4] Settlement net     : Rs {pu_b[4]:>14,.2f}   diff [3]-[4]: Rs {pu_b[3]-pu_b[4]:>10,.2f}  (fees)')
print(f'  Total fees             : Rs {pu_b[5]:>14,.2f}   MDR: {pu_b[5]/pu_b[3]*100:.4f}%')
print(f'  Unmatched (orphan) gross : Rs {pu_gross - pu_b[3]:>12,.2f}   orphan rows: {pu_tot - pu_b[0]:,}')

print('\n### B4. RAZORPAY ###\n')
rzp_b = con.execute("""
    SELECT
        COUNT(DISTINCT j.order_id)                  AS juspay_txns,
        SUM(j.amount)                               AS juspay_amt,
        SUM(r.amount)                               AS pg_amt,
        SUM(r.amount)                               AS sett_gross,
        SUM(r.amount) - SUM(r.fee) - SUM(r.tax)    AS sett_net,
        SUM(r.fee) + SUM(r.tax)                     AS fees,
        COUNT(DISTINCT j.source_month)              AS source_months
    FROM razorpay_transactions r
    INNER JOIN juspay_transactions j ON r.order_receipt = j.order_id
    WHERE r.type = 'payment' AND r.settled_at >= '2026-01-01' AND r.settled_at < '2026-02-01'
""").fetchone()

print(f'  Juspay txns matched    : {rzp_b[0]:,}  (spanning {rzp_b[6]} source month(s))')
print(f'  [1] Juspay orig amount : Rs {rzp_b[1]:>14,.2f}')
print(f'  [2] PG txn amount      : Rs {rzp_b[2]:>14,.2f}   diff [1]-[2]: Rs {rzp_b[1]-rzp_b[2]:>10,.2f}')
print(f'  [3] Settlement gross   : Rs {rzp_b[3]:>14,.2f}   (embedded in txn row)')
print(f'  [4] Settlement net     : Rs {rzp_b[4]:>14,.2f}   diff [3]-[4]: Rs {rzp_b[3]-rzp_b[4]:>10,.2f}  (fees)')
print(f'  Total fees             : Rs {rzp_b[5]:>14,.2f}   MDR: {rzp_b[5]/rzp_b[3]*100:.4f}%')
print(f'  Unmatched (orphan) gross : Rs {rzp_gross - rzp_b[3]:>12,.2f}   orphan rows: {rzp_tot - rzp_b[0]:,}')

# ======================================================================
# PART C: SOURCE-MONTH WATERFALL
#         Which txn creation months are inside Jan26 settlements?
# ======================================================================
print('\n\n' + SEP)
print('PART C: SOURCE-MONTH WATERFALL  --  Which txn months are inside Jan26 settlements?')
print(SEP)

print('\n### C1. PAYTM -- source_month breakdown of Jan26 settlements ###\n')
ptm_wf = con.execute("""
    SELECT
        j.source_month,
        COUNT(DISTINCT s.order_id)           AS settle_rows,
        SUM(s.amount)                        AS gross,
        SUM(s.settled_amount)                AS net,
        MIN(s.settled_date)                  AS earliest_sett,
        MAX(s.settled_date)                  AS latest_sett
    FROM paytm_settlements s
    INNER JOIN paytm_transactions t   ON TRIM(s.order_id, chr(39)) = TRIM(t.Order_ID, chr(39))
    INNER JOIN juspay_transactions j  ON TRIM(t.Order_ID, chr(39)) = TRIM(j.juspay_txn_id, chr(39))
    WHERE YEAR(s.settled_date) = 2026 AND MONTH(s.settled_date) = 1
    GROUP BY j.source_month
    ORDER BY settle_rows DESC
""").fetchall()
print(f'  {"Source Month":15s} {"Txns":>9s} {"Gross (Rs)":>14s} {"Net (Rs)":>14s} {"Settled Range"}')
print('  ' + SEP2[:80])
for r in ptm_wf:
    print(f'  {str(r[0]):15s} {r[1]:>9,} Rs {r[2]:>10,.0f} Rs {r[3]:>10,.0f}  {str(r[4])} -> {str(r[5])}')

print('\n### C2. PHONEPE ###\n')
pp_wf = con.execute("""
    SELECT
        j.source_month,
        COUNT(DISTINCT s."Merchant Order Id")          AS settle_rows,
        SUM(CAST(s."Transaction Amount" AS DOUBLE))   AS gross,
        SUM(CAST(s."Net Amount" AS DOUBLE))            AS net
    FROM phonepe_settlements s
    INNER JOIN phonepe_transactions t ON s."Merchant Order Id" = t."Merchant Order Id"
    INNER JOIN juspay_transactions j  ON t."Merchant Order Id" = j.juspay_txn_id
    WHERE CAST(s."Settlement Date" AS DATE) >= '2026-01-01'
      AND CAST(s."Settlement Date" AS DATE) < '2026-02-01'
      AND COALESCE(s."Transaction Type", '') != ''
    GROUP BY j.source_month
    ORDER BY settle_rows DESC
""").fetchall()
for r in pp_wf:
    print(f'  {str(r[0]):15s} {r[1]:>9,} Rs {r[2]:>10,.0f} Rs {r[3]:>10,.0f}')

print('\n### C3. PAYU ###\n')
pu_wf = con.execute("""
    SELECT
        j.source_month,
        COUNT(DISTINCT s."Merchant Txn ID")   AS settle_rows,
        SUM(CAST(s."Amount" AS DOUBLE))       AS gross,
        SUM(CAST(s."Net Amount" AS DOUBLE))   AS net
    FROM payu_settlements s
    INNER JOIN payu_transactions t  ON s."Merchant Txn ID" = t.txnid
    INNER JOIN juspay_transactions j ON t.txnid = j.juspay_txn_id
    WHERE CAST(s."AddedOn" AS VARCHAR) LIKE '2026-01%'
    GROUP BY j.source_month
    ORDER BY settle_rows DESC
""").fetchall()
for r in pu_wf:
    print(f'  {str(r[0]):15s} {r[1]:>9,} Rs {r[2]:>10,.0f} Rs {r[3]:>10,.0f}')

print('\n### C4. RAZORPAY ###\n')
rzp_wf = con.execute("""
    SELECT
        j.source_month,
        COUNT(DISTINCT r.order_receipt)             AS settle_rows,
        SUM(r.amount)                               AS gross,
        SUM(r.amount) - SUM(r.fee) - SUM(r.tax)    AS net
    FROM razorpay_transactions r
    INNER JOIN juspay_transactions j ON r.order_receipt = j.order_id
    WHERE r.type = 'payment' AND r.settled_at >= '2026-01-01' AND r.settled_at < '2026-02-01'
    GROUP BY j.source_month
    ORDER BY settle_rows DESC
""").fetchall()
for r in rzp_wf:
    print(f'  {str(r[0]):15s} {r[1]:>9,} Rs {r[2]:>10,.0f} Rs {r[3]:>10,.0f}')

# ======================================================================
# PART D: REFUND ROWS SETTLED IN JAN 2026
# ======================================================================
print('\n\n' + SEP)
print('PART D: REFUND ROWS SETTLED IN JAN 2026')
print(SEP)

print('\n### D1. PHONEPE REFUND SETTLEMENTS in Jan26 ###\n')
pp_ref = con.execute("""
    SELECT
        COALESCE(j.source_month, 'NOT IN JUSPAY')  AS origin_month,
        COUNT(*)                                    AS rows,
        SUM(CAST(s."Transaction Amount" AS DOUBLE)) AS refund_amt,
        SUM(CAST(s."Net Amount" AS DOUBLE))          AS net
    FROM phonepe_settlements s
    LEFT JOIN phonepe_transactions t ON s."Merchant Order Id" = t."Merchant Order Id"
    LEFT JOIN juspay_transactions j  ON t."Merchant Order Id" = j.juspay_txn_id
    WHERE CAST(s."Settlement Date" AS DATE) >= '2026-01-01'
      AND CAST(s."Settlement Date" AS DATE) < '2026-02-01'
      AND s."Payment Type" = 'REFUND'
    GROUP BY 1
    ORDER BY rows DESC
""").fetchall()
print(f'  {"Origin Month":20s} {"Rows":>8s} {"Refund Amt (Rs)":>18s} {"Net (Rs)":>14s}')
print('  ' + '-'*65)
for r in pp_ref:
    print(f'  {str(r[0]):20s} {r[1]:>8,} Rs {(r[2] or 0):>14,.0f} Rs {(r[3] or 0):>10,.0f}')

print('\n### D2. PAYTM REFUNDS settled in Jan26 (paytm_refunds table) ###\n')
ptm_ref = con.execute("""
    SELECT
        COUNT(DISTINCT r.Order_ID)        AS refund_rows,
        SUM(r.Amount)                     AS refund_amt,
        SUM(r.Settled_Amount)             AS settled_amt,
        MIN(r.Settled_Date)               AS earliest,
        MAX(r.Settled_Date)               AS latest
    FROM paytm_refunds r
    WHERE CAST(r.Settled_Date AS VARCHAR) LIKE '2026-01%'
""").fetchone()
print(f'  Paytm refund rows settled in Jan26  : {(ptm_ref[0] or 0):,}')
print(f'  Total refund amount                 : Rs {(ptm_ref[1] or 0):,.2f}')
print(f'  Total settled (refunded to customer): Rs {(ptm_ref[2] or 0):,.2f}')
print(f'  Settled date range                  : {ptm_ref[3]} to {ptm_ref[4]}')

print('\n### D3. RAZORPAY REFUND rows settled in Jan26 ###\n')
rzp_ref = con.execute("""
    SELECT
        COALESCE(j.source_month, 'NOT IN JUSPAY')  AS origin_month,
        COUNT(*)                                    AS rows,
        SUM(r.amount)                               AS refund_amt
    FROM razorpay_transactions r
    LEFT JOIN juspay_transactions j ON r.order_receipt = j.order_id
    WHERE r.type = 'refund' AND r.settled_at >= '2026-01-01' AND r.settled_at < '2026-02-01'
    GROUP BY 1
    ORDER BY rows DESC
""").fetchall()
for r in rzp_ref:
    print(f'  {str(r[0]):15s}  {r[1]:,} rows  Rs {(r[2] or 0):,.0f}')

print('\n### D4. JUSPAY REFUNDS -- Jan26 txns refunded in Jan26 ###\n')
jr_d = con.execute("""
    SELECT
        j.payment_gateway,
        COUNT(*)                 AS refunds,
        SUM(jr.refund_amount)    AS total_amt,
        COUNT(CASE WHEN jr.refund_status='SUCCESS' THEN 1 END) AS success,
        SUM(CASE WHEN jr.refund_status='SUCCESS' THEN jr.refund_amount ELSE 0 END) AS success_amt
    FROM juspay_refunds jr
    INNER JOIN juspay_transactions j ON jr.order_id = j.order_id
    WHERE CAST(jr.refund_date AS VARCHAR) LIKE '2026-01%'
    GROUP BY j.payment_gateway
    ORDER BY refunds DESC
""").fetchall()
print(f'  {"Gateway":15s} {"Refunds":>10s} {"Total Amt":>14s} {"SUCCESS":>10s} {"SUCCESS Amt":>14s}')
print('  ' + '-'*68)
for r in jr_d:
    print(f'  {str(r[0]):15s} {r[1]:>10,} Rs {r[2]:>10,.0f} {r[3]:>10,} Rs {r[4]:>10,.0f}')

# ======================================================================
# PART E: CONSOLIDATED SUMMARY + BANK RECONCILIATION
# ======================================================================
print('\n\n' + SEP)
print('PART E: CONSOLIDATED SUMMARY  (Jan26 settlement date scope)')
print(SEP)
print()

# Grand total settled gross/net in Jan26 (from what matched Juspay)
matched_gross = ptm_b[3] + pp_b[3] + pu_b[3] + rzp_b[3]
matched_net   = ptm_b[4] + pp_b[4] + pu_b[4] + rzp_b[4]
matched_fees  = ptm_b[5] + pp_b[5] + pu_b[5] + rzp_b[5]
total_gross   = ptm_gross_tot + pp_gross + pu_gross + rzp_gross
total_net     = ptm_net_tot   + pp_net   + pu_net   + rzp_net
total_fees    = ptm_fees_tot  + pp_fees  + pu_fees  + rzp_fees

print(f'{"Gateway":12s} {"Sett Rows":>10s} {"Juspay Matched":>15s} {"Matched%":>10s} {"Gross (Rs)":>16s} {"Net (Rs)":>14s} {"Fees (Rs)":>12s} {"MDR%":>7s}')
print(SEP)
rows_summary = [
    ('PAYTM_V2',  ptm_tot,  ptm_b[0], ptm_b[3], ptm_b[4], ptm_b[5]),
    ('PHONEPE',   pp_tot,   pp_b[0],  pp_b[3],  pp_b[4],  pp_b[5]),
    ('PAYU',      pu_tot,   pu_b[0],  pu_b[3],  pu_b[4],  pu_b[5]),
    ('RAZORPAY',  rzp_tot,  rzp_b[0], rzp_b[3], rzp_b[4], rzp_b[5]),
]
for gw, tot, matched, gross, net, fees in rows_summary:
    pct = 100 * matched / tot if tot else 0
    mdr = fees/gross*100 if gross else 0
    print(f'{gw:12s} {tot:>10,} {matched:>15,} {pct:>9.2f}% Rs {gross:>12,.0f} Rs {net:>10,.0f} Rs {fees:>8,.0f} {mdr:>6.4f}%')
print(SEP)
print(f'{"ALL GWs":12s} {ptm_tot+pp_tot+pu_tot+rzp_tot:>10,} {ptm_b[0]+pp_b[0]+pu_b[0]+rzp_b[0]:>15,} {"":>10s} Rs {matched_gross:>12,.0f} Rs {matched_net:>10,.0f} Rs {matched_fees:>8,.0f} {matched_fees/matched_gross*100:>6.4f}%')
print(f'  Total incl orphans: gross Rs {total_gross:,.0f}  net Rs {total_net:,.0f}')

# Refund deductions (settled in Jan26)
ptm_refund_jan26 = ptm_ref[1] or 0
print(f'\n  Refunds settled in Jan26 (Paytm):          Rs {ptm_refund_jan26:>12,.2f}')
print(f'  Net after Paytm refund deductions          : Rs {ptm_b[4] - ptm_refund_jan26:>12,.2f}')

# -----------------------------------------------------------------------
# BANK RECEIPT RECONCILIATION
# -----------------------------------------------------------------------
print('\n\n' + SEP)
print('PART E2: BANK RECEIPT RECONCILIATION (Jan 2026)')
print('  Bank account "01 Paytm-Wallet (WIOM Gold)" receives RTGS from all PGs')
print(SEP)

bank = con.execute("""
    SELECT
        COUNT(*)                                    AS deposits,
        SUM(CAST("Deposit Amt(INR)" AS DOUBLE))     AS total,
        MIN(CAST("Transaction" AS DATE))            AS first_date,
        MAX(CAST("Transaction" AS DATE))            AS last_date
    FROM bank_receipt_from_pg
    WHERE YEAR(CAST("Transaction" AS DATE)) = 2026
      AND MONTH(CAST("Transaction" AS DATE)) = 1
""").fetchone()

bank_daily = con.execute("""
    SELECT
        CAST("Transaction" AS DATE) AS dt,
        CAST("Deposit Amt(INR)" AS DOUBLE) AS amt,
        "Transaction Remarks"
    FROM bank_receipt_from_pg
    WHERE YEAR(CAST("Transaction" AS DATE)) = 2026
      AND MONTH(CAST("Transaction" AS DATE)) = 1
    ORDER BY dt
""").fetchall()

print(f'\n  Bank deposits in Jan 2026:')
print(f'    Count          : {bank[0]}')
print(f'    Total deposited: Rs {bank[1]:,.2f}')
print(f'    Date range     : {bank[2]} to {bank[3]}')

print(f'\n  Settlement amounts settled in Jan 2026 (matched to Juspay):')
print(f'    Paytm net      : Rs {ptm_b[4]:>14,.2f}')
print(f'    PhonePe net    : Rs {pp_b[4]:>14,.2f}')
print(f'    PayU net       : Rs {pu_b[4]:>14,.2f}')
print(f'    Razorpay net   : Rs {rzp_b[4]:>14,.2f}')
print(f'    TOTAL net      : Rs {matched_net:>14,.2f}')

diff_paytm_only = ptm_b[4] - bank[1]
print(f'\n  Comparison:')
print(f'    Paytm net settled   vs bank total : Rs {ptm_b[4]:,.2f} - Rs {bank[1]:,.2f} = Rs {diff_paytm_only:,.2f}')
print(f'    All-PG net settled  vs bank total : Rs {matched_net:,.2f} - Rs {bank[1]:,.2f} = Rs {matched_net-bank[1]:,.2f}')
print(f'\n  NOTE: If bank account receives ONLY Paytm settlements,')
print(f'        gap = Rs {diff_paytm_only:,.2f} (Paytm net minus bank deposits)')
print(f'        This small diff = Paytm refund deductions (Rs {ptm_refund_jan26:,.2f}) + timing rounding')
print(f'\n  Paytm net - Paytm refunds settled = Rs {ptm_b[4] - ptm_refund_jan26:,.2f}')
print(f'  Bank deposits                      = Rs {bank[1]:,.2f}')
print(f'  Remaining gap                      = Rs {ptm_b[4] - ptm_refund_jan26 - bank[1]:,.2f}')

print('\n  Daily bank deposits (Jan 2026):')
print(f'  {"Date":14s} {"Amount (Rs)":>16s}  Remarks (truncated)')
print('  ' + '-'*80)
for r in bank_daily:
    print(f'  {str(r[0]):14s} Rs {r[1]:>12,.2f}  {str(r[2])[:50]}')

con.close()
print('\n' + SEP)
print('END OF JAN 2026 SETTLEMENT RECONCILIATION')
print(SEP)
