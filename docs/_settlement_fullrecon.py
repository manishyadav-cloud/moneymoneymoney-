# -*- coding: utf-8 -*-
"""
Settlement Full Reconciliation
===============================
Traces ALL settlement records (all gateways, all months) back to
Juspay transactions + Juspay refunds.

Parts:
  A. Settlement universe categorised by Juspay linkage (all months)
  B. 4-way amount bridge for Jan26 Juspay SUCCESS scope
  C. Refund reconciliation (refund credits in settlement files)
  D. Consolidated cross-gateway summary
  E. Bank receipt check (Jan26 net settlements vs bank deposits)
"""
import duckdb

con = duckdb.connect('data.duckdb', read_only=True)

SEP  = '=' * 120
SEP2 = '-' * 120
SEP3 = '.' * 120

print(SEP)
print('SETTLEMENT FULL RECONCILIATION  --  ALL SETTLEMENTS TRACED TO JUSPAY + REFUNDS')
print(SEP)

# ======================================================================
# PART A: SETTLEMENT UNIVERSE  (all months, forward payments only)
#         For every settlement row -> what is it in Juspay?
# ======================================================================
print('\n\n' + SEP)
print('PART A: SETTLEMENT UNIVERSE -- ALL RECORDS BACK TO JUSPAY (all months)')
print(SEP)

# -----------------------------------------------------------------------
# A1: PAYTM
# -----------------------------------------------------------------------
print('\n### A1. PAYTM SETTLEMENTS (all 927,025 rows) ###\n')

paytm_univ = con.execute("""
    SELECT
        CASE
            WHEN j.order_id IS NULL AND t.Order_ID IS NULL THEN '5. Orphan (no PG txn match)'
            WHEN j.order_id IS NULL                        THEN '4. PG txn only (not in Juspay)'
            WHEN j.payment_status='SUCCESS'
             AND j.source_month='Jan26'                    THEN '1. Juspay Jan26 SUCCESS'
            WHEN j.payment_status='SUCCESS'                THEN '2. Juspay other-month SUCCESS'
            ELSE                                                '3. Juspay non-SUCCESS'
        END AS category,
        COUNT(*)                    AS settle_rows,
        SUM(s.amount)               AS gross_settled,
        SUM(s.settled_amount)       AS net_settled,
        SUM(s.commission)+SUM(s.gst) AS fees
    FROM paytm_settlements s
    LEFT JOIN paytm_transactions t
        ON TRIM(s.order_id, chr(39)) = TRIM(t.Order_ID, chr(39))
    LEFT JOIN juspay_transactions j
        ON TRIM(t.Order_ID, chr(39)) = TRIM(j.juspay_txn_id, chr(39))
    GROUP BY 1
    ORDER BY 1
""").fetchall()

total_rows  = sum(r[1] for r in paytm_univ)
total_gross = sum(r[2] for r in paytm_univ)
total_net   = sum(r[3] for r in paytm_univ)
total_fees  = sum(r[4] for r in paytm_univ)

print(f'{"Category":45s} {"Rows":>9s} {"Match%":>8s} {"Gross (Rs)":>16s} {"Net (Rs)":>14s} {"Fees (Rs)":>12s}')
print(SEP2)
for r in paytm_univ:
    pct = 100*r[1]/total_rows if total_rows else 0
    print(f'{r[0]:45s} {r[1]:>9,} {pct:>7.3f}% Rs {r[2]:>12,.0f} Rs {r[3]:>10,.0f} Rs {r[4]:>8,.0f}')
print(SEP2)
print(f'{"TOTAL":45s} {total_rows:>9,} {"100.000%":>8s} Rs {total_gross:>12,.0f} Rs {total_net:>10,.0f} Rs {total_fees:>8,.0f}')

# Jan26 universe match rate
jan26_paytm = next((r for r in paytm_univ if 'Jan26 SUCCESS' in r[0]), None)
if jan26_paytm:
    print(f'\n  Jan26 SUCCESS in settlements  : {jan26_paytm[1]:,} rows | Rs {jan26_paytm[2]:,.0f} gross | Rs {jan26_paytm[3]:,.0f} net')

# -----------------------------------------------------------------------
# A2: PHONEPE  (split by payment vs refund rows)
# -----------------------------------------------------------------------
print('\n\n### A2. PHONEPE SETTLEMENTS (all rows) ###\n')

# Payment type breakdown first
pp_types = con.execute("""
    SELECT
        COALESCE("Transaction Type", 'NULL') AS txn_type,
        COALESCE("Payment Type", 'NULL')     AS pay_type,
        COUNT(*) AS rows,
        SUM(CAST("Transaction Amount" AS DOUBLE)) AS gross,
        SUM(CAST("Net Amount" AS DOUBLE))          AS net,
        SUM(CAST("Total Fees" AS DOUBLE))           AS fees
    FROM phonepe_settlements
    GROUP BY 1, 2
    ORDER BY rows DESC
""").fetchall()
print('Payment type composition:')
print(f'  {"Txn Type":25s} {"Pay Type":15s} {"Rows":>8s} {"Gross (Rs)":>14s} {"Net (Rs)":>14s} {"Fees (Rs)":>12s}')
print('  ' + SEP3[:85])
for r in pp_types:
    print(f'  {r[0]:25s} {r[1]:15s} {r[2]:>8,} Rs {r[3]:>10,.0f} Rs {r[4]:>10,.0f} Rs {r[5]:>8,.2f}')

# Forward payments back to Juspay
print()
pp_fwd = con.execute("""
    SELECT
        CASE
            WHEN j.order_id IS NULL AND t."Merchant Order Id" IS NULL THEN '5. Orphan (no PG txn match)'
            WHEN j.order_id IS NULL                                    THEN '4. PG txn only (not in Juspay)'
            WHEN j.payment_status='SUCCESS' AND j.source_month='Jan26' THEN '1. Juspay Jan26 SUCCESS'
            WHEN j.payment_status='SUCCESS'                            THEN '2. Juspay other-month SUCCESS'
            ELSE                                                            '3. Juspay non-SUCCESS'
        END AS category,
        COUNT(*) AS rows,
        SUM(CAST(s."Transaction Amount" AS DOUBLE)) AS gross,
        SUM(CAST(s."Net Amount" AS DOUBLE))          AS net,
        SUM(CAST(s."Total Fees" AS DOUBLE))           AS fees
    FROM phonepe_settlements s
    LEFT JOIN phonepe_transactions t ON s."Merchant Order Id" = t."Merchant Order Id"
    LEFT JOIN juspay_transactions j  ON t."Merchant Order Id" = j.juspay_txn_id
    WHERE COALESCE(s."Transaction Type",'') != '' OR s."Payment Type" != 'REFUND'
    GROUP BY 1
    ORDER BY 1
""").fetchall()

pp_tot = sum(r[1] for r in pp_fwd)
print(f'Forward settlements (FORWARD_TRANSACTION rows) traced to Juspay:')
print(f'{"Category":45s} {"Rows":>9s} {"Match%":>8s} {"Gross (Rs)":>16s} {"Net (Rs)":>14s} {"Fees (Rs)":>12s}')
print(SEP2)
for r in pp_fwd:
    pct = 100*r[1]/pp_tot if pp_tot else 0
    print(f'{r[0]:45s} {r[1]:>9,} {pct:>7.3f}% Rs {r[2]:>12,.0f} Rs {r[3]:>10,.0f} Rs {r[4]:>8,.2f}')
print(SEP2)
print(f'{"TOTAL":45s} {pp_tot:>9,} {"100.000%":>8s}')

# Refund rows in settlements
pp_ref_sett = con.execute("""
    SELECT COUNT(*) AS rows,
           SUM(CAST("Transaction Amount" AS DOUBLE)) AS refund_amt,
           SUM(CAST("Net Amount" AS DOUBLE))          AS net_refund
    FROM phonepe_settlements
    WHERE "Payment Type" = 'REFUND'
""").fetchone()
print(f'\n  Refund rows in PhonePe settlements : {pp_ref_sett[0]:,} rows | Refund amt Rs {pp_ref_sett[1]:,.0f} | Net Rs {pp_ref_sett[2]:,.0f}')

# -----------------------------------------------------------------------
# A3: PAYU
# -----------------------------------------------------------------------
print('\n\n### A3. PAYU SETTLEMENTS (all rows) ###\n')

payu_univ = con.execute("""
    SELECT
        CASE
            WHEN j.order_id IS NULL AND t.txnid IS NULL THEN '5. Orphan (no PG txn match)'
            WHEN j.order_id IS NULL                     THEN '4. PG txn only (not in Juspay)'
            WHEN j.payment_status='SUCCESS' AND j.source_month='Jan26' THEN '1. Juspay Jan26 SUCCESS'
            WHEN j.payment_status='SUCCESS'             THEN '2. Juspay other-month SUCCESS'
            ELSE                                             '3. Juspay non-SUCCESS'
        END AS category,
        COUNT(*) AS rows,
        SUM(CAST(s."Amount" AS DOUBLE))      AS gross,
        SUM(CAST(s."Net Amount" AS DOUBLE))  AS net
    FROM payu_settlements s
    LEFT JOIN payu_transactions t ON s."Merchant Txn ID" = t.txnid
    LEFT JOIN juspay_transactions j ON t.txnid = j.juspay_txn_id
    GROUP BY 1
    ORDER BY 1
""").fetchall()

pu_tot = sum(r[1] for r in payu_univ)
print(f'{"Category":45s} {"Rows":>9s} {"Match%":>8s} {"Gross (Rs)":>16s} {"Net (Rs)":>14s}')
print(SEP2)
for r in payu_univ:
    pct = 100*r[1]/pu_tot if pu_tot else 0
    print(f'{r[0]:45s} {r[1]:>9,} {pct:>7.3f}% Rs {r[2]:>12,.0f} Rs {r[3]:>10,.0f}')
print(SEP2)
print(f'{"TOTAL":45s} {pu_tot:>9,} {"100.000%":>8s} Rs {sum(r[2] for r in payu_univ):>12,.0f} Rs {sum(r[3] for r in payu_univ):>10,.0f}')

# -----------------------------------------------------------------------
# A4: RAZORPAY  (embedded, split by type)
# -----------------------------------------------------------------------
print('\n\n### A4. RAZORPAY (razorpay_transactions, all types) ###\n')

rzp_types = con.execute("""
    SELECT type, COUNT(*) AS rows, SUM(amount) AS gross, SUM(fee) AS fee, SUM(tax) AS tax
    FROM razorpay_transactions
    GROUP BY type ORDER BY rows DESC
""").fetchall()
print('Transaction type composition:')
print(f'  {"Type":15s} {"Rows":>9s} {"Gross (Rs)":>14s} {"Fee (Rs)":>12s} {"Tax (Rs)":>10s}')
print('  ' + SEP2[:65])
for r in rzp_types: print(f'  {str(r[0]):15s} {r[1]:>9,} Rs {r[2]:>10,.0f} Rs {r[3]:>8,.2f} Rs {r[4]:>6,.2f}')

rzp_univ = con.execute("""
    SELECT
        r.type,
        CASE
            WHEN j.order_id IS NULL THEN 'Not in Juspay'
            WHEN j.payment_status='SUCCESS' AND j.source_month='Jan26' THEN 'Juspay Jan26 SUCCESS'
            WHEN j.payment_status='SUCCESS' THEN 'Juspay other-month SUCCESS'
            ELSE 'Juspay non-SUCCESS'
        END AS juspay_category,
        COUNT(*) AS rows,
        SUM(r.amount) AS gross,
        SUM(r.amount) - SUM(r.fee) - SUM(r.tax) AS net,
        SUM(r.fee) AS fee,
        SUM(r.tax) AS tax
    FROM razorpay_transactions r
    LEFT JOIN juspay_transactions j ON r.order_receipt = j.order_id
    GROUP BY 1, 2
    ORDER BY 1, rows DESC
""").fetchall()

print()
print(f'{"Type":12s} {"Juspay Category":35s} {"Rows":>8s} {"Gross (Rs)":>14s} {"Net (Rs)":>14s} {"Fee (Rs)":>10s}')
print(SEP2)
for r in rzp_univ:
    print(f'{str(r[0]):12s} {r[1]:35s} {r[2]:>8,} Rs {r[3]:>10,.0f} Rs {r[4]:>10,.0f} Rs {r[5]:>6,.2f}')


# ======================================================================
# PART B: 4-WAY AMOUNT BRIDGE  (Jan26 Juspay SUCCESS)
#         Juspay amt -> PG txn amt -> Settlement gross -> Settlement net
# ======================================================================
print('\n\n' + SEP)
print('PART B: 4-WAY AMOUNT BRIDGE -- JAN26 JUSPAY SUCCESS SCOPE')
print('  [1] Juspay  ->  [2] PG txn  ->  [3] Settlement gross  ->  [4] Settlement net (after fees)')
print(SEP)

print('\n### B1. PAYTM 4-WAY BRIDGE ###\n')
ptm_bridge = con.execute("""
    SELECT
        COUNT(DISTINCT j.order_id)       AS juspay_txns,
        SUM(j.amount)                    AS juspay_amt,
        SUM(t.Amount)                    AS pg_txn_amt,
        SUM(s.amount)                    AS settle_gross,
        SUM(s.settled_amount)            AS settle_net,
        SUM(s.commission)+SUM(s.gst)     AS settle_fees
    FROM juspay_transactions j
    INNER JOIN paytm_transactions t   ON TRIM(j.juspay_txn_id,chr(39)) = TRIM(t.Order_ID,chr(39))
    INNER JOIN paytm_settlements s    ON TRIM(t.Order_ID,chr(39)) = TRIM(s.order_id,chr(39))
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26' AND j.payment_gateway='PAYTM_V2'
""").fetchone()

ptm_diff12 = ptm_bridge[1] - ptm_bridge[2]  # Juspay - PG txn
ptm_diff23 = ptm_bridge[2] - ptm_bridge[3]  # PG txn - settle gross
ptm_diff34 = ptm_bridge[3] - ptm_bridge[4]  # settle gross - settle net (= fees)

print(f'  [1] Juspay amount          : Rs {ptm_bridge[1]:>14,.2f}   ({ptm_bridge[0]:,} txns)')
print(f'  [2] Paytm txn amount       : Rs {ptm_bridge[2]:>14,.2f}   diff [1]-[2]: Rs {ptm_diff12:,.2f}')
print(f'  [3] Settlement gross       : Rs {ptm_bridge[3]:>14,.2f}   diff [2]-[3]: Rs {ptm_diff23:,.2f}')
print(f'  [4] Settlement net         : Rs {ptm_bridge[4]:>14,.2f}   diff [3]-[4]: Rs {ptm_diff34:,.2f}  (= fees Rs {ptm_bridge[5]:,.2f})')
print(f'  Total fees (commission+GST): Rs {ptm_bridge[5]:>14,.2f}   Effective MDR: {ptm_bridge[5]/ptm_bridge[3]*100:.4f}%')
print(f'  Juspay to net diff         : Rs {ptm_bridge[1]-ptm_bridge[4]:>14,.2f}')


print('\n### B2. PHONEPE 4-WAY BRIDGE ###\n')
pp_bridge = con.execute("""
    SELECT
        COUNT(DISTINCT j.order_id)                             AS juspay_txns,
        SUM(j.amount)                                          AS juspay_amt,
        SUM(CAST(t."Transaction Amount" AS DOUBLE))            AS pg_txn_amt,
        SUM(CAST(s."Transaction Amount" AS DOUBLE))            AS settle_gross,
        SUM(CAST(s."Net Amount" AS DOUBLE))                    AS settle_net,
        ABS(SUM(CAST(s."Total Fees" AS DOUBLE)))               AS settle_fees
    FROM juspay_transactions j
    INNER JOIN phonepe_transactions t ON j.juspay_txn_id = t."Merchant Order Id"
    INNER JOIN phonepe_settlements s  ON t."Merchant Order Id" = s."Merchant Order Id"
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26' AND j.payment_gateway='PHONEPE'
    AND COALESCE(s."Transaction Type",'') != ''
""").fetchone()

pp_diff12 = pp_bridge[1] - pp_bridge[2]
pp_diff23 = pp_bridge[2] - pp_bridge[3]
pp_diff34 = pp_bridge[3] - pp_bridge[4]

print(f'  [1] Juspay amount          : Rs {pp_bridge[1]:>14,.2f}   ({pp_bridge[0]:,} txns)')
print(f'  [2] PhonePe txn amount     : Rs {pp_bridge[2]:>14,.2f}   diff [1]-[2]: Rs {pp_diff12:,.2f}')
print(f'  [3] Settlement gross       : Rs {pp_bridge[3]:>14,.2f}   diff [2]-[3]: Rs {pp_diff23:,.2f}')
print(f'  [4] Settlement net         : Rs {pp_bridge[4]:>14,.2f}   diff [3]-[4]: Rs {pp_diff34:,.2f}  (= fees Rs {pp_bridge[5]:,.2f})')
print(f'  Total fees (MDR + IGST)    : Rs {pp_bridge[5]:>14,.2f}   Effective MDR: {pp_bridge[5]/pp_bridge[3]*100:.4f}%')
print(f'  Juspay to net diff         : Rs {pp_bridge[1]-pp_bridge[4]:>14,.2f}')


print('\n### B3. PAYU 4-WAY BRIDGE ###\n')
payu_bridge = con.execute("""
    SELECT
        COUNT(DISTINCT j.order_id)                    AS juspay_txns,
        SUM(j.amount)                                 AS juspay_amt,
        SUM(CAST(t.amount AS DOUBLE))                 AS pg_txn_amt,
        SUM(CAST(s."Amount" AS DOUBLE))               AS settle_gross,
        SUM(CAST(s."Net Amount" AS DOUBLE))           AS settle_net,
        SUM(CAST(s."Amount" AS DOUBLE) - CAST(s."Net Amount" AS DOUBLE)) AS settle_fees
    FROM juspay_transactions j
    INNER JOIN payu_transactions t  ON j.juspay_txn_id = t.txnid
    INNER JOIN payu_settlements s   ON t.txnid = s."Merchant Txn ID"
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26' AND j.payment_gateway='PAYU'
""").fetchone()

pu_diff12 = payu_bridge[1] - payu_bridge[2]
pu_diff23 = payu_bridge[2] - payu_bridge[3]
pu_diff34 = payu_bridge[3] - payu_bridge[4]

print(f'  [1] Juspay amount          : Rs {payu_bridge[1]:>14,.2f}   ({payu_bridge[0]:,} txns)')
print(f'  [2] PayU txn amount        : Rs {payu_bridge[2]:>14,.2f}   diff [1]-[2]: Rs {pu_diff12:,.2f}')
print(f'  [3] Settlement gross       : Rs {payu_bridge[3]:>14,.2f}   diff [2]-[3]: Rs {pu_diff23:,.2f}')
print(f'  [4] Settlement net         : Rs {payu_bridge[4]:>14,.2f}   diff [3]-[4]: Rs {pu_diff34:,.2f}  (= fees Rs {payu_bridge[5]:,.2f})')
print(f'  Total fees                 : Rs {payu_bridge[5]:>14,.2f}   Effective MDR: {payu_bridge[5]/payu_bridge[3]*100:.4f}%')
print(f'  Juspay to net diff         : Rs {payu_bridge[1]-payu_bridge[4]:>14,.2f}')
print(f'  NOTE: diff [1]-[2] = Rs {pu_diff12:,.0f} = PayU refunded/chargebacked txns (243 txns, amount reduced by PG)')


print('\n### B4. RAZORPAY 4-WAY BRIDGE ###\n')
rzp_bridge = con.execute("""
    SELECT
        COUNT(DISTINCT j.order_id)      AS juspay_txns,
        SUM(j.amount)                   AS juspay_amt,
        SUM(r.amount)                   AS rzp_amt,
        SUM(r.amount)                   AS settle_gross,
        SUM(r.amount)-SUM(r.fee)-SUM(r.tax) AS settle_net,
        SUM(r.fee)+SUM(r.tax)           AS settle_fees
    FROM juspay_transactions j
    INNER JOIN razorpay_transactions r ON j.order_id = r.order_receipt AND r.type='payment'
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26' AND j.payment_gateway='RAZORPAY'
""").fetchone()

rz_diff = rzp_bridge[1] - rzp_bridge[2]
print(f'  [1] Juspay amount          : Rs {rzp_bridge[1]:>14,.2f}   ({rzp_bridge[0]:,} txns)')
print(f'  [2] Razorpay txn amount    : Rs {rzp_bridge[2]:>14,.2f}   diff [1]-[2]: Rs {rz_diff:,.2f}')
print(f'  [3] Settlement gross       : Rs {rzp_bridge[3]:>14,.2f}   (embedded in same row as txn)')
print(f'  [4] Settlement net         : Rs {rzp_bridge[4]:>14,.2f}   diff [3]-[4]: Rs {rzp_bridge[5]:,.2f}  (= fees)')
print(f'  Total fees (MDR+GST)       : Rs {rzp_bridge[5]:>14,.2f}   Effective MDR: {rzp_bridge[5]/rzp_bridge[3]*100:.4f}%')
print(f'  Juspay to net diff         : Rs {rzp_bridge[1]-rzp_bridge[4]:>14,.2f}')


# ======================================================================
# PART C: REFUND RECONCILIATION
#         Refund credits in settlement files + Juspay refunds trace
# ======================================================================
print('\n\n' + SEP)
print('PART C: REFUND RECONCILIATION')
print(SEP)

# --- C1: Juspay refunds universe ---
print('\n### C1. JUSPAY REFUNDS UNIVERSE ###\n')
jr_summary = con.execute("""
    SELECT
        j.payment_gateway,
        COUNT(*)                    AS refund_txns,
        SUM(jr.refund_amount)       AS total_refund_amt,
        COUNT(CASE WHEN jr.refund_status='SUCCESS' THEN 1 END) AS success_refunds,
        SUM(CASE WHEN jr.refund_status='SUCCESS' THEN jr.refund_amount ELSE 0 END) AS success_refund_amt,
        MIN(jr.refund_date)         AS earliest,
        MAX(jr.refund_date)         AS latest
    FROM juspay_refunds jr
    INNER JOIN juspay_transactions j ON jr.order_id = j.order_id
    GROUP BY j.payment_gateway
    ORDER BY refund_txns DESC
""").fetchall()
print(f'{"Gateway":15s} {"Refund Txns":>12s} {"Total Amt":>14s} {"SUCCESS":>10s} {"SUCCESS Amt":>14s} {"Earliest":>14s} {"Latest":>14s}')
print(SEP2)
for r in jr_summary:
    print(f'{str(r[0]):15s} {r[1]:>12,} Rs {r[2]:>10,.0f} {r[3]:>10,} Rs {r[4]:>10,.0f}  {str(r[5])[:12]:>14s} {str(r[6])[:12]:>14s}')

# Also show Jan26 only
print()
jr_jan26 = con.execute("""
    SELECT
        j.payment_gateway,
        COUNT(*)               AS refund_txns,
        SUM(jr.refund_amount)  AS total_refund_amt
    FROM juspay_refunds jr
    INNER JOIN juspay_transactions j ON jr.order_id = j.order_id
    WHERE j.source_month = 'Jan26'
    GROUP BY j.payment_gateway
    ORDER BY refund_txns DESC
""").fetchall()
print('Jan26 scope refunds:')
for r in jr_jan26:
    print(f'  {str(r[0]):15s}  {r[1]:,} refunds  Rs {r[2]:,.0f}')

# --- C2: PhonePe refund settlements ---
print('\n\n### C2. PHONEPE -- REFUND ROWS IN SETTLEMENTS ###\n')

pp_ref_detail = con.execute("""
    SELECT
        s."Payment Type",
        COALESCE(s."Transaction Status", 'NULL') AS txn_status,
        COUNT(*) AS rows,
        SUM(CAST(s."Transaction Amount" AS DOUBLE)) AS txn_amt,
        SUM(CAST(s."Net Amount" AS DOUBLE))          AS net_amt
    FROM phonepe_settlements s
    WHERE s."Payment Type" = 'REFUND'
    GROUP BY 1, 2
    ORDER BY rows DESC
""").fetchall()
print('Refund rows in phonepe_settlements:')
for r in pp_ref_detail:
    print(f'  PayType={r[0]}  Status={r[1]}  {r[2]:,} rows  Txn Amt Rs {(r[3] or 0):,.0f}  Net Rs {(r[4] or 0):,.0f}')

# Link PhonePe refund settlements to phonepe_refunds and juspay_refunds
pp_ref_trace = con.execute("""
    SELECT
        COUNT(DISTINCT s."Merchant Order Id")          AS sett_refund_rows,
        COUNT(DISTINCT r."Merchant Order Id")          AS matched_phonepe_refunds,
        COUNT(DISTINCT jr.order_id)                    AS matched_juspay_refunds,
        SUM(CAST(s."Transaction Amount" AS DOUBLE))    AS sett_refund_amt,
        SUM(CAST(r."Total Refund Amount" AS DOUBLE))   AS pg_refund_amt,
        SUM(jr.refund_amount)                          AS juspay_refund_amt
    FROM phonepe_settlements s
    LEFT JOIN phonepe_refunds r    ON s."Merchant Order Id" = r."Merchant Order Id"
    LEFT JOIN juspay_refunds jr    ON s."Merchant Order Id" = jr.order_id
    WHERE s."Payment Type" = 'REFUND'
""").fetchone()
print(f'\n  Settlement refund rows        : {pp_ref_trace[0]:,}  |  Rs {(pp_ref_trace[3] or 0):,.0f}')
print(f'  Matched in phonepe_refunds    : {pp_ref_trace[1]:,}  |  Rs {(pp_ref_trace[4] or 0):,.0f}')
print(f'  Matched in juspay_refunds     : {pp_ref_trace[2]:,}  |  Rs {(pp_ref_trace[5] or 0):,.0f}')
print(f'  Settlement refund amt - Juspay refund amt diff : Rs {(pp_ref_trace[3] or 0) - (pp_ref_trace[5] or 0):,.0f}')

# --- C3: Paytm refund settlements ---
print('\n\n### C3. PAYTM -- REFUND SETTLEMENT TREATMENT ###\n')

# Check paytm_refunds: are they in paytm_settlements?
ptm_ref_match = con.execute("""
    SELECT
        COUNT(DISTINCT r.Order_ID) AS paytm_refund_rows,
        COUNT(DISTINCT s.order_id) AS matched_in_settlements,
        SUM(r.Amount) AS refund_amt,
        SUM(s.amount) AS sett_amt
    FROM paytm_refunds r
    LEFT JOIN paytm_settlements s ON TRIM(r.Order_ID, chr(39)) = TRIM(s.order_id, chr(39))
""").fetchone()
print(f'  Total Paytm refund records (paytm_refunds): {ptm_ref_match[0]:,}')
print(f'  Of those, found in paytm_settlements       : {ptm_ref_match[1]:,}  |  Rs {(ptm_ref_match[3] or 0):,.0f}')
print(f'  Total refund amount (paytm_refunds.Amount) : Rs {(ptm_ref_match[2] or 0):,.0f}')

# Paytm refunds <-> juspay_refunds
ptm_ref_juspay = con.execute("""
    SELECT
        COUNT(DISTINCT r.Order_ID) AS ptm_refunds,
        COUNT(DISTINCT jr.order_id) AS matched_juspay,
        SUM(r.Amount) AS ptm_amt,
        SUM(jr.refund_amount) AS juspay_amt
    FROM paytm_refunds r
    LEFT JOIN juspay_refunds jr ON TRIM(r.Order_ID, chr(39)) = jr.order_id
""").fetchone()
print(f'\n  Paytm refund rows              : {ptm_ref_juspay[0]:,}  |  Rs {(ptm_ref_juspay[2] or 0):,.0f}')
print(f'  Matched in juspay_refunds      : {ptm_ref_juspay[1]:,}  |  Rs {(ptm_ref_juspay[3] or 0):,.0f}')
print(f'  Unmatched in Juspay            : {ptm_ref_juspay[0]-ptm_ref_juspay[1]:,}')

# --- C4: PayU refund settlements ---
print('\n\n### C4. PAYU -- REFUND AMOUNTS IN JUSPAY VS SETTLEMENT IMPACT ###\n')

# For PayU, the 243 mismatch txns: how much did settlement report vs juspay?
# The diff between Juspay amt and PayU txn amt = refunded portion
payu_ref_gap = con.execute("""
    SELECT
        p.status                        AS payu_status,
        COUNT(*)                        AS txns,
        SUM(j.amount)                   AS juspay_amt,
        SUM(CAST(p.amount AS DOUBLE))   AS payu_amt,
        SUM(j.amount) - SUM(CAST(p.amount AS DOUBLE)) AS gap,
        COUNT(DISTINCT s."Merchant Txn ID") AS in_settlements,
        SUM(CAST(s."Amount" AS DOUBLE)) AS sett_gross
    FROM juspay_transactions j
    INNER JOIN payu_transactions p ON j.juspay_txn_id = p.txnid
    LEFT JOIN payu_settlements s   ON p.txnid = s."Merchant Txn ID"
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26' AND j.payment_gateway='PAYU'
    AND j.amount != CAST(p.amount AS DOUBLE)
    GROUP BY p.status ORDER BY txns DESC
""").fetchall()
print(f'{"PayU Status":22s} {"Txns":>7s} {"Juspay Amt":>14s} {"PayU Amt":>14s} {"Gap":>12s} {"In Sett?":>10s} {"Sett Gross":>14s}')
print(SEP2)
for r in payu_ref_gap:
    print(f'{str(r[0]):22s} {r[1]:>7,} Rs {r[2]:>10,.0f} Rs {r[3]:>10,.0f} Rs {r[4]:>8,.0f} {(r[5] or 0):>10,} Rs {(r[6] or 0):>10,.0f}')

# --- C5: Razorpay refund type rows ---
print('\n\n### C5. RAZORPAY -- REFUND TYPE ROWS IN TRANSACTIONS TABLE ###\n')

rzp_refunds = con.execute("""
    SELECT
        r.type,
        COUNT(*) AS rows,
        SUM(r.amount) AS gross,
        SUM(r.fee)    AS fee,
        SUM(r.tax)    AS tax,
        COUNT(CASE WHEN j.order_id IS NOT NULL THEN 1 END)   AS matched_juspay_txn,
        COUNT(CASE WHEN jr.order_id IS NOT NULL THEN 1 END)  AS matched_juspay_refund
    FROM razorpay_transactions r
    LEFT JOIN juspay_transactions j ON r.order_receipt = j.order_id
    LEFT JOIN juspay_refunds jr      ON r.order_receipt = jr.order_id
    WHERE r.type = 'refund'
    GROUP BY 1
""").fetchall()

if rzp_refunds:
    for rr in rzp_refunds:
        print(f'  Type: {rr[0]} | Rows: {rr[1]:,} | Gross Rs {rr[2]:,.0f} | Fee Rs {rr[3]:,.2f}')
        print(f'  Matched juspay_transactions : {rr[5]:,}')
        print(f'  Matched juspay_refunds      : {rr[6]:,}')
else:
    rzp_all_types = con.execute("SELECT DISTINCT type, COUNT(*) FROM razorpay_transactions GROUP BY 1").fetchall()
    print(f'  No rows with type=refund found. Types in table: {rzp_all_types}')

# --- C6: Refund deductions impact on net settlement ---
print('\n\n### C6. REFUND DEDUCTIONS -- IMPACT ON NET SETTLEMENT (Jan26 scope) ###\n')

# Total refund amounts in Jan26 (from Juspay)
jr_jan26_total = con.execute("""
    SELECT
        COUNT(*) AS txns,
        SUM(jr.refund_amount) AS total_refund_amt,
        COUNT(CASE WHEN jr.refund_status='SUCCESS' THEN 1 END) AS success,
        SUM(CASE WHEN jr.refund_status='SUCCESS' THEN jr.refund_amount ELSE 0 END) AS success_amt
    FROM juspay_refunds jr
    INNER JOIN juspay_transactions j ON jr.order_id = j.order_id
    WHERE j.source_month='Jan26'
""").fetchone()
print(f'  Jan26 Juspay refund txns (all gateways): {jr_jan26_total[0]:,}')
print(f'  Total refund amount (all statuses)      : Rs {jr_jan26_total[1]:,.0f}')
print(f'  SUCCESS refunds                         : {jr_jan26_total[2]:,}  |  Rs {jr_jan26_total[3]:,.0f}')

# PhonePe refund rows: how much was deducted from settlement
pp_ref_deduction = con.execute("""
    SELECT
        COUNT(*) AS rows,
        SUM(CAST(s."Transaction Amount" AS DOUBLE)) AS refund_amt,
        SUM(CAST(s."Net Amount" AS DOUBLE)) AS net_refund
    FROM phonepe_settlements s
    INNER JOIN phonepe_transactions t ON s."Merchant Order Id" = t."Merchant Order Id"
    INNER JOIN juspay_transactions j  ON t."Merchant Order Id" = j.juspay_txn_id
    WHERE s."Payment Type" = 'REFUND' AND j.source_month = 'Jan26'
""").fetchone()
print(f'\n  PhonePe settlement refund deductions (Jan26 txns): {pp_ref_deduction[0]:,} rows  Rs {(pp_ref_deduction[1] or 0):,.0f}')

# ======================================================================
# PART D: CONSOLIDATED CROSS-GATEWAY SUMMARY
# ======================================================================
print('\n\n' + SEP)
print('PART D: CONSOLIDATED CROSS-GATEWAY SUMMARY (Jan26 Juspay SUCCESS scope)')
print(SEP)
print()

gw_data = {
    'PAYTM_V2': {
        'txns': ptm_bridge[0], 'juspay': ptm_bridge[1],
        'gross': ptm_bridge[3], 'net': ptm_bridge[4], 'fees': ptm_bridge[5]
    },
    'PHONEPE': {
        'txns': pp_bridge[0], 'juspay': pp_bridge[1],
        'gross': pp_bridge[3], 'net': pp_bridge[4], 'fees': pp_bridge[5]
    },
    'PAYU': {
        'txns': payu_bridge[0], 'juspay': payu_bridge[1],
        'gross': payu_bridge[3], 'net': payu_bridge[4], 'fees': payu_bridge[5]
    },
    'RAZORPAY': {
        'txns': rzp_bridge[0], 'juspay': rzp_bridge[1],
        'gross': rzp_bridge[3], 'net': rzp_bridge[4], 'fees': rzp_bridge[5]
    },
}

print(f'{"Gateway":12s} {"Txns":>9s} {"Juspay Amt":>16s} {"Sett Gross":>16s} {"Sett Net":>16s} {"Fees":>12s} {"MDR%":>7s} {"Match%":>8s}')
print(SEP)
t_txns = t_jus = t_gro = t_net = t_fee = 0
for gw, d in gw_data.items():
    mdr = d['fees']/d['gross']*100 if d['gross'] else 0
    match_pct = d['gross']/d['juspay']*100 if d['juspay'] else 0
    print(f'{gw:12s} {d["txns"]:>9,} Rs {d["juspay"]:>12,.0f} Rs {d["gross"]:>12,.0f} Rs {d["net"]:>12,.0f} Rs {d["fees"]:>8,.0f} {mdr:>6.4f}% {match_pct:>6.2f}%')
    t_txns += d['txns']; t_jus += d['juspay']; t_gro += d['gross']; t_net += d['net']; t_fee += d['fees']
print(SEP)
tot_mdr = t_fee/t_gro*100 if t_gro else 0
print(f'{"ALL GWs":12s} {t_txns:>9,} Rs {t_jus:>12,.0f} Rs {t_gro:>12,.0f} Rs {t_net:>12,.0f} Rs {t_fee:>8,.0f} {tot_mdr:>6.4f}%')

# Refund-adjusted net (forward settlements - refund deductions)
print(f'\n  Refund deductions from settlements (Jan26): Rs {jr_jan26_total[3]:,.0f}  (Juspay SUCCESS refunds)')
print(f'  Net after refund deductions                : Rs {t_net - jr_jan26_total[3]:,.0f}')
print(f'  Total PG cost (fees + refunds)             : Rs {t_fee + jr_jan26_total[3]:,.0f}')


# ======================================================================
# PART E: BANK RECEIPT CHECK (Jan 2026)
# ======================================================================
print('\n\n' + SEP)
print('PART E: BANK RECEIPT CHECK -- JAN 2026 NET SETTLEMENTS vs BANK DEPOSITS')
print('  NOTE: All bank deposits labeled "01 Paytm-Wallet (WIOM Gold)" = single bank account for ALL PGs')
print(SEP)
print()

bank_jan26 = con.execute("""
    SELECT
        COUNT(*) AS deposits,
        SUM(CAST("Deposit Amt(INR)" AS DOUBLE)) AS total_deposited,
        MIN(CAST("Transaction" AS DATE)) AS earliest,
        MAX(CAST("Transaction" AS DATE)) AS latest
    FROM bank_receipt_from_pg
    WHERE YEAR(CAST("Transaction" AS DATE)) = 2026
    AND MONTH(CAST("Transaction" AS DATE)) = 1
""").fetchone()

bank_monthly = con.execute("""
    SELECT
        Month,
        COUNT(*) AS deposits,
        SUM(CAST("Deposit Amt(INR)" AS DOUBLE)) AS total
    FROM bank_receipt_from_pg
    GROUP BY Month
    ORDER BY MIN(CAST("Transaction" AS DATE))
""").fetchall()

print(f'Jan 2026 Bank Deposits:')
print(f'  Deposits          : {bank_jan26[0]:,}')
print(f'  Total deposited   : Rs {bank_jan26[1]:>14,.2f}')
print(f'  Date range        : {bank_jan26[2]} to {bank_jan26[3]}')

print(f'\n  Jan26 Net settled (all 4 PGs combined) : Rs {t_net:>14,.2f}')
print(f'  Jan26 Bank deposits                     : Rs {bank_jan26[1]:>14,.2f}')
print(f'  Difference (net settled - bank deposit) : Rs {t_net - bank_jan26[1]:>14,.2f}')
print()
print('  Explanation of difference:')
print('  - Settlement reports cover txns in Jan26 Juspay scope (Jan creation date)')
print('  - Bank deposits include ALL settlements regardless of txn origin month')
print('  - Dec25 txns settle into Jan26 bank; Jan26 late txns may settle Feb26')
print('  - Refund deductions may further reduce net deposited')

print('\nAll months bank deposit summary:')
print(f'  {"Month":10s} {"Deposits":>10s} {"Total Deposited (Rs)":>22s}')
print('  ' + '-'*46)
for r in bank_monthly:
    print(f'  {str(r[0]):10s} {r[1]:>10,} Rs {r[2]:>18,.2f}')
print(f'  {"TOTAL":10s} {sum(r[1] for r in bank_monthly):>10,} Rs {sum(r[2] for r in bank_monthly):>18,.2f}')

con.close()
print('\n' + SEP)
print('END OF SETTLEMENT FULL RECONCILIATION REPORT')
print(SEP)
