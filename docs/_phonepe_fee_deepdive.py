# -*- coding: utf-8 -*-
"""PhonePe negative fee root cause - sign convention + per-txn breakdown."""
import duckdb

con = duckdb.connect('data.duckdb', read_only=True)

SEP  = '=' * 110
SEP2 = '-' * 110

print(SEP)
print('PHONEPE NEGATIVE FEES -- ROOT CAUSE ANALYSIS (TRACED TO WIOM DB)')
print(SEP)

# ================================================================
# STEP 1: Confirm sign convention — sample payment rows with fees
# ================================================================
print('\n### STEP 1: PhonePe fee sign convention ###\n')

sign_check = con.execute("""
    SELECT
        "Transaction Type", "Payment Type", "Transaction Status",
        COUNT(*) AS rows,
        SUM(CAST("Transaction Amount" AS DOUBLE)) AS txn_amt,
        SUM(CAST("Net Amount" AS DOUBLE))         AS net_amt,
        SUM(CAST("Fee" AS DOUBLE))                AS fee,
        SUM(CAST("IGST" AS DOUBLE))               AS igst,
        SUM(CAST("Total Fees" AS DOUBLE))         AS total_fees,
        SUM(CAST("Net Amount" AS DOUBLE)) - SUM(CAST("Transaction Amount" AS DOUBLE)) AS net_minus_txn
    FROM phonepe_settlements
    GROUP BY 1, 2, 3
    ORDER BY rows DESC
""").fetchdf()
print(sign_check.to_string(index=False))

print('\n--- Conclusion: PhonePe stores fees as NEGATIVE numbers ---')
print('    Net Amount = Transaction Amount + Total_Fees  (where Total_Fees < 0 = charge TO merchant)')
print('    Net < Transaction = merchant received less = normal MDR deduction')

# ================================================================
# STEP 2: For Jan26 Juspay SUCCESS scope: which rows have fees vs not
# ================================================================
print('\n\n### STEP 2: Jan26 Juspay SUCCESS PhonePe settlements — fee vs no-fee rows ###\n')

fee_breakdown = con.execute("""
    SELECT
        CASE
            WHEN CAST(s."Total Fees" AS DOUBLE) < 0 THEN 'Fee charged (Total Fees < 0)'
            WHEN CAST(s."Total Fees" AS DOUBLE) = 0 THEN 'No fee (Total Fees = 0)'
            ELSE 'Fee credited (Total Fees > 0)'
        END AS fee_category,
        s."Mode",
        COUNT(*)                                          AS rows,
        SUM(CAST(s."Transaction Amount" AS DOUBLE))      AS txn_amt,
        SUM(CAST(s."Net Amount" AS DOUBLE))              AS net_amt,
        SUM(CAST(s."Fee" AS DOUBLE))                     AS fee,
        SUM(CAST(s."IGST" AS DOUBLE))                   AS igst,
        SUM(CAST(s."Total Fees" AS DOUBLE))              AS total_fees
    FROM juspay_transactions j
    INNER JOIN phonepe_transactions t ON j.juspay_txn_id = t."Merchant Order Id"
    INNER JOIN phonepe_settlements s  ON t."Merchant Order Id" = s."Merchant Order Id"
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='PHONEPE'
    GROUP BY 1, 2
    ORDER BY rows DESC
""").fetchdf()
print(fee_breakdown.to_string(index=False))

# ================================================================
# STEP 3: Sample of rows WITH fees — what's different about them?
# ================================================================
print('\n\n### STEP 3: Sample rows that HAVE fees charged (Total Fees < 0) ###\n')

fee_rows = con.execute("""
    SELECT
        j.order_id,
        j.amount                                           AS juspay_amt,
        CAST(s."Transaction Amount" AS DOUBLE)            AS sett_txn_amt,
        CAST(s."Net Amount" AS DOUBLE)                    AS sett_net_amt,
        CAST(s."Total Fees" AS DOUBLE)                    AS total_fees,
        CAST(s."Fee" AS DOUBLE)                           AS fee,
        CAST(s."IGST" AS DOUBLE)                          AS igst,
        s."Mode",
        s."Instrument",
        s."Payment Type",
        CAST(s."Transaction Date" AS VARCHAR)             AS txn_date,
        CAST(s."Settlement Date" AS VARCHAR)              AS settle_date,
        s."Settlement UTR",
        t."Transaction Status"                            AS pg_status
    FROM juspay_transactions j
    INNER JOIN phonepe_transactions t ON j.juspay_txn_id = t."Merchant Order Id"
    INNER JOIN phonepe_settlements s  ON t."Merchant Order Id" = s."Merchant Order Id"
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='PHONEPE'
    AND CAST(s."Total Fees" AS DOUBLE) < 0
    ORDER BY CAST(s."Total Fees" AS DOUBLE) ASC
""").fetchdf()

print(f'Rows with fees charged (Total Fees < 0) in Jan26 scope: {len(fee_rows):,}')
print(f'Total fee charged  : Rs {abs(fee_rows["total_fees"].sum()):,.2f}')
print(f'Fee breakdown: Fee={abs(fee_rows["fee"].sum()):,.2f}, IGST={abs(fee_rows["igst"].sum()):,.2f}')
print()
print('--- Mode (payment instrument) for fee-charged rows ---')
print(fee_rows['Mode'].value_counts().to_string())
print()
print('--- Instrument ---')
print(fee_rows['Instrument'].value_counts(dropna=False).head(10).to_string())
print()
print('--- Fee distribution ---')
print(fee_rows['total_fees'].describe().to_string())
print()
print('--- Top 20 rows with highest fee (most negative) ---')
print(fee_rows[['order_id','juspay_amt','sett_txn_amt','sett_net_amt','total_fees','fee','igst','Mode','Instrument','txn_date']].head(20).to_string(index=False))

# ================================================================
# STEP 4: Rows with NO fee — what's different (UPI = zero MDR)?
# ================================================================
print('\n\n### STEP 4: Mode breakdown for ALL Jan26 PhonePe rows (fee vs no-fee) ###\n')

mode_fee = con.execute("""
    SELECT
        s."Mode",
        SUM(CASE WHEN CAST(s."Total Fees" AS DOUBLE) < 0 THEN 1 ELSE 0 END) AS has_fee,
        SUM(CASE WHEN CAST(s."Total Fees" AS DOUBLE) = 0 THEN 1 ELSE 0 END) AS no_fee,
        COUNT(*) AS total,
        SUM(CAST(s."Total Fees" AS DOUBLE)) AS total_fees_sum
    FROM juspay_transactions j
    INNER JOIN phonepe_transactions t ON j.juspay_txn_id = t."Merchant Order Id"
    INNER JOIN phonepe_settlements s  ON t."Merchant Order Id" = s."Merchant Order Id"
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='PHONEPE'
    GROUP BY 1
    ORDER BY total DESC
""").fetchdf()
print(mode_fee.to_string(index=False))
print()
print('Interpretation:')
print('  UPI transactions: 0 MDR per RBI mandate (zero-fee settlement)')
print('  Non-UPI (cards/wallets): MDR applies (fees deducted = negative in PhonePe format)')

# ================================================================
# STEP 5: Trace fee-charged rows all the way to Wiom DB
# ================================================================
print('\n\n### STEP 5: Trace fee-charged rows -> Wiom DB ###\n')

trace = con.execute("""
    WITH fee_txns AS (
        SELECT
            j.order_id,
            j.juspay_txn_id,
            j.amount                                      AS juspay_amt,
            j.order_date_created,
            j.customer_id,
            CAST(s."Transaction Amount" AS DOUBLE)        AS sett_txn_amt,
            CAST(s."Net Amount" AS DOUBLE)                AS sett_net_amt,
            CAST(s."Total Fees" AS DOUBLE)                AS total_fees,
            CAST(s."Fee" AS DOUBLE)                       AS fee,
            CAST(s."IGST" AS DOUBLE)                      AS igst,
            s."Mode"                                      AS phonepe_mode,
            s."Instrument"                                AS phonepe_instrument,
            s."Settlement UTR"                            AS settlement_utr,
            CAST(s."Settlement Date" AS VARCHAR)          AS settlement_date,
            CASE
                WHEN j.order_id LIKE 'custGen_%'    THEN 'custGen_*'
                WHEN j.order_id LIKE 'custWgSubs_%' THEN 'custWgSubs_*'
                WHEN j.order_id LIKE 'cusSubs_%'    THEN 'cusSubs_*'
                WHEN j.order_id LIKE 'w_%'          THEN 'w_*'
                WHEN j.order_id LIKE 'mr_%'         THEN 'mr_*'
                ELSE 'other'
            END AS order_pattern
        FROM juspay_transactions j
        INNER JOIN phonepe_transactions t ON j.juspay_txn_id = t."Merchant Order Id"
        INNER JOIN phonepe_settlements s  ON t."Merchant Order Id" = s."Merchant Order Id"
        WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
        AND j.payment_gateway='PHONEPE'
        AND CAST(s."Total Fees" AS DOUBLE) < 0
    ),
    wiom_pr AS (
        SELECT TRANSACTION_ID AS order_id, 'wiom_primary_revenue' AS wiom_table,
               RECHARGE_DT AS wiom_date, CAST(TOTALPAID AS DOUBLE) AS wiom_amount,
               MODE AS wiom_mode, CAST(MOBILE AS VARCHAR) AS wiom_mobile,
               PLAN_TAKEN AS wiom_plan, PLAN_TYPE AS wiom_plan_type
        FROM wiom_primary_revenue
    ),
    wiom_ni AS (
        SELECT TXN_ID AS order_id, 'wiom_net_income' AS wiom_table,
               TXN_DT AS wiom_date, CAST(AMOUNT AS DOUBLE) AS wiom_amount,
               MODE AS wiom_mode, CAST(MOBILE AS VARCHAR) AS wiom_mobile,
               NULL::VARCHAR AS wiom_plan, REV_MODE AS wiom_plan_type
        FROM wiom_net_income
    ),
    wiom_bt AS (
        SELECT BOOKING_TXN_ID AS order_id, 'wiom_booking_transactions' AS wiom_table,
               CREATED_ON AS wiom_date, CAST(BOOKING_FEE AS DOUBLE) AS wiom_amount,
               'booking' AS wiom_mode, NULL::VARCHAR AS wiom_mobile,
               NULL::VARCHAR AS wiom_plan, NULL::VARCHAR AS wiom_plan_type
        FROM wiom_booking_transactions
    ),
    wiom_all AS (SELECT * FROM wiom_pr UNION ALL SELECT * FROM wiom_ni UNION ALL SELECT * FROM wiom_bt)
    SELECT
        f.*,
        w.wiom_table,
        w.wiom_date,
        w.wiom_amount,
        w.wiom_mode,
        w.wiom_mobile,
        w.wiom_plan,
        w.wiom_plan_type
    FROM fee_txns f
    LEFT JOIN wiom_all w ON f.order_id = w.order_id
    ORDER BY f.total_fees ASC
""").fetchdf()

print(f'Fee-charged rows: {len(trace):,}')
print()
print('--- order_id pattern ---')
for val, cnt in trace['order_pattern'].value_counts().items():
    sub = trace[trace['order_pattern']==val]
    print(f'  {str(val):20s}  {cnt:>5,} txns | Juspay Rs {sub["juspay_amt"].sum():>10,.0f} | Fees Rs {abs(sub["total_fees"].sum()):>8,.2f}')

print()
print('--- Wiom DB table ---')
for val, cnt in trace['wiom_table'].value_counts(dropna=False).items():
    sub = trace[trace['wiom_table']==val]
    print(f'  {str(val):35s}  {cnt:>5,} txns | Wiom Rs {sub["wiom_amount"].sum():>10,.0f} | Fees Rs {abs(sub["total_fees"].sum()):>8,.2f}')

print()
print('--- Wiom mode (product type) ---')
for val, cnt in trace['wiom_mode'].value_counts(dropna=False).items():
    sub = trace[trace['wiom_mode']==val]
    print(f'  {str(val):20s}  {cnt:>5,} txns | Fees Rs {abs(sub["total_fees"].sum()):>8,.2f}')

print()
print('--- Wiom plan type ---')
for val, cnt in trace['wiom_plan_type'].value_counts(dropna=False).head(10).items():
    sub = trace[trace['wiom_plan_type']==val]
    print(f'  {str(val):20s}  {cnt:>5,} txns | Fees Rs {abs(sub["total_fees"].sum()):>8,.2f}')

print()
print('--- Amount summary ---')
print(f'  Juspay total amt     : Rs {trace["juspay_amt"].sum():>12,.0f}')
print(f'  Settlement gross     : Rs {trace["sett_txn_amt"].sum():>12,.0f}')
print(f'  Settlement net       : Rs {trace["sett_net_amt"].sum():>12,.0f}')
print(f'  Total fees (MDR)     : Rs {abs(trace["total_fees"].sum()):>12,.2f}  (Fee: Rs {abs(trace["fee"].sum()):,.2f} + IGST: Rs {abs(trace["igst"].sum()):,.2f})')
print(f'  Effective MDR %      : {abs(trace["total_fees"].sum())/trace["juspay_amt"].sum()*100:.3f}%')

print()
print('--- Sample rows (all columns) top 15 ---')
print(trace[['order_id','order_pattern','juspay_amt','sett_txn_amt','sett_net_amt',
             'total_fees','fee','igst','phonepe_mode','phonepe_instrument',
             'wiom_table','wiom_amount','wiom_mobile','wiom_plan']].head(15).to_string(index=False))

# Export
trace.to_csv('docs/phonepe_fee_charged_trace.csv', index=False)
print(f'\nExported: docs/phonepe_fee_charged_trace.csv ({len(trace):,} rows)')

con.close()
print('\n' + SEP)
