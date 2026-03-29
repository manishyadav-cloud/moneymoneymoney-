# -*- coding: utf-8 -*-
"""Razorpay fee investigation — fee/tax cols exist in razorpay_transactions."""
import duckdb

con = duckdb.connect('data.duckdb', read_only=True)

SEP  = '=' * 110
SEP2 = '-' * 110

print(SEP)
print('RAZORPAY FEE INVESTIGATION -- TRACED TO WIOM DB (JAN 2026)')
print(SEP)

# ================================================================
# STEP 1: Overall fee picture for Jan26 Juspay scope
# ================================================================
print('\n### STEP 1: Razorpay fee summary for Jan26 Juspay SUCCESS scope ###\n')

summary = con.execute("""
    SELECT
        COUNT(DISTINCT j.order_id)      AS juspay_txns,
        SUM(r.amount)                   AS gross_collected,
        SUM(r.fee)                      AS total_fee,
        SUM(r.tax)                      AS total_tax,
        SUM(r.fee) + SUM(r.tax)         AS total_fee_incl_tax,
        SUM(r.amount) - SUM(r.fee) - SUM(r.tax) AS net_settled,
        COUNT(CASE WHEN r.fee > 0 THEN 1 END)   AS txns_with_fee,
        COUNT(CASE WHEN r.fee = 0 THEN 1 END)   AS txns_zero_fee,
        MIN(r.fee)                      AS min_fee,
        MAX(r.fee)                      AS max_fee,
        AVG(r.fee)                      AS avg_fee,
        SUM(r.fee) / SUM(r.amount) * 100 AS effective_mdr_pct
    FROM juspay_transactions j
    INNER JOIN razorpay_transactions r ON j.order_id = r.order_receipt AND r.type='payment'
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='RAZORPAY'
""").fetchone()

print(f'{"Metric":50s} {"Value":>20s}')
print(SEP2)
print(f'{"Juspay SUCCESS txns (RAZORPAY, Jan26)":50s} {summary[0]:>20,}')
print(f'{"Gross collected (razorpay.amount)":50s} Rs {summary[1]:>16,.2f}')
print(f'{"Total fee (razorpay.fee)":50s} Rs {summary[2]:>16,.2f}')
print(f'{"Total tax/GST (razorpay.tax)":50s} Rs {summary[3]:>16,.2f}')
print(f'{"Total fee incl. GST":50s} Rs {summary[4]:>16,.2f}')
print(f'{"Net settled (gross - fee - tax)":50s} Rs {summary[5]:>16,.2f}')
print(f'{"Txns WITH fee > 0":50s} {summary[6]:>20,}')
print(f'{"Txns with fee = 0":50s} {summary[7]:>20,}')
print(f'{"Min fee per txn":50s} Rs {summary[8]:>16,.2f}')
print(f'{"Max fee per txn":50s} Rs {summary[9]:>16,.2f}')
print(f'{"Avg fee per txn":50s} Rs {summary[10]:>16,.4f}')
print(f'{"Effective MDR %":50s} {summary[11]:>19.4f}%')

# ================================================================
# STEP 2: Fee breakdown by payment method
# ================================================================
print('\n\n### STEP 2: Fee breakdown by payment method ###\n')

by_method = con.execute("""
    SELECT
        r.method,
        r.card_network,
        r.card_type,
        COUNT(*)                AS txns,
        SUM(r.amount)           AS gross,
        SUM(r.fee)              AS fee,
        SUM(r.tax)              AS tax,
        SUM(r.fee)+SUM(r.tax)   AS fee_incl_tax,
        SUM(r.amount)-SUM(r.fee)-SUM(r.tax) AS net,
        ROUND(SUM(r.fee)/NULLIF(SUM(r.amount),0)*100, 4) AS mdr_pct
    FROM juspay_transactions j
    INNER JOIN razorpay_transactions r ON j.order_id = r.order_receipt AND r.type='payment'
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='RAZORPAY'
    GROUP BY r.method, r.card_network, r.card_type
    ORDER BY txns DESC
""").fetchdf()

print(f'{"Method":12s} {"Network":12s} {"Type":10s} {"Txns":>7s} {"Gross":>12s} {"Fee":>10s} {"Tax":>8s} {"Fee+Tax":>10s} {"Net":>12s} {"MDR%":>7s}')
print(SEP2)
for _, row in by_method.iterrows():
    print(f'{str(row["method"]):12s} {str(row["card_network"]):12s} {str(row["card_type"]):10s} '
          f'{int(row["txns"]):>7,} Rs {row["gross"]:>8,.0f} Rs {row["fee"]:>6,.2f} Rs {row["tax"]:>4,.2f} '
          f'Rs {row["fee_incl_tax"]:>6,.2f} Rs {row["net"]:>8,.0f} {row["mdr_pct"]:>6.4f}%')
print(SEP2)
print(f'{"TOTAL":47s} {int(by_method["txns"].sum()):>7,} Rs {by_method["gross"].sum():>8,.0f} '
      f'Rs {by_method["fee"].sum():>6,.2f} Rs {by_method["tax"].sum():>4,.2f} '
      f'Rs {(by_method["fee"]+by_method["tax"]).sum():>6,.2f} Rs {by_method["net"].sum():>8,.0f}')

# ================================================================
# STEP 3: Settlement batch level — fee per batch
# ================================================================
print('\n\n### STEP 3: Fee per settlement batch (settlement_id) ###\n')

by_batch = con.execute("""
    SELECT
        r.settlement_id,
        r.settled_at,
        COUNT(*)                AS txns,
        SUM(r.amount)           AS gross,
        SUM(r.fee)              AS fee,
        SUM(r.tax)              AS tax,
        SUM(r.amount)-SUM(r.fee)-SUM(r.tax) AS net
    FROM juspay_transactions j
    INNER JOIN razorpay_transactions r ON j.order_id = r.order_receipt AND r.type='payment'
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='RAZORPAY'
    GROUP BY r.settlement_id, r.settled_at
    ORDER BY r.settled_at
""").fetchall()

print(f'{"Settlement ID":30s} {"Settled At":>14s} {"Txns":>7s} {"Gross":>12s} {"Fee":>10s} {"Tax":>8s} {"Net":>12s}')
print(SEP2)
for x in by_batch:
    print(f'{str(x[0]):30s} {str(x[1]):>14s} {x[2]:>7,} Rs {x[3]:>8,.0f} Rs {x[4]:>6,.2f} Rs {x[5]:>4,.2f} Rs {x[6]:>8,.0f}')

# ================================================================
# STEP 4: Correction to Layer 3 — what the correct numbers should be
# ================================================================
print('\n\n### STEP 4: Corrected Layer 3 Razorpay figures ###\n')
print(f'  PREVIOUSLY REPORTED in Layer 3 (incorrect):')
print(f'    Gross settled = Rs 2,444,189  Net settled = Rs 2,444,189  Fees = Rs 0')
print(f'  CORRECT figures:')
print(f'    Gross settled = Rs {summary[1]:,.2f}')
print(f'    Total fee     = Rs {summary[2]:,.2f}')
print(f'    Total GST     = Rs {summary[3]:,.2f}')
print(f'    Net settled   = Rs {summary[5]:,.2f}')
print(f'    Effective MDR = {summary[11]:.4f}%')

# ================================================================
# STEP 5: Trace fee-bearing txns back to Wiom DB
# ================================================================
print('\n\n### STEP 5: Full trace — Razorpay fee txns -> Juspay -> Wiom DB ###\n')

trace = con.execute("""
    WITH rzp AS (
        SELECT
            j.order_id,
            j.juspay_txn_id,
            j.amount                    AS juspay_amt,
            j.customer_id,
            j.order_date_created,
            r.amount                    AS rzp_gross,
            r.fee                       AS rzp_fee,
            r.tax                       AS rzp_tax,
            r.amount - r.fee - r.tax    AS rzp_net,
            r.method,
            r.card_network,
            r.card_type,
            r.settlement_id,
            r.settled_at,
            CASE
                WHEN j.order_id LIKE 'custGen_%'    THEN 'custGen_*'
                WHEN j.order_id LIKE 'custWgSubs_%' THEN 'custWgSubs_*'
                WHEN j.order_id LIKE 'cusSubs_%'    THEN 'cusSubs_*'
                WHEN j.order_id LIKE 'w_%'          THEN 'w_*'
                WHEN j.order_id LIKE 'mr_%'         THEN 'mr_*'
                ELSE 'other'
            END AS order_pattern
        FROM juspay_transactions j
        INNER JOIN razorpay_transactions r ON j.order_id = r.order_receipt AND r.type='payment'
        WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
        AND j.payment_gateway='RAZORPAY'
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
    SELECT r.*, w.wiom_table, w.wiom_date, w.wiom_amount, w.wiom_mode,
           w.wiom_mobile, w.wiom_plan, w.wiom_plan_type
    FROM rzp r
    LEFT JOIN wiom_all w ON r.order_id = w.order_id
    ORDER BY r.rzp_fee DESC
""").fetchdf()

# Summary by order_pattern
print('--- Fee breakdown by order_id pattern ---')
print(f'{"Pattern":20s} {"Txns":>7s} {"Gross":>12s} {"Fee":>10s} {"Tax":>8s} {"Fee+Tax":>10s} {"MDR%":>7s}')
print(SEP2)
for pat in trace['order_pattern'].value_counts().index:
    sub = trace[trace['order_pattern']==pat]
    fee_total = sub['rzp_fee'].sum()
    tax_total = sub['rzp_tax'].sum()
    gross = sub['rzp_gross'].sum()
    mdr = fee_total/gross*100 if gross else 0
    print(f'{str(pat):20s} {len(sub):>7,} Rs {gross:>8,.0f} Rs {fee_total:>6,.2f} Rs {tax_total:>4,.2f} Rs {fee_total+tax_total:>6,.2f} {mdr:>6.4f}%')

print()
print('--- Fee breakdown by Wiom DB table ---')
print(f'{"Wiom Table":35s} {"Txns":>7s} {"Wiom Amt":>12s} {"Gross":>12s} {"Fee+Tax":>10s}')
print(SEP2)
for tbl in trace['wiom_table'].value_counts(dropna=False).index:
    sub = trace[trace['wiom_table']==tbl]
    print(f'{str(tbl):35s} {len(sub):>7,} Rs {sub["wiom_amount"].sum():>8,.0f} Rs {sub["rzp_gross"].sum():>8,.0f} Rs {(sub["rzp_fee"]+sub["rzp_tax"]).sum():>6,.2f}')

print()
print('--- Fee breakdown by Wiom plan type ---')
for val, cnt in trace['wiom_plan_type'].value_counts(dropna=False).head(10).items():
    sub = trace[trace['wiom_plan_type']==val]
    print(f'  {str(val):20s}  {cnt:>5,} txns  Fee+Tax Rs {(sub["rzp_fee"]+sub["rzp_tax"]).sum():>8,.2f}  MDR {sub["rzp_fee"].sum()/sub["rzp_gross"].sum()*100:.4f}%')

print()
print('--- Top 15 highest fee transactions ---')
print(trace[['order_id','order_pattern','rzp_gross','rzp_fee','rzp_tax','method',
             'card_network','card_type','wiom_table','wiom_mobile','wiom_plan']].head(15).to_string(index=False))

# Export
trace.to_csv('docs/razorpay_fee_trace_jan26.csv', index=False)
print(f'\nExported: docs/razorpay_fee_trace_jan26.csv ({len(trace):,} rows)')

con.close()
print('\n' + SEP)
