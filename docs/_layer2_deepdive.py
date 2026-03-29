# -*- coding: utf-8 -*-
"""Layer 2 Deep Dive: Why do Juspay vs PG amounts differ? (Jan 2026)"""
import duckdb

con = duckdb.connect('data.duckdb', read_only=True)

print('=' * 100)
print('LAYER 2 DEEP DIVE — JUSPAY vs PG AMOUNT / ORPHAN ANALYSIS (JAN 2026)')
print('=' * 100)

# ============================================================
# SECTION 1: PAYU — 244 txns where juspay.amount != payu.amount
# ============================================================
print('\n\n### SECTION 1: PAYU -- Amount Mismatch Deep Dive ###\n')

df_payu = con.execute("""
    SELECT
        j.order_id,
        j.amount                        AS juspay_amt,
        CAST(p.amount AS DOUBLE)        AS payu_amt,
        j.amount - CAST(p.amount AS DOUBLE) AS diff,
        p.status                        AS payu_status,
        p.addedon,
        p.mode,
        p.discount,
        p.additional_charges,
        CAST(p."amount(inr)" AS DOUBLE) AS amount_inr,
        CAST(p.merchant_subvention_amount AS DOUBLE) AS merchant_subvention,
        p.errorDescription,
        p.bank_name,
        j.payment_flow,
        j.txn_flow_type
    FROM juspay_transactions j
    INNER JOIN payu_transactions p ON j.juspay_txn_id = p.txnid
    WHERE j.payment_status='SUCCESS' AND j.source_month='Jan26'
    AND j.payment_gateway='PAYU'
    AND j.amount != CAST(p.amount AS DOUBLE)
    ORDER BY diff DESC
""").fetchdf()

total_diff = df_payu['diff'].sum()
zero_rows  = df_payu[df_payu['payu_amt'] == 0]
part_rows  = df_payu[df_payu['payu_amt'] >  0]

print(f'Total mismatched rows          : {len(df_payu):,}')
print(f'Total Juspay amount (mismatch) : Rs {df_payu["juspay_amt"].sum():,.0f}')
print(f'Total PayU amount  (mismatch)  : Rs {df_payu["payu_amt"].sum():,.0f}')
print(f'Total diff (Juspay - PayU)     : Rs {total_diff:,.0f}')
print()

print(f'--- Split: payu.amount = 0 vs partial ---')
print(f'  payu.amount = 0    : {len(zero_rows):>5,} txns  |  Juspay Rs {zero_rows["juspay_amt"].sum():>10,.0f}  |  PayU Rs 0')
print(f'  payu.amount > 0    : {len(part_rows):>5,} txns  |  Juspay Rs {part_rows["juspay_amt"].sum():>10,.0f}  |  PayU Rs {part_rows["payu_amt"].sum():>10,.0f}  |  Diff Rs {part_rows["diff"].sum():>8,.0f}')

print('\n--- PayU status breakdown ---')
for val, cnt in df_payu['payu_status'].value_counts().items():
    sub = df_payu[df_payu['payu_status'] == val]
    print(f'  {str(val):20s}  {cnt:>5,} txns  PayU Rs {sub["payu_amt"].sum():>10,.0f}  Juspay Rs {sub["juspay_amt"].sum():>10,.0f}')

print('\n--- PayU mode breakdown ---')
for val, cnt in df_payu['mode'].value_counts().items():
    sub = df_payu[df_payu['mode'] == val]
    print(f'  {str(val):25s}  {cnt:>5,} txns  Juspay Rs {sub["juspay_amt"].sum():>10,.0f}  PayU Rs {sub["payu_amt"].sum():>10,.0f}')

print('\n--- PayU errorDescription (top 10) ---')
for val, cnt in df_payu['errorDescription'].value_counts().head(10).items():
    print(f'  {str(val):50s}  {cnt:>5,} txns')

print('\n--- Merchant subvention (Wiom subsidises customer) ---')
has_subvention = df_payu[df_payu['merchant_subvention'] > 0]
print(f'  Rows with merchant_subvention > 0  : {len(has_subvention):,}')
if len(has_subvention) > 0:
    print(f'  Total merchant_subvention          : Rs {has_subvention["merchant_subvention"].sum():,.0f}')

print('\n--- discount column ---')
has_discount = df_payu[df_payu['discount'].notna() & (df_payu['discount'] != 0)]
print(f'  Rows with discount != 0/null: {len(has_discount):,}  Total discount: {has_discount["discount"].sum() if len(has_discount)>0 else 0}')

print('\n--- payment flow for mismatched txns ---')
for val, cnt in df_payu['payment_flow'].value_counts().items():
    print(f'  {str(val):25s}  {cnt:>5,}')
for val, cnt in df_payu['txn_flow_type'].value_counts().items():
    print(f'  txn_flow={str(val):20s}  {cnt:>5,}')

print('\n--- order_id pattern for mismatched txns ---')
import re
df_payu['pattern'] = df_payu['order_id'].apply(
    lambda x: re.match(r'^([a-zA-Z]+)_', str(x)).group(1) + '_*' if re.match(r'^([a-zA-Z]+)_', str(x)) else 'other'
)
for val, cnt in df_payu['pattern'].value_counts().items():
    sub = df_payu[df_payu['pattern'] == val]
    print(f'  {str(val):20s}  {cnt:>5,} txns  Juspay Rs {sub["juspay_amt"].sum():>10,.0f}  PayU Rs {sub["payu_amt"].sum():>8,.0f}')

print('\n--- Top 15 rows by diff ---')
print(f'{"order_id":45s} {"Juspay":>8s} {"PayU":>8s} {"Diff":>8s} {"amt_inr":>8s} {"subvention":>12s} {"mode":>10s} {"bank":>15s}')
print('-' * 120)
for _, row in df_payu.head(15).iterrows():
    print(f'{row["order_id"]:45s} {row["juspay_amt"]:>8,.0f} {row["payu_amt"]:>8,.0f} {row["diff"]:>8,.0f} {str(row["amount_inr"]):>8} {str(row["merchant_subvention"]):>12} {str(row["mode"]):>10} {str(row["bank_name"]):>15}')

# Cross-check: what does PayU show for those 0-amount rows — any other amount cols?
print('\n--- For payu.amount=0 rows: checking all PayU amount-related columns ---')
zero_txnids = zero_rows['order_id'].tolist()[:5]
if zero_txnids:
    placeholders = ','.join(["'" + x + "'" for x in zero_txnids])
    sample = con.execute(f"""
        SELECT txnid, amount, "amount(inr)", settlement_amount, merchant_subvention_amount,
               discount, additional_charges, service_fees, mode, bank_name, status, errorDescription
        FROM payu_transactions
        WHERE txnid IN (
            SELECT juspay_txn_id FROM juspay_transactions
            WHERE order_id IN ({placeholders}) AND source_month='Jan26'
        )
    """).fetchdf()
    print(sample.to_string(index=False))


# ============================================================
# SECTION 2: PAYTM — 35 PG-only txns (in Paytm but not Juspay Jan26)
# ============================================================
print('\n\n### SECTION 2: PAYTM -- 35 PG-only txns Deep Dive ###\n')

df_ptm_only = con.execute("""
    SELECT
        TRIM(p.Order_ID, chr(39))    AS order_id,
        p.Amount,
        TRIM(p.Transaction_Date, chr(39)) AS txn_date,
        TRIM(p.Transaction_ID, chr(39))   AS txn_id,
        -- check if this order_id exists in juspay at all (any status, any month)
        MAX(CASE WHEN j.order_id IS NOT NULL THEN 1 ELSE 0 END) AS in_juspay_any,
        MAX(j.source_month)          AS juspay_month,
        MAX(j.payment_status)        AS juspay_status
    FROM paytm_transactions p
    LEFT JOIN juspay_transactions j
        ON TRIM(p.Order_ID, chr(39)) = j.juspay_txn_id
    WHERE TRIM(p.Transaction_Date, chr(39)) LIKE '2026-01%'
    AND NOT EXISTS (
        SELECT 1 FROM juspay_transactions jj
        WHERE jj.payment_status='SUCCESS' AND jj.source_month='Jan26'
        AND TRIM(p.Order_ID, chr(39)) = TRIM(jj.juspay_txn_id, chr(39))
    )
    GROUP BY p.Order_ID, p.Amount, p.Transaction_Date, p.Transaction_ID
    ORDER BY p.Amount DESC
""").fetchdf()

print(f'Total Paytm-only txns (Jan26, not in Juspay Jan26 SUCCESS): {len(df_ptm_only):,}')
print(f'Total amount: Rs {df_ptm_only["Amount"].sum():,.0f}')
print()
print('--- Are they in Juspay at all (any status/month)? ---')
print(df_ptm_only['in_juspay_any'].value_counts().to_string())
print()
print('--- For those in Juspay: which month and status? ---')
in_j = df_ptm_only[df_ptm_only['in_juspay_any'] == 1]
if len(in_j) > 0:
    print(f'  Count: {len(in_j)}')
    print('  Months:', in_j['juspay_month'].value_counts().to_string())
    print('  Statuses:', in_j['juspay_status'].value_counts().to_string())
print()
print('--- Full list (all 35) ---')
print(f'{"order_id":50s} {"Amount":>8s} {"txn_date":>22s} {"in_juspay":>10s} {"j_month":>10s} {"j_status":>12s}')
print('-' * 120)
for _, row in df_ptm_only.iterrows():
    print(f'{str(row["order_id"]):50s} {row["Amount"]:>8,.0f} {str(row["txn_date"]):>22s} {str(row["in_juspay_any"]):>10s} {str(row["juspay_month"]):>10s} {str(row["juspay_status"]):>12s}')


# ============================================================
# SECTION 3: PAYU — 2 PG-only txns
# ============================================================
print('\n\n### SECTION 3: PAYU -- 2 PG-only txns Deep Dive ###\n')

df_payu_only = con.execute("""
    SELECT
        p.txnid,
        CAST(p.amount AS DOUBLE) AS payu_amt,
        p.addedon,
        p.status,
        p.mode,
        -- check juspay any month
        MAX(CASE WHEN j.order_id IS NOT NULL THEN 1 ELSE 0 END) AS in_juspay_any,
        MAX(j.source_month)   AS juspay_month,
        MAX(j.payment_status) AS juspay_status,
        MAX(j.amount)         AS juspay_amt
    FROM payu_transactions p
    LEFT JOIN juspay_transactions j ON p.txnid = j.juspay_txn_id
    WHERE p.addedon LIKE '%-01-2026%' AND p.status = 'captured'
    AND NOT EXISTS (
        SELECT 1 FROM juspay_transactions jj
        WHERE jj.payment_status='SUCCESS' AND jj.source_month='Jan26'
        AND p.txnid = jj.juspay_txn_id
    )
    GROUP BY p.txnid, p.amount, p.addedon, p.status, p.mode
""").fetchdf()

print(f'Total PayU-only captured txns (Jan26): {len(df_payu_only):,}')
print()
print(df_payu_only.to_string(index=False))

con.close()
print('\n' + '=' * 100)
print('END OF DEEP DIVE')
print('=' * 100)
