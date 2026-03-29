# -*- coding: utf-8 -*-
"""Trace 243 PayU refunded/chargebacked txns back to Wiom DB. 1 row per order."""
import duckdb
import pandas as pd

con = duckdb.connect('data.duckdb', read_only=True)

print('=' * 100)
print('TRACE: 243 PayU Refunded/Chargebacked Txns -> Juspay -> Wiom DB')
print('=' * 100)

# ============================================================
# Single query: join everything, prioritise wiom_refunded > wiom_primary_revenue
# ============================================================
df = con.execute("""
    WITH mismatch AS (
        SELECT
            j.order_id,
            j.juspay_txn_id,
            j.amount                        AS juspay_amount,
            j.payment_status                AS juspay_status,
            j.payment_gateway               AS juspay_gateway,
            j.order_date_created            AS juspay_order_date,
            j.customer_id                   AS juspay_customer_id,
            j.description                   AS juspay_description,
            j.payment_flow,
            p.txnid                         AS payu_txnid,
            p.status                        AS payu_status,
            CAST(p.amount AS DOUBLE)        AS payu_amount,
            j.amount - CAST(p.amount AS DOUBLE) AS amount_diff,
            p.addedon                       AS payu_date,
            p.mode                          AS payu_mode,
            p.bank_name                     AS payu_bank,
            CASE
                WHEN j.order_id LIKE 'custGen_%'    THEN 'custGen_*'
                WHEN j.order_id LIKE 'custWgSubs_%' THEN 'custWgSubs_*'
                WHEN j.order_id LIKE 'cusSubs_%'    THEN 'cusSubs_*'
                WHEN j.order_id LIKE 'w_%'          THEN 'w_*'
                WHEN j.order_id LIKE 'mr_%'         THEN 'mr_*'
                WHEN j.order_id LIKE 'cxTeam_%'     THEN 'cxTeam_*'
                ELSE 'other'
            END AS order_pattern
        FROM juspay_transactions j
        INNER JOIN payu_transactions p ON j.juspay_txn_id = p.txnid
        WHERE j.payment_status = 'SUCCESS'
        AND j.source_month = 'Jan26'
        AND j.payment_gateway = 'PAYU'
        AND j.amount != CAST(p.amount AS DOUBLE)
    ),

    -- Juspay refund record (one per order max)
    juspay_ref AS (
        SELECT DISTINCT ON (order_id)
            order_id,
            refund_status,
            refund_amount,
            refund_date,
            refund_type,
            refund_unique_id AS refund_id
        FROM juspay_refunds
        WHERE refund_status = 'SUCCESS'
        ORDER BY order_id, refund_date DESC
    ),

    -- Wiom refunded_transactions (priority table)
    wiom_rf AS (
        SELECT
            TRANSACTION_ID                  AS order_id,
            'wiom_refunded_transactions'    AS wiom_table,
            CAST(DT AS VARCHAR)             AS wiom_date,
            CAST(PAY_AMMOUNT AS DOUBLE)     AS wiom_amount,
            PAYMENT_MODE                    AS wiom_mode,
            CAST(PAYMENT_STATUS AS VARCHAR) AS wiom_payment_status,
            CAST(MOBILE AS VARCHAR)         AS wiom_mobile,
            CAST(REFUND_STATUS AS VARCHAR)  AS wiom_refund_status,
            CAST(PAYMENT_TYPE AS VARCHAR)   AS wiom_payment_type,
            TXN_STATUS                      AS wiom_txn_status
        FROM wiom_refunded_transactions
    ),

    -- Wiom primary_revenue (fallback)
    wiom_pr AS (
        SELECT
            TRANSACTION_ID                  AS order_id,
            'wiom_primary_revenue'          AS wiom_table,
            RECHARGE_DT                     AS wiom_date,
            CAST(TOTALPAID AS DOUBLE)       AS wiom_amount,
            MODE                            AS wiom_mode,
            NULL::VARCHAR                   AS wiom_payment_status,
            CAST(MOBILE AS VARCHAR)         AS wiom_mobile,
            NULL::VARCHAR                   AS wiom_refund_status,
            PLAN_TYPE                       AS wiom_payment_type,
            NULL::VARCHAR                   AS wiom_txn_status
        FROM wiom_primary_revenue
    )

    SELECT
        m.order_id,
        m.order_pattern,

        -- Juspay
        m.juspay_amount,
        m.juspay_order_date,
        m.juspay_customer_id,
        m.juspay_description,
        m.payment_flow          AS juspay_payment_flow,

        -- PayU
        m.payu_amount,
        m.amount_diff,
        m.payu_status,
        m.payu_date,
        m.payu_mode,
        m.payu_bank,

        -- Wiom DB: prefer refunded_transactions, fall back to primary_revenue
        COALESCE(rf.wiom_table,       pr.wiom_table)           AS wiom_table,
        COALESCE(rf.wiom_date,   pr.wiom_date)   AS wiom_date,
        COALESCE(rf.wiom_amount, pr.wiom_amount) AS wiom_amount,
        COALESCE(rf.wiom_mode,   pr.wiom_mode)   AS wiom_mode,
        COALESCE(rf.wiom_mobile, pr.wiom_mobile) AS wiom_mobile,
        rf.wiom_payment_status                                  AS wiom_payment_status,
        rf.wiom_refund_status                                   AS wiom_refund_status,
        rf.wiom_txn_status                                      AS wiom_txn_status,
        COALESCE(rf.wiom_payment_type, pr.wiom_payment_type)   AS wiom_payment_type,
        CASE
            WHEN rf.order_id IS NOT NULL AND pr.order_id IS NOT NULL THEN 'BOTH (RF+PR)'
            WHEN rf.order_id IS NOT NULL THEN 'refunded_transactions'
            WHEN pr.order_id IS NOT NULL THEN 'primary_revenue only'
            ELSE 'NOT FOUND'
        END AS wiom_presence,

        -- Juspay refund
        jr.refund_status        AS juspay_refund_status,
        jr.refund_amount        AS juspay_refund_amount,
        jr.refund_date          AS juspay_refund_date,
        jr.refund_type          AS juspay_refund_type,
        jr.refund_id            AS juspay_refund_id,

        -- IDs for tracing
        m.juspay_txn_id,
        m.payu_txnid

    FROM mismatch m
    LEFT JOIN wiom_rf rf ON m.order_id = rf.order_id
    LEFT JOIN wiom_pr pr ON m.order_id = pr.order_id
    LEFT JOIN juspay_ref jr ON m.order_id = jr.order_id
    ORDER BY m.juspay_amount DESC
""").fetchdf()

# ============================================================
# Print summary
# ============================================================
print(f'\nTotal orders traced : {len(df):,}')
print()

print('--- PayU status breakdown ---')
for val, cnt in df['payu_status'].value_counts().items():
    sub = df[df['payu_status']==val]
    print(f'  {str(val):22s}  {cnt:>5,} txns  |  Juspay Rs {sub["juspay_amount"].sum():>10,.0f}  |  PayU Rs {sub["payu_amount"].sum():>8,.0f}  |  Diff Rs {sub["amount_diff"].sum():>8,.0f}')

print()
print('--- Wiom DB presence ---')
for val, cnt in df['wiom_presence'].value_counts().items():
    sub = df[df['wiom_presence']==val]
    print(f'  {str(val):30s}  {cnt:>5,} txns  Juspay Rs {sub["juspay_amount"].sum():>10,.0f}')

print()
print('--- Juspay refund record present? ---')
has_ref  = df['juspay_refund_status'].notna()
print(f'  Has Juspay refund record : {has_ref.sum():,}  Rs {df.loc[has_ref,"juspay_refund_amount"].sum():,.0f}')
print(f'  No Juspay refund record  : {(~has_ref).sum():,}  Rs {df.loc[~has_ref,"juspay_amount"].sum():,.0f}')
print()
no_ref = df[~has_ref]
if len(no_ref):
    print(f'  --> {len(no_ref)} txns with NO Juspay refund record, payu_status breakdown:')
    for val, cnt in no_ref['payu_status'].value_counts().items():
        sub = no_ref[no_ref['payu_status']==val]
        print(f'       {str(val):22s}  {cnt:>5,}  Rs {sub["juspay_amount"].sum():>8,.0f}')

print()
print('--- order_id pattern ---')
for val, cnt in df['order_pattern'].value_counts().items():
    sub = df[df['order_pattern']==val]
    print(f'  {str(val):20s}  {cnt:>5,}  Juspay Rs {sub["juspay_amount"].sum():>10,.0f}  PayU Rs {sub["payu_amount"].sum():>8,.0f}')

print()
print('--- Wiom refund_status (from refunded_transactions) ---')
for val, cnt in df['wiom_refund_status'].value_counts(dropna=False).items():
    print(f'  {str(val):10s}  {cnt:>5,}')

print()
print('--- Wiom payment_status (from refunded_transactions) ---')
for val, cnt in df['wiom_payment_status'].value_counts(dropna=False).items():
    print(f'  {str(val):10s}  {cnt:>5,}')

print()
print('--- Amount 3-way comparison: Wiom vs Juspay vs PayU ---')
df_has_wiom = df[df['wiom_amount'].notna()].copy()
df_has_wiom['wiom_amount_num'] = pd.to_numeric(df_has_wiom['wiom_amount'], errors='coerce')
print(f'  Wiom total   : Rs {df_has_wiom["wiom_amount_num"].sum():>12,.0f}')
print(f'  Juspay total : Rs {df_has_wiom["juspay_amount"].sum():>12,.0f}')
print(f'  PayU total   : Rs {df_has_wiom["payu_amount"].sum():>12,.0f}')

# ============================================================
# Export CSV
# ============================================================
csv_cols = [
    'order_id', 'order_pattern', 'wiom_presence',
    'wiom_table', 'wiom_date', 'wiom_amount', 'wiom_mobile',
    'wiom_mode', 'wiom_payment_status', 'wiom_refund_status',
    'wiom_txn_status', 'wiom_payment_type',
    'juspay_amount', 'juspay_order_date', 'juspay_customer_id',
    'juspay_description', 'juspay_payment_flow',
    'payu_status', 'payu_amount', 'amount_diff',
    'payu_date', 'payu_mode', 'payu_bank',
    'juspay_refund_status', 'juspay_refund_amount',
    'juspay_refund_date', 'juspay_refund_type', 'juspay_refund_id',
    'juspay_txn_id', 'payu_txnid'
]
df[csv_cols].to_csv('docs/payu_refund_trace_jan26.csv', index=False)
print(f'\nExported: docs/payu_refund_trace_jan26.csv  ({len(df):,} rows, {len(csv_cols)} columns)')

con.close()
print('=' * 100)
