# -*- coding: utf-8 -*-
"""
January 2026 Reverse Reconciliation: Bank -> Settlement -> Juspay -> Wiom DB
=============================================================================
Direction: Bank deposits (Layer 4) traced forward to Settlement (3b -> 3a),
           then to Juspay (Layer 2), then to Wiom DB (Layer 1).

Scope: All settlement rows where settled_date falls in January 2026.

Output:
  - Persistent DuckDB table: recon_jan26_base
  - CSV exports: docs/gap_settlement_no_juspay_jan26.csv
                 docs/gap_juspay_no_wiom_jan26.csv
"""
import duckdb
import os

DB_PATH  = 'data.duckdb'
DOCS_DIR = 'docs'

con = duckdb.connect(DB_PATH)

SEP  = '=' * 130
SEP2 = '-' * 130

print(SEP)
print('JANUARY 2026 REVERSE RECONCILIATION  --  Bank -> Settlement -> Juspay -> Wiom DB')
print(SEP)

# ============================================================
# PART 0: BUILD BASE TABLE  (per-gateway, step-by-step)
# ============================================================
print('\n' + SEP)
print('PART 0: BUILDING BASE TABLE  recon_jan26_base')
print(SEP)

# --- Step 0a: Pre-build juspay lookup per gateway (indexed temp tables) ---
print('  0a. Pre-building Juspay lookup tables ...')
con.execute("DROP TABLE IF EXISTS _tmp_juspay_paytm")
con.execute("""
    CREATE TEMP TABLE _tmp_juspay_paytm AS
    SELECT juspay_txn_id,
           order_id,
           CAST(amount AS DOUBLE) AS juspay_amount,
           order_status AS juspay_status,
           payment_gateway AS juspay_gateway,
           CAST(order_date_created AS DATE) AS juspay_created_date
    FROM juspay_transactions
    WHERE payment_gateway = 'PAYTM_V2'
""")
con.execute("DROP TABLE IF EXISTS _tmp_juspay_phonepe")
con.execute("""
    CREATE TEMP TABLE _tmp_juspay_phonepe AS
    SELECT juspay_txn_id,
           order_id,
           CAST(amount AS DOUBLE) AS juspay_amount,
           order_status AS juspay_status,
           payment_gateway AS juspay_gateway,
           CAST(order_date_created AS DATE) AS juspay_created_date
    FROM juspay_transactions
    WHERE payment_gateway = 'PHONEPE'
""")
con.execute("DROP TABLE IF EXISTS _tmp_juspay_payu")
con.execute("""
    CREATE TEMP TABLE _tmp_juspay_payu AS
    SELECT juspay_txn_id,
           order_id,
           CAST(amount AS DOUBLE) AS juspay_amount,
           order_status AS juspay_status,
           payment_gateway AS juspay_gateway,
           CAST(order_date_created AS DATE) AS juspay_created_date
    FROM juspay_transactions
    WHERE payment_gateway = 'PAYU'
""")
con.execute("DROP TABLE IF EXISTS _tmp_juspay_razorpay")
con.execute("""
    CREATE TEMP TABLE _tmp_juspay_razorpay AS
    SELECT order_id,
           juspay_txn_id,
           CAST(amount AS DOUBLE) AS juspay_amount,
           order_status AS juspay_status,
           payment_gateway AS juspay_gateway,
           CAST(order_date_created AS DATE) AS juspay_created_date
    FROM juspay_transactions
    WHERE payment_gateway = 'RAZORPAY'
""")
print('  Juspay lookups done.')

# --- Step 0b: Pre-build bank daily lookup per gateway ---
print('  0b. Pre-building bank daily lookup ...')
con.execute("DROP TABLE IF EXISTS _tmp_bank_daily")
con.execute("""
    CREATE TEMP TABLE _tmp_bank_daily AS
    SELECT "Payment Gateway" AS gw_name,
           CAST("Transaction" AS DATE) AS bank_date,
           SUM(CAST("Deposit Amt(INR)" AS DOUBLE)) AS bank_daily_deposit
    FROM bank_receipt_from_pg
    GROUP BY 1, 2
""")
print('  Bank lookups done.')

# --- Step 0c: Pre-build Wiom lookup table (order_id -> wiom_txn, wiom_table, wiom_amount) ---
print('  0c. Pre-building Wiom ID lookup ...')
con.execute("DROP TABLE IF EXISTS _tmp_wiom_lookup")
con.execute("""
    CREATE TEMP TABLE _tmp_wiom_lookup AS
    SELECT BOOKING_TXN_ID AS order_id,
           BOOKING_TXN_ID AS wiom_txn_id,
           'wiom_booking_transactions' AS wiom_table,
           CAST(BOOKING_FEE AS DOUBLE) AS wiom_amount
    FROM wiom_booking_transactions
    UNION ALL
    SELECT TRANSACTION_ID AS order_id,
           TRANSACTION_ID AS wiom_txn_id,
           'wiom_primary_revenue' AS wiom_table,
           CAST(TOTALPAID AS DOUBLE) AS wiom_amount
    FROM wiom_primary_revenue
    UNION ALL
    SELECT TXN_ID AS order_id,
           TXN_ID AS wiom_txn_id,
           'wiom_net_income' AS wiom_table,
           CAST(AMOUNT AS DOUBLE) AS wiom_amount
    FROM wiom_net_income
    UNION ALL
    SELECT TRANSACTION_ID AS order_id,
           TRANSACTION_ID AS wiom_txn_id,
           'wiom_mobile_recharge_transactions' AS wiom_table,
           CAST(PAY_AMMOUNT AS DOUBLE) AS wiom_amount
    FROM wiom_mobile_recharge_transactions
    UNION ALL
    SELECT SD_TXN_ID AS order_id,
           SD_TXN_ID AS wiom_txn_id,
           'wiom_customer_security_deposit' AS wiom_table,
           CAST(SD_AMOUNT AS DOUBLE) AS wiom_amount
    FROM wiom_customer_security_deposit
    UNION ALL
    SELECT TRANSACTION_ID AS order_id,
           TRANSACTION_ID AS wiom_txn_id,
           'wiom_ott_transactions' AS wiom_table,
           CAST(PAY_AMMOUNT AS DOUBLE) AS wiom_amount
    FROM wiom_ott_transactions
""")
# If same order_id appears in multiple tables, take first match (booking > primary_rev > net > mr > sd > ott)
# Create a deduplicated version with priority ordering
con.execute("DROP TABLE IF EXISTS _tmp_wiom_lookup_dedup")
con.execute("""
    CREATE TEMP TABLE _tmp_wiom_lookup_dedup AS
    SELECT order_id, wiom_txn_id, wiom_table, wiom_amount
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY order_id
                   ORDER BY CASE wiom_table
                       WHEN 'wiom_booking_transactions'          THEN 1
                       WHEN 'wiom_primary_revenue'               THEN 2
                       WHEN 'wiom_net_income'                    THEN 3
                       WHEN 'wiom_mobile_recharge_transactions'  THEN 4
                       WHEN 'wiom_customer_security_deposit'     THEN 5
                       WHEN 'wiom_ott_transactions'              THEN 6
                       ELSE 7
                   END
               ) AS rn
        FROM _tmp_wiom_lookup
        WHERE order_id IS NOT NULL
    ) t
    WHERE rn = 1
""")
print('  Wiom lookup done.')

# --- Utility function to derive l1_match and trace_status ---
# We embed this logic in SQL

# --- Helper SQL snippet for final column derivation ---
FINAL_COLS = """
    CASE
        WHEN j.order_id IS NULL THEN 'NO_JUSPAY'
        WHEN w.wiom_txn_id IS NOT NULL THEN 'MATCHED'
        WHEN j.order_id LIKE 'WIFI_SRVC_%' OR j.order_id LIKE 'BILL_PAID_%' THEN 'PARTNER_WALLET'
        ELSE 'JUSPAY_ONLY'
    END AS l1_match,
    SPLIT_PART(COALESCE(j.order_id, s_oid), '_', 1) AS order_id_prefix,
    CASE
        WHEN j.order_id IS NULL THEN 'MISSING_JUSPAY'
        WHEN j.order_id LIKE 'WIFI_SRVC_%' OR j.order_id LIKE 'BILL_PAID_%' THEN 'PARTNER_WALLET'
        WHEN w.wiom_txn_id IS NOT NULL THEN 'FULLY_TRACED'
        ELSE 'MISSING_WIOM'
    END AS trace_status
"""

# --- Step 0d: Create empty base table ---
print('  0d. Creating empty base table ...')
con.execute("DROP TABLE IF EXISTS recon_jan26_base")
con.execute("""
    CREATE TABLE recon_jan26_base (
        gateway                VARCHAR,
        settlement_order_id    VARCHAR,
        bank_date              DATE,
        bank_daily_deposit     DOUBLE,
        settled_date           DATE,
        sett_gross             DOUBLE,
        sett_net               DOUBLE,
        sett_fee               DOUBLE,
        sett_utr               VARCHAR,
        sett_txn_type          VARCHAR,
        juspay_order_id        VARCHAR,
        juspay_txn_id          VARCHAR,
        juspay_amount          DOUBLE,
        juspay_status          VARCHAR,
        juspay_gateway         VARCHAR,
        juspay_created_date    DATE,
        l2_match               VARCHAR,
        wiom_txn_id            VARCHAR,
        wiom_table             VARCHAR,
        wiom_amount            DOUBLE,
        l1_match               VARCHAR,
        order_id_prefix        VARCHAR,
        trace_status           VARCHAR
    )
""")

# ============================================================
# GATEWAY 1: PAYTM_V2
# ============================================================
print('\n  Building PAYTM_V2 rows ...')
con.execute("""
    INSERT INTO recon_jan26_base
    SELECT
        'PAYTM_V2'                                               AS gateway,
        REPLACE(s.order_id, chr(39), '')                        AS settlement_order_id,
        b.bank_date                                              AS bank_date,
        b.bank_daily_deposit                                     AS bank_daily_deposit,
        s.settled_date,
        CAST(s.amount AS DOUBLE)                                 AS sett_gross,
        CAST(s.settled_amount AS DOUBLE)                        AS sett_net,
        CAST(COALESCE(s.commission,0)+COALESCE(s.gst,0) AS DOUBLE) AS sett_fee,
        CAST(s.utr_no AS VARCHAR)                               AS sett_utr,
        CAST(s.transaction_type AS VARCHAR)                     AS sett_txn_type,
        j.order_id                                              AS juspay_order_id,
        j.juspay_txn_id,
        j.juspay_amount,
        j.juspay_status,
        j.juspay_gateway,
        j.juspay_created_date,
        CASE WHEN j.order_id IS NOT NULL THEN 'MATCHED' ELSE 'SETT_ONLY' END AS l2_match,
        w.wiom_txn_id,
        w.wiom_table,
        w.wiom_amount,
        CASE
            WHEN j.order_id IS NULL THEN 'NO_JUSPAY'
            WHEN w.wiom_txn_id IS NOT NULL THEN 'MATCHED'
            WHEN j.order_id LIKE 'WIFI_SRVC_%' OR j.order_id LIKE 'BILL_PAID_%' THEN 'PARTNER_WALLET'
            ELSE 'JUSPAY_ONLY'
        END AS l1_match,
        SPLIT_PART(COALESCE(j.order_id, REPLACE(s.order_id, chr(39), '')), '_', 1) AS order_id_prefix,
        CASE
            WHEN j.order_id IS NULL THEN 'MISSING_JUSPAY'
            WHEN j.order_id LIKE 'WIFI_SRVC_%' OR j.order_id LIKE 'BILL_PAID_%' THEN 'PARTNER_WALLET'
            WHEN w.wiom_txn_id IS NOT NULL THEN 'FULLY_TRACED'
            ELSE 'MISSING_WIOM'
        END AS trace_status
    FROM paytm_settlements s
    LEFT JOIN _tmp_bank_daily b
        ON b.gw_name = '01 Paytm-Wallet (WIOM Gold)' AND b.bank_date = s.settled_date
    LEFT JOIN _tmp_juspay_paytm j
        ON REPLACE(s.order_id, chr(39), '') = j.juspay_txn_id
    LEFT JOIN _tmp_wiom_lookup_dedup w
        ON j.order_id = w.order_id
    WHERE YEAR(s.settled_date) = 2026
      AND MONTH(s.settled_date) = 1
      AND s.transaction_type = 'ACQUIRING'
""")
n_ptm = con.execute("SELECT COUNT(*) FROM recon_jan26_base WHERE gateway='PAYTM_V2'").fetchone()[0]
print(f'  PAYTM_V2: {n_ptm:,} rows inserted.')

# ============================================================
# GATEWAY 2: PHONEPE
# ============================================================
print('  Building PHONEPE rows ...')
con.execute("""
    INSERT INTO recon_jan26_base
    SELECT
        'PHONEPE'                                                AS gateway,
        CAST(s."Merchant Order Id" AS VARCHAR)                  AS settlement_order_id,
        b.bank_date,
        b.bank_daily_deposit,
        CAST(s."Settlement Date" AS DATE)                       AS settled_date,
        CAST(s."Transaction Amount" AS DOUBLE)                  AS sett_gross,
        CAST(s."Net Amount" AS DOUBLE)                          AS sett_net,
        ABS(COALESCE(CAST(s."Total Fees" AS DOUBLE), 0))        AS sett_fee,
        CAST(s."Settlement UTR" AS VARCHAR)                     AS sett_utr,
        CAST(s."Transaction Type" AS VARCHAR)                   AS sett_txn_type,
        j.order_id                                              AS juspay_order_id,
        j.juspay_txn_id,
        j.juspay_amount,
        j.juspay_status,
        j.juspay_gateway,
        j.juspay_created_date,
        CASE WHEN j.order_id IS NOT NULL THEN 'MATCHED' ELSE 'SETT_ONLY' END AS l2_match,
        w.wiom_txn_id,
        w.wiom_table,
        w.wiom_amount,
        CASE
            WHEN j.order_id IS NULL THEN 'NO_JUSPAY'
            WHEN w.wiom_txn_id IS NOT NULL THEN 'MATCHED'
            WHEN j.order_id LIKE 'WIFI_SRVC_%' OR j.order_id LIKE 'BILL_PAID_%' THEN 'PARTNER_WALLET'
            ELSE 'JUSPAY_ONLY'
        END AS l1_match,
        SPLIT_PART(COALESCE(j.order_id, CAST(s."Merchant Order Id" AS VARCHAR)), '_', 1) AS order_id_prefix,
        CASE
            WHEN j.order_id IS NULL THEN 'MISSING_JUSPAY'
            WHEN j.order_id LIKE 'WIFI_SRVC_%' OR j.order_id LIKE 'BILL_PAID_%' THEN 'PARTNER_WALLET'
            WHEN w.wiom_txn_id IS NOT NULL THEN 'FULLY_TRACED'
            ELSE 'MISSING_WIOM'
        END AS trace_status
    FROM phonepe_settlements s
    LEFT JOIN _tmp_bank_daily b
        ON b.gw_name = '05 PhonePe Wallet-2'
        AND b.bank_date = CAST(s."Settlement Date" AS DATE)
    LEFT JOIN _tmp_juspay_phonepe j
        ON CAST(s."Merchant Order Id" AS VARCHAR) = j.juspay_txn_id
    LEFT JOIN _tmp_wiom_lookup_dedup w
        ON j.order_id = w.order_id
    WHERE YEAR(CAST(s."Settlement Date" AS DATE)) = 2026
      AND MONTH(CAST(s."Settlement Date" AS DATE)) = 1
      AND s."Transaction Type" NOT LIKE '%REVERSAL%'
      AND s."Transaction Type" NOT LIKE '%REFUND%'
      AND s."Transaction Type" IS NOT NULL
""")
n_pp = con.execute("SELECT COUNT(*) FROM recon_jan26_base WHERE gateway='PHONEPE'").fetchone()[0]
print(f'  PHONEPE: {n_pp:,} rows inserted.')

# ============================================================
# GATEWAY 3: PAYU
# ============================================================
print('  Building PAYU rows ...')
con.execute("""
    INSERT INTO recon_jan26_base
    SELECT
        'PAYU'                                                   AS gateway,
        CAST(s."Merchant Txn ID" AS VARCHAR)                    AS settlement_order_id,
        b.bank_date,
        b.bank_daily_deposit,
        CAST(LEFT(CAST(s."AddedOn" AS VARCHAR), 10) AS DATE)    AS settled_date,
        CAST(s."Amount" AS DOUBLE)                              AS sett_gross,
        CAST(s."Net Amount" AS DOUBLE)                          AS sett_net,
        CAST(COALESCE(s."Amount",0) - COALESCE(s."Net Amount",0) AS DOUBLE) AS sett_fee,
        CAST(s."Merchant UTR" AS VARCHAR)                       AS sett_utr,
        CAST(s."Status" AS VARCHAR)                             AS sett_txn_type,
        j.order_id                                              AS juspay_order_id,
        j.juspay_txn_id,
        j.juspay_amount,
        j.juspay_status,
        j.juspay_gateway,
        j.juspay_created_date,
        CASE WHEN j.order_id IS NOT NULL THEN 'MATCHED' ELSE 'SETT_ONLY' END AS l2_match,
        w.wiom_txn_id,
        w.wiom_table,
        w.wiom_amount,
        CASE
            WHEN j.order_id IS NULL THEN 'NO_JUSPAY'
            WHEN w.wiom_txn_id IS NOT NULL THEN 'MATCHED'
            WHEN j.order_id LIKE 'WIFI_SRVC_%' OR j.order_id LIKE 'BILL_PAID_%' THEN 'PARTNER_WALLET'
            ELSE 'JUSPAY_ONLY'
        END AS l1_match,
        SPLIT_PART(COALESCE(j.order_id, CAST(s."Merchant Txn ID" AS VARCHAR)), '_', 1) AS order_id_prefix,
        CASE
            WHEN j.order_id IS NULL THEN 'MISSING_JUSPAY'
            WHEN j.order_id LIKE 'WIFI_SRVC_%' OR j.order_id LIKE 'BILL_PAID_%' THEN 'PARTNER_WALLET'
            WHEN w.wiom_txn_id IS NOT NULL THEN 'FULLY_TRACED'
            ELSE 'MISSING_WIOM'
        END AS trace_status
    FROM payu_settlements s
    LEFT JOIN _tmp_bank_daily b
        ON b.gw_name = '02 Payu-Wallet'
        AND b.bank_date = CAST(LEFT(CAST(s."AddedOn" AS VARCHAR), 10) AS DATE)
    LEFT JOIN _tmp_juspay_payu j
        ON CAST(s."Merchant Txn ID" AS VARCHAR) = j.juspay_txn_id
    LEFT JOIN _tmp_wiom_lookup_dedup w
        ON j.order_id = w.order_id
    WHERE LEFT(CAST(s."AddedOn" AS VARCHAR), 7) = '2026-01'
""")
n_pu = con.execute("SELECT COUNT(*) FROM recon_jan26_base WHERE gateway='PAYU'").fetchone()[0]
print(f'  PAYU: {n_pu:,} rows inserted.')

# ============================================================
# GATEWAY 4: RAZORPAY
# ============================================================
print('  Building RAZORPAY rows ...')
con.execute("""
    INSERT INTO recon_jan26_base
    SELECT
        'RAZORPAY'                                               AS gateway,
        CAST(r.order_receipt AS VARCHAR)                        AS settlement_order_id,
        b.bank_date,
        b.bank_daily_deposit,
        CAST(r.settled_at AS DATE)                              AS settled_date,
        CAST(r.amount AS DOUBLE)                                AS sett_gross,
        CAST(r.amount - COALESCE(r.fee,0) - COALESCE(r.tax,0) AS DOUBLE) AS sett_net,
        CAST(COALESCE(r.fee,0) + COALESCE(r.tax,0) AS DOUBLE)  AS sett_fee,
        CAST(r.settlement_utr AS VARCHAR)                       AS sett_utr,
        'PAYMENT'                                               AS sett_txn_type,
        j.order_id                                              AS juspay_order_id,
        j.juspay_txn_id,
        j.juspay_amount,
        j.juspay_status,
        j.juspay_gateway,
        j.juspay_created_date,
        CASE WHEN j.order_id IS NOT NULL THEN 'MATCHED' ELSE 'SETT_ONLY' END AS l2_match,
        w.wiom_txn_id,
        w.wiom_table,
        w.wiom_amount,
        CASE
            WHEN j.order_id IS NULL THEN 'NO_JUSPAY'
            WHEN w.wiom_txn_id IS NOT NULL THEN 'MATCHED'
            WHEN j.order_id LIKE 'WIFI_SRVC_%' OR j.order_id LIKE 'BILL_PAID_%' THEN 'PARTNER_WALLET'
            ELSE 'JUSPAY_ONLY'
        END AS l1_match,
        SPLIT_PART(COALESCE(j.order_id, CAST(r.order_receipt AS VARCHAR)), '_', 1) AS order_id_prefix,
        CASE
            WHEN j.order_id IS NULL THEN 'MISSING_JUSPAY'
            WHEN j.order_id LIKE 'WIFI_SRVC_%' OR j.order_id LIKE 'BILL_PAID_%' THEN 'PARTNER_WALLET'
            WHEN w.wiom_txn_id IS NOT NULL THEN 'FULLY_TRACED'
            ELSE 'MISSING_WIOM'
        END AS trace_status
    FROM razorpay_transactions r
    LEFT JOIN _tmp_bank_daily b
        ON b.gw_name = '06 Razorpay Wallet'
        AND b.bank_date = CAST(r.settled_at AS DATE)
    LEFT JOIN _tmp_juspay_razorpay j
        ON CAST(r.order_receipt AS VARCHAR) = j.order_id
    LEFT JOIN _tmp_wiom_lookup_dedup w
        ON j.order_id = w.order_id
    WHERE r.type = 'payment'
      AND LEFT(CAST(r.settled_at AS VARCHAR), 7) = '2026-01'
""")
n_rzp = con.execute("SELECT COUNT(*) FROM recon_jan26_base WHERE gateway='RAZORPAY'").fetchone()[0]
print(f'  RAZORPAY: {n_rzp:,} rows inserted.')

total_rows = con.execute("SELECT COUNT(*) FROM recon_jan26_base").fetchone()[0]
print(f'\n  Done. recon_jan26_base has {total_rows:,} rows total.')
print(f'  Breakdown: PAYTM_V2={n_ptm:,}  PHONEPE={n_pp:,}  PAYU={n_pu:,}  RAZORPAY={n_rzp:,}')

# ============================================================
# PART 1: SUMMARY COUNTS AND AMOUNTS PER GATEWAY PER TRACE_STATUS
# ============================================================
print('\n' + SEP)
print('PART 1: SUMMARY BY GATEWAY x TRACE_STATUS')
print(SEP)

summary = con.execute("""
    SELECT
        gateway,
        trace_status,
        COUNT(*)                          AS rows,
        COALESCE(SUM(sett_gross), 0)      AS sett_gross_total,
        COALESCE(SUM(sett_net), 0)        AS sett_net_total,
        COALESCE(SUM(juspay_amount), 0)   AS juspay_total,
        COALESCE(SUM(wiom_amount), 0)     AS wiom_total
    FROM recon_jan26_base
    GROUP BY gateway, trace_status
    ORDER BY gateway, rows DESC
""").fetchall()

print(f'\n{"Gateway":12s} {"Trace Status":22s} {"Rows":>9s}  {"Sett Gross":>16s}  {"Sett Net":>14s}  {"Juspay Amt":>14s}  {"Wiom Amt":>14s}')
print(SEP2)
prev_gw = None
gw_totals = {}
for r in summary:
    gw, ts, rows, sg, sn, ja, wa = r
    if gw != prev_gw:
        if prev_gw is not None:
            t = gw_totals[prev_gw]
            print(f'  {"SUBTOTAL":10s} {"":22s} {t[0]:>9,}  Rs {t[1]:>12,.0f}  Rs {t[2]:>10,.0f}  Rs {t[3]:>10,.0f}  Rs {t[4]:>10,.0f}')
            print(SEP2)
        prev_gw = gw
    if gw not in gw_totals:
        gw_totals[gw] = [0, 0, 0, 0, 0]
    gw_totals[gw][0] += rows
    gw_totals[gw][1] += sg
    gw_totals[gw][2] += sn
    gw_totals[gw][3] += ja
    gw_totals[gw][4] += wa
    print(f'  {gw:10s} {ts:22s} {rows:>9,}  Rs {sg:>12,.0f}  Rs {sn:>10,.0f}  Rs {ja:>10,.0f}  Rs {wa:>10,.0f}')

if prev_gw:
    t = gw_totals[prev_gw]
    print(f'  {"SUBTOTAL":10s} {"":22s} {t[0]:>9,}  Rs {t[1]:>12,.0f}  Rs {t[2]:>10,.0f}  Rs {t[3]:>10,.0f}  Rs {t[4]:>10,.0f}')
    print(SEP2)

grand = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(sett_gross),0), COALESCE(SUM(sett_net),0),
           COALESCE(SUM(juspay_amount),0), COALESCE(SUM(wiom_amount),0)
    FROM recon_jan26_base
""").fetchone()
print(f'  {"GRAND TOTAL":10s} {"":22s} {grand[0]:>9,}  Rs {grand[1]:>12,.0f}  Rs {grand[2]:>10,.0f}  Rs {grand[3]:>10,.0f}  Rs {grand[4]:>10,.0f}')


# ============================================================
# PART 2: AMOUNT WATERFALL
# ============================================================
print('\n\n' + SEP)
print('PART 2: AMOUNT WATERFALL  (Jan 2026 settlements)')
print(SEP)

bank_jan = con.execute("""
    SELECT "Payment Gateway",
           SUM(CAST("Deposit Amt(INR)" AS DOUBLE)) AS bank_dep
    FROM bank_receipt_from_pg
    WHERE YEAR(CAST("Transaction" AS DATE)) = 2026
      AND MONTH(CAST("Transaction" AS DATE)) = 1
    GROUP BY "Payment Gateway"
    ORDER BY "Payment Gateway"
""").fetchall()

bank_gw_map = {r[0]: r[1] for r in bank_jan}
bank_total_jan = sum(r[1] for r in bank_jan)

wf = con.execute("""
    SELECT
        gateway,
        COUNT(*)                         AS sett_rows,
        COALESCE(SUM(sett_gross),0)      AS sett_gross,
        COALESCE(SUM(sett_net),0)        AS sett_net,
        COALESCE(SUM(sett_fee),0)        AS sett_fee,
        COALESCE(SUM(CASE WHEN l2_match='MATCHED' THEN juspay_amount ELSE 0 END),0) AS juspay_matched_amt,
        COALESCE(SUM(CASE WHEN l1_match='MATCHED' THEN wiom_amount   ELSE 0 END),0) AS wiom_matched_amt
    FROM recon_jan26_base
    GROUP BY gateway
    ORDER BY sett_gross DESC
""").fetchall()

bank_col = {'PAYTM_V2': '01 Paytm-Wallet (WIOM Gold)',
            'PHONEPE':  '05 PhonePe Wallet-2',
            'PAYU':     '02 Payu-Wallet',
            'RAZORPAY': '06 Razorpay Wallet'}

print(f'\n{"Layer":35s} {"PAYTM_V2":>16s}  {"PHONEPE":>14s}  {"PAYU":>14s}  {"RAZORPAY":>14s}  {"TOTAL":>14s}')
print(SEP2)

wf_dict = {r[0]: r for r in wf}
gws = ['PAYTM_V2', 'PHONEPE', 'PAYU', 'RAZORPAY']

def wf_row(label, values):
    vals = [v or 0 for v in values]
    total = sum(vals)
    print(f'  {label:33s}' + ''.join(f'  Rs {v:>10,.0f}' for v in vals) + f'  Rs {total:>10,.0f}')

bank_vals   = [bank_gw_map.get(bank_col[g], 0) for g in gws]
gross_vals  = [(wf_dict.get(g, (None,None,0))[2] or 0) for g in gws]
net_vals    = [(wf_dict.get(g, (None,None,None,0))[3] or 0) for g in gws]
juspay_vals = [(wf_dict.get(g, (None,None,None,None,None,0))[5] or 0) for g in gws]
wiom_vals   = [(wf_dict.get(g, (None,None,None,None,None,None,0))[6] or 0) for g in gws]

wf_row('Bank deposits (Jan 2026)',        bank_vals)
wf_row('Settlement gross',                gross_vals)
wf_row('Settlement net (after fees)',     net_vals)
wf_row('Juspay matched amount',           juspay_vals)
wf_row('Wiom DB matched amount',          wiom_vals)
print(SEP2)

bank_vs_gross   = [bank_vals[i] - gross_vals[i] for i in range(4)]
gross_vs_juspay = [gross_vals[i] - juspay_vals[i] for i in range(4)]
juspay_vs_wiom  = [juspay_vals[i] - wiom_vals[i] for i in range(4)]
print(f'  {"Gap: Bank minus Sett Gross":33s}' + ''.join(f'  Rs {v:>10,.0f}' for v in bank_vs_gross) + f'  Rs {sum(bank_vs_gross):>10,.0f}')
print(f'  {"Gap: Sett Gross minus Juspay":33s}' + ''.join(f'  Rs {v:>10,.0f}' for v in gross_vs_juspay) + f'  Rs {sum(gross_vs_juspay):>10,.0f}')
print(f'  {"Gap: Juspay minus Wiom":33s}' + ''.join(f'  Rs {v:>10,.0f}' for v in juspay_vs_wiom) + f'  Rs {sum(juspay_vs_wiom):>10,.0f}')
print()
print('  Notes:')
print('  - PAYTM_V2 bank Rs 5.76Cr >> sett gross: Jan26 bank includes Dec25 txn settlements')
print('  - Sett Gross vs Juspay negative: txns created Dec25/other months, settled in Jan26')
print('  - Juspay vs Wiom: juspay rows with no wiom match (true gaps + refunded txns)')


# ============================================================
# PART 3: GAP ANALYSIS
# ============================================================
print('\n\n' + SEP)
print('PART 3: GAP ANALYSIS')
print(SEP)

# ---- Gap A: Bank vs Settlement NET ----
print('\n--- Gap A: Bank deposit vs Settlement NET by gateway (Jan 2026) ---')
print('  (Date-level: refund netting means bank < sett net on some days)')
print()

gap_a = con.execute("""
    WITH daily_sett AS (
        SELECT gateway, settled_date,
               SUM(sett_gross) AS sett_gross,
               SUM(sett_net)   AS sett_net,
               COUNT(*)        AS txns
        FROM recon_jan26_base
        GROUP BY 1, 2
    )
    SELECT ds.gateway,
           COUNT(DISTINCT ds.settled_date)             AS sett_days,
           SUM(ds.txns)                                AS sett_rows,
           SUM(ds.sett_gross)                          AS sett_gross,
           SUM(ds.sett_net)                            AS sett_net,
           COALESCE(SUM(b.bank_daily_deposit), 0)      AS bank_dep,
           COALESCE(SUM(b.bank_daily_deposit),0) - SUM(ds.sett_net) AS gap_net_vs_bank
    FROM daily_sett ds
    LEFT JOIN _tmp_bank_daily b
        ON b.bank_date = ds.settled_date
        AND b.gw_name = CASE ds.gateway
            WHEN 'PAYTM_V2' THEN '01 Paytm-Wallet (WIOM Gold)'
            WHEN 'PHONEPE'  THEN '05 PhonePe Wallet-2'
            WHEN 'PAYU'     THEN '02 Payu-Wallet'
            WHEN 'RAZORPAY' THEN '06 Razorpay Wallet'
        END
    GROUP BY ds.gateway
    ORDER BY ds.gateway
""").fetchall()

print(f'  {"Gateway":12s} {"Sett Days":>10s} {"Sett Rows":>10s} {"Sett Gross":>16s} {"Sett Net":>14s} {"Bank Dep":>14s} {"Gap(Bank-Net)":>15s} {"Gap%":>8s}')
print('  ' + '-'*105)
for r in gap_a:
    gw, days, rows, sg, sn, bd, gap = r
    pct = gap / sn * 100 if sn else 0
    print(f'  {gw:12s} {days:>10,} {rows:>10,} Rs {sg:>12,.0f} Rs {sn:>10,.0f} Rs {bd:>10,.0f} Rs {gap:>11,.0f} {pct:>7.2f}%')
tot_sg  = sum(r[3] for r in gap_a)
tot_sn  = sum(r[4] for r in gap_a)
tot_bd  = sum(r[5] for r in gap_a)
tot_gap = tot_bd - tot_sn
print('  ' + '-'*105)
print(f'  {"TOTAL":12s} {"":>10s} {sum(r[2] for r in gap_a):>10,} Rs {tot_sg:>12,.0f} Rs {tot_sn:>10,.0f} Rs {tot_bd:>10,.0f} Rs {tot_gap:>11,.0f} {tot_gap/tot_sn*100 if tot_sn else 0:>7.2f}%')


# ---- Gap B: Settlement rows not in Juspay ----
print('\n\n--- Gap B: Settlement rows NOT matched in Juspay (by order_id prefix) ---')

gap_b_total = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(sett_gross),0), COALESCE(SUM(sett_net),0)
    FROM recon_jan26_base WHERE l2_match = 'SETT_ONLY'
""").fetchone()
print(f'  Total settlement rows with no Juspay match: {gap_b_total[0]:,} rows,  Rs {gap_b_total[1]:,.0f} gross,  Rs {gap_b_total[2]:,.0f} net')
print()

gap_b = con.execute("""
    SELECT gateway, order_id_prefix,
           COUNT(*)                      AS rows,
           COALESCE(SUM(sett_gross),0)   AS gross,
           COALESCE(SUM(sett_net),0)     AS net
    FROM recon_jan26_base
    WHERE l2_match = 'SETT_ONLY'
    GROUP BY gateway, order_id_prefix
    ORDER BY gross DESC
    LIMIT 50
""").fetchall()

print(f'  {"Gateway":12s} {"Prefix":25s} {"Rows":>8s} {"Gross":>16s} {"Net":>14s}')
print('  ' + '-'*80)
for r in gap_b:
    print(f'  {r[0]:12s} {str(r[1]):25s} {r[2]:>8,} Rs {r[3]:>12,.0f} Rs {r[4]:>10,.0f}')


# ---- Gap C: Juspay records not in Wiom ----
print('\n\n--- Gap C: Juspay records with no Wiom match (by order_id prefix) ---')

gap_c_total = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(juspay_amount),0), COALESCE(SUM(sett_gross),0)
    FROM recon_jan26_base WHERE l1_match = 'JUSPAY_ONLY'
""").fetchone()
print(f'  Total Juspay records with no Wiom match: {gap_c_total[0]:,} rows,  Rs {gap_c_total[1]:,.0f} juspay amt,  Rs {gap_c_total[2]:,.0f} sett gross')
print()

gap_c = con.execute("""
    SELECT gateway, order_id_prefix,
           COUNT(*)                         AS rows,
           COALESCE(SUM(juspay_amount),0)   AS juspay_amt,
           COALESCE(SUM(sett_gross),0)      AS sett_gross
    FROM recon_jan26_base
    WHERE l1_match = 'JUSPAY_ONLY'
    GROUP BY gateway, order_id_prefix
    ORDER BY juspay_amt DESC
    LIMIT 50
""").fetchall()

print(f'  {"Gateway":12s} {"Prefix":25s} {"Rows":>8s} {"Juspay Amt":>14s} {"Sett Gross":>14s}')
print('  ' + '-'*78)
for r in gap_c:
    print(f'  {r[0]:12s} {str(r[1]):25s} {r[2]:>8,} Rs {r[3]:>10,.0f} Rs {r[4]:>10,.0f}')


# ---- Gap D: Amount mismatches (where matched) ----
print('\n\n--- Gap D: Amount mismatches (Juspay amount vs Wiom amount, where L1=MATCHED) ---')

gap_d = con.execute("""
    SELECT
        gateway,
        COUNT(*)                                                             AS matched_rows,
        SUM(CASE WHEN ABS(COALESCE(juspay_amount,0) - COALESCE(wiom_amount,0)) < 0.50 THEN 1 ELSE 0 END) AS exact_match,
        SUM(CASE WHEN ABS(COALESCE(juspay_amount,0) - COALESCE(wiom_amount,0)) >= 0.50 THEN 1 ELSE 0 END) AS diff_rows,
        COALESCE(SUM(juspay_amount),0)                                      AS juspay_total,
        COALESCE(SUM(wiom_amount),0)                                        AS wiom_total,
        COALESCE(SUM(juspay_amount - wiom_amount),0)                        AS diff_total
    FROM recon_jan26_base
    WHERE l1_match = 'MATCHED'
      AND juspay_amount IS NOT NULL
      AND wiom_amount IS NOT NULL
    GROUP BY gateway
    ORDER BY gateway
""").fetchall()

print(f'  {"Gateway":12s} {"Matched":>10s} {"Exact":>10s} {"Diff Rows":>10s} {"Juspay Amt":>16s} {"Wiom Amt":>16s} {"Diff":>14s}')
print('  ' + '-'*95)
for r in gap_d:
    gw, total, exact, diff_rows, ja, wa, dt = r
    print(f'  {gw:12s} {total:>10,} {exact:>10,} {diff_rows:>10,} Rs {ja:>12,.0f} Rs {wa:>12,.0f} Rs {dt:>10,.0f}')

if gap_d:
    print('  ' + '-'*95)
    tot_m  = sum(r[1] for r in gap_d)
    tot_e  = sum(r[2] for r in gap_d)
    tot_dr = sum(r[3] for r in gap_d)
    tot_ja = sum(r[4] for r in gap_d)
    tot_wa = sum(r[5] for r in gap_d)
    tot_dt = sum(r[6] for r in gap_d)
    print(f'  {"TOTAL":12s} {tot_m:>10,} {tot_e:>10,} {tot_dr:>10,} Rs {tot_ja:>12,.0f} Rs {tot_wa:>12,.0f} Rs {tot_dt:>10,.0f}')


# ============================================================
# PART 4: SOURCE MONTH WATERFALL
# ============================================================
print('\n\n' + SEP)
print('PART 4: SOURCE MONTH WATERFALL  (which month were txns created in?)')
print(SEP)
print('  Scope: All Jan26 settlement rows (juspay_created_date = NULL for SETT_ONLY rows)')
print()

src_month = con.execute("""
    SELECT
        gateway,
        CASE
            WHEN juspay_created_date IS NULL                              THEN 'NOT_IN_JUSPAY'
            WHEN juspay_created_date < '2025-12-01'                       THEN 'PRE_DEC25'
            WHEN juspay_created_date >= '2025-12-01'
             AND juspay_created_date < '2026-01-01'                       THEN 'DEC_2025'
            WHEN juspay_created_date >= '2026-01-01'
             AND juspay_created_date < '2026-02-01'                       THEN 'JAN_2026'
            ELSE                                                           'FEB26_OR_LATER'
        END                                                   AS txn_month,
        COUNT(*)                                              AS rows,
        COALESCE(SUM(sett_gross), 0)                          AS sett_gross,
        COALESCE(SUM(juspay_amount), 0)                       AS juspay_amt
    FROM recon_jan26_base
    GROUP BY gateway, txn_month
    ORDER BY gateway, rows DESC
""").fetchall()

print(f'  {"Gateway":12s} {"Txn Created Month":22s} {"Rows":>9s} {"Sett Gross":>16s} {"Juspay Amt":>14s}')
print(SEP2)
prev_gw2 = None
gw_sub = {}
for r in src_month:
    gw, mo, rows, sg, ja = r
    if gw != prev_gw2:
        if prev_gw2 is not None:
            t = gw_sub[prev_gw2]
            print(f'  {"SUBTOTAL":12s} {"":22s} {t[0]:>9,}  Rs {t[1]:>12,.0f}  Rs {t[2]:>10,.0f}')
            print(SEP2)
        prev_gw2 = gw
    if gw not in gw_sub:
        gw_sub[gw] = [0, 0, 0]
    gw_sub[gw][0] += rows
    gw_sub[gw][1] += sg
    gw_sub[gw][2] += ja
    print(f'  {gw:12s} {mo:22s} {rows:>9,}  Rs {sg:>12,.0f}  Rs {ja:>10,.0f}')

if prev_gw2:
    t = gw_sub[prev_gw2]
    print(f'  {"SUBTOTAL":12s} {"":22s} {t[0]:>9,}  Rs {t[1]:>12,.0f}  Rs {t[2]:>10,.0f}')
    print(SEP2)


# ============================================================
# PART 5: EXPORT GAP CSVs
# ============================================================
print('\n\n' + SEP)
print('PART 5: EXPORTING GAP CSVs')
print(SEP)

csv_sett_no_juspay = os.path.join(DOCS_DIR, 'gap_settlement_no_juspay_jan26.csv')
csv_juspay_no_wiom = os.path.join(DOCS_DIR, 'gap_juspay_no_wiom_jan26.csv')

# Export 1: Settlement rows not in Juspay
con.execute(f"""
    COPY (
        SELECT
            gateway,
            settlement_order_id,
            settled_date,
            sett_gross,
            sett_net,
            sett_fee,
            sett_utr,
            sett_txn_type,
            order_id_prefix,
            trace_status
        FROM recon_jan26_base
        WHERE l2_match = 'SETT_ONLY'
        ORDER BY gateway, sett_gross DESC
    ) TO '{csv_sett_no_juspay}' (HEADER, DELIMITER ',')
""")
c1 = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(sett_gross),0), COALESCE(SUM(sett_net),0)
    FROM recon_jan26_base WHERE l2_match='SETT_ONLY'
""").fetchone()
print(f'\n  Exported: {csv_sett_no_juspay}')
print(f'  Rows: {c1[0]:,}  |  Sett Gross: Rs {c1[1]:,.0f}  |  Sett Net: Rs {c1[2]:,.0f}')

# Export 2: Juspay records not in Wiom
con.execute(f"""
    COPY (
        SELECT
            gateway,
            settlement_order_id,
            settled_date,
            sett_gross,
            sett_net,
            juspay_order_id,
            juspay_txn_id,
            juspay_amount,
            juspay_status,
            juspay_created_date,
            l2_match,
            l1_match,
            order_id_prefix,
            trace_status
        FROM recon_jan26_base
        WHERE l1_match = 'JUSPAY_ONLY'
        ORDER BY gateway, juspay_amount DESC
    ) TO '{csv_juspay_no_wiom}' (HEADER, DELIMITER ',')
""")
c2 = con.execute("""
    SELECT COUNT(*), COALESCE(SUM(juspay_amount),0), COALESCE(SUM(sett_gross),0)
    FROM recon_jan26_base WHERE l1_match='JUSPAY_ONLY'
""").fetchone()
print(f'\n  Exported: {csv_juspay_no_wiom}')
print(f'  Rows: {c2[0]:,}  |  Juspay Amt: Rs {c2[1]:,.0f}  |  Sett Gross: Rs {c2[2]:,.0f}')


# ============================================================
# PART 6: OVERALL SCORECARD
# ============================================================
print('\n\n' + SEP)
print('PART 6: OVERALL SCORECARD')
print(SEP)

sc = con.execute("""
    SELECT
        COUNT(*)                                                                AS total_sett_rows,
        COALESCE(SUM(sett_gross), 0)                                           AS total_sett_gross,
        COALESCE(SUM(sett_net), 0)                                             AS total_sett_net,
        SUM(CASE WHEN l2_match='MATCHED' THEN 1 ELSE 0 END)                    AS l2_matched_rows,
        SUM(CASE WHEN l2_match='SETT_ONLY' THEN 1 ELSE 0 END)                 AS l2_sett_only_rows,
        COALESCE(SUM(CASE WHEN l2_match='MATCHED' THEN sett_gross END), 0)     AS l2_matched_gross,
        COALESCE(SUM(CASE WHEN l2_match='SETT_ONLY' THEN sett_gross END), 0)  AS l2_sett_only_gross,
        SUM(CASE WHEN l1_match='MATCHED' THEN 1 ELSE 0 END)                    AS l1_matched,
        SUM(CASE WHEN l1_match='JUSPAY_ONLY' THEN 1 ELSE 0 END)               AS l1_juspay_only,
        SUM(CASE WHEN l1_match='PARTNER_WALLET' THEN 1 ELSE 0 END)            AS l1_partner_wallet,
        SUM(CASE WHEN l1_match='NO_JUSPAY' THEN 1 ELSE 0 END)                 AS l1_no_juspay,
        SUM(CASE WHEN trace_status='FULLY_TRACED' THEN 1 ELSE 0 END)           AS ts_fully_traced,
        SUM(CASE WHEN trace_status='MISSING_JUSPAY' THEN 1 ELSE 0 END)        AS ts_missing_juspay,
        SUM(CASE WHEN trace_status='MISSING_WIOM' THEN 1 ELSE 0 END)          AS ts_missing_wiom,
        SUM(CASE WHEN trace_status='PARTNER_WALLET' THEN 1 ELSE 0 END)        AS ts_partner_wallet,
        COALESCE(SUM(CASE WHEN trace_status='FULLY_TRACED' THEN sett_gross END), 0)    AS ts_ft_gross,
        COALESCE(SUM(CASE WHEN trace_status='MISSING_JUSPAY' THEN sett_gross END), 0)  AS ts_mj_gross,
        COALESCE(SUM(CASE WHEN trace_status='MISSING_WIOM' THEN sett_gross END), 0)    AS ts_mw_gross,
        COALESCE(SUM(CASE WHEN trace_status='PARTNER_WALLET' THEN sett_gross END), 0)  AS ts_pw_gross
    FROM recon_jan26_base
""").fetchone()

(total_rows_sc, tot_gross, tot_net,
 l2_match, l2_sett_only, l2_match_gross, l2_sett_only_gross,
 l1_match_r, l1_jo, l1_pw, l1_nj,
 ts_ft, ts_mj, ts_mw, ts_pw,
 ts_ft_g, ts_mj_g, ts_mw_g, ts_pw_g) = sc

bank_jan_total = con.execute("""
    SELECT COALESCE(SUM(CAST("Deposit Amt(INR)" AS DOUBLE)), 0)
    FROM bank_receipt_from_pg
    WHERE YEAR(CAST("Transaction" AS DATE)) = 2026
      AND MONTH(CAST("Transaction" AS DATE)) = 1
""").fetchone()[0]

print()
print(f'  LAYER 4 (Bank) -----------------------------------------------')
print(f'  {"Bank deposits Jan 2026 (all gateways)":48s}  Rs {bank_jan_total:>14,.0f}')
print()
print(f'  LAYER 3b (Settlement) ----------------------------------------')
print(f'  {"Total settlement rows (Jan 2026)":48s}  {total_rows_sc:>14,} rows')
print(f'  {"Total settlement gross":48s}  Rs {tot_gross:>14,.0f}')
print(f'  {"Total settlement net":48s}  Rs {tot_net:>14,.0f}')
ptm_bank = bank_gw_map.get('01 Paytm-Wallet (WIOM Gold)', 0)
ptm_sett_net = con.execute("SELECT COALESCE(SUM(sett_net),0) FROM recon_jan26_base WHERE gateway='PAYTM_V2'").fetchone()[0]
print(f'  {"  Paytm bank gap (note: includes Dec25 txn settlements)":48s}  Rs {ptm_bank - ptm_sett_net:>14,.0f}')
print()
print(f'  LAYER 2 (Settlement -> Juspay) -------------------------------')
print(f'  {"Matched in Juspay":48s}  {l2_match:>14,} rows  Rs {l2_match_gross:>12,.0f}  ({l2_match/total_rows_sc*100:.2f}%)')
print(f'  {"Not in Juspay (SETT_ONLY)":48s}  {l2_sett_only:>14,} rows  Rs {l2_sett_only_gross:>12,.0f}  ({l2_sett_only/total_rows_sc*100:.2f}%)')
print()
print(f'  LAYER 1 (Juspay -> Wiom DB) ----------------------------------')
print(f'  {"Matched in Wiom DB":48s}  {l1_match_r:>14,} rows  ({l1_match_r/(l2_match or 1)*100:.2f}% of juspay-matched)')
print(f'  {"Not in Wiom (JUSPAY_ONLY -- true gap)":48s}  {l1_jo:>14,} rows')
print(f'  {"Partner wallet (WIFI_SRVC/BILL_PAID)":48s}  {l1_pw:>14,} rows')
print(f'  {"No juspay (same as SETT_ONLY)":48s}  {l1_nj:>14,} rows')
print()
print(f'  TRACE STATUS SUMMARY -----------------------------------------')
print(f'  {"FULLY_TRACED":48s}  {ts_ft:>14,} rows  Rs {ts_ft_g:>12,.0f}  ({ts_ft/total_rows_sc*100:.2f}%)')
print(f'  {"MISSING_JUSPAY":48s}  {ts_mj:>14,} rows  Rs {ts_mj_g:>12,.0f}  ({ts_mj/total_rows_sc*100:.2f}%)')
print(f'  {"MISSING_WIOM":48s}  {ts_mw:>14,} rows  Rs {ts_mw_g:>12,.0f}  ({ts_mw/total_rows_sc*100:.2f}%)')
print(f'  {"PARTNER_WALLET":48s}  {ts_pw:>14,} rows  Rs {ts_pw_g:>12,.0f}  ({ts_pw/total_rows_sc*100:.2f}%)')
print()
print(f'  CSV Exports')
print(f'    {csv_sett_no_juspay}  ({c1[0]:,} rows, Rs {c1[1]:,.0f})')
print(f'    {csv_juspay_no_wiom}  ({c2[0]:,} rows, Rs {c2[1]:,.0f})')

con.close()

print('\n' + SEP)
print('END OF REVERSE RECONCILIATION REPORT')
print(SEP)
