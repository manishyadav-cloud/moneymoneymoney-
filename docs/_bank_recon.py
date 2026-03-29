# -*- coding: utf-8 -*-
"""
Settlement <-> Bank Receipt Reconciliation  (ALL 4 GATEWAYS)
=============================================================
Links PG settlement records to actual bank deposits in bank_receipt_from_pg.

Bank file has 4 sheets (now all loaded):
  01 Paytm-Wallet (WIOM Gold)  -> 357 rows
  02 Payu-Wallet               -> 263 rows
  05 PhonePe Wallet-2          -> 240 rows
  06 Razorpay Wallet           -> 238 rows

Settlement sources:
  Paytm    : paytm_settlements   (settled_date)
  PhonePe  : phonepe_settlements ("Settlement Date")
  PayU     : payu_settlements    ("AddedOn")
  Razorpay : razorpay_transactions (type='payment', settled_at)

Refund sources:
  Paytm    : paytm_refunds  (Settled_Date)
  PhonePe  : phonepe_refunds (has no settlement_date -- refunds show in phonepe_settlements as REVERSAL rows)
  PayU     : payu_settlements where status='Refunded' / 'Chargebacked'
  Razorpay : razorpay_transactions where type='refund'

Parts:
  A. Summary: bank deposits vs total settlement net per gateway
  B. Monthly reconciliation per gateway (settlement net vs bank deposit)
  C. January 2026 deep-dive all 4 gateways
  D. Refund impact analysis per gateway
  E. Overall scorecard
"""
import duckdb

con = duckdb.connect('data.duckdb', read_only=True)

SEP  = '=' * 140
SEP2 = '-' * 140
SEP3 = '.' * 140

print(SEP)
print('SETTLEMENT  <-->  BANK RECEIPT RECONCILIATION  (ALL 4 GATEWAYS)')
print(SEP)

# ======================================================================
# PART A: OVERALL SUMMARY  (bank totals vs settlement totals)
# ======================================================================
print('\n' + SEP)
print('PART A: OVERALL SUMMARY  (bank total vs settlement net per gateway)')
print(SEP)

# Bank deposits by gateway
bank_summary = con.execute("""
    SELECT "Payment Gateway",
           COUNT(*) AS deposits,
           MIN(CAST("Transaction" AS DATE)) AS first_dt,
           MAX(CAST("Transaction" AS DATE)) AS last_dt,
           SUM(CAST("Deposit Amt(INR)" AS DOUBLE)) AS bank_total
    FROM bank_receipt_from_pg
    GROUP BY "Payment Gateway"
    ORDER BY 1
""").fetchall()

print('\n--- Bank deposits per gateway ---')
print(f'  {"Payment Gateway":32s} {"Deposits":>8s} {"From":>12s} {"To":>12s} {"Total Bank (Rs)":>18s}')
print('  ' + '-'*85)
for r in bank_summary:
    print(f'  {str(r[0]):32s} {r[1]:>8,} {str(r[2]):>12s} {str(r[3]):>12s} Rs {r[4]:>14,.2f}')
bank_grand = sum(r[4] for r in bank_summary)
print('  ' + '-'*85)
print(f'  {"TOTAL":32s} {sum(r[1] for r in bank_summary):>8,} {"":>12s} {"":>12s} Rs {bank_grand:>14,.2f}')

# Settlement net by gateway
print('\n--- Settlement net per gateway (from PG settlement tables) ---')
paytm_sett = con.execute("""
    SELECT 'Paytm' AS pg,
           COUNT(*) AS rows,
           MIN(settled_date) AS first_dt, MAX(settled_date) AS last_dt,
           SUM(amount) AS gross, SUM(settled_amount) AS net,
           SUM(commission)+SUM(gst) AS fees
    FROM paytm_settlements
""").fetchone()

pp_sett = con.execute("""
    SELECT 'PhonePe' AS pg,
           COUNT(*) AS rows,
           MIN(CAST("Settlement Date" AS DATE)) AS first_dt,
           MAX(CAST("Settlement Date" AS DATE)) AS last_dt,
           SUM("Transaction Amount") AS gross,
           SUM("Net Amount") AS net,
           ABS(SUM("Total Fees")) AS fees
    FROM phonepe_settlements
    WHERE "Transaction Type" NOT LIKE '%REVERSAL%'
      AND "Transaction Type" NOT LIKE '%REFUND%'
      AND "Transaction Type" IS NOT NULL
""").fetchone()

pu_sett = con.execute("""
    SELECT 'PayU' AS pg,
           COUNT(*) AS rows,
           MIN(CAST("AddedOn" AS TIMESTAMP)) AS first_dt,
           MAX(CAST("AddedOn" AS TIMESTAMP)) AS last_dt,
           SUM("Amount") AS gross,
           SUM("Net Amount") AS net,
           SUM("Amount" - "Net Amount") AS fees
    FROM payu_settlements
    WHERE "Status" NOT IN ('Refunded','Chargebacked')
      OR "Status" IS NULL
""").fetchone()

rzp_sett = con.execute("""
    SELECT 'Razorpay' AS pg,
           COUNT(*) AS rows,
           MIN(settled_at) AS first_dt, MAX(settled_at) AS last_dt,
           SUM(amount) AS gross,
           SUM(amount)-SUM(fee)-SUM(tax) AS net,
           SUM(fee)+SUM(tax) AS fees
    FROM razorpay_transactions
    WHERE type = 'payment'
""").fetchone()

print(f'  {"Gateway":12s} {"Rows":>10s} {"From":>12s} {"To":>12s} {"Gross (Rs)":>16s} {"Net (Rs)":>16s} {"Fees (Rs)":>12s}')
print('  ' + '-'*95)
for r in [paytm_sett, pp_sett, pu_sett, rzp_sett]:
    print(f'  {r[0]:12s} {r[1]:>10,} {str(r[2])[:10]:>12s} {str(r[3])[:10]:>12s} Rs {r[4]:>12,.0f} Rs {r[5]:>12,.0f} Rs {r[6]:>8,.2f}')
sett_grand_net = paytm_sett[5] + pp_sett[5] + pu_sett[5] + rzp_sett[5]
print('  ' + '-'*95)
print(f'  {"TOTAL":12s} {"":>10s} {"":>12s} {"":>12s} {"":>16s} Rs {sett_grand_net:>12,.0f}')

print(f'\n  Total settlement net (all PGs) : Rs {sett_grand_net:>14,.0f}')
print(f'  Total bank deposits  (all PGs) : Rs {bank_grand:>14,.0f}')
print(f'  Overall gap                    : Rs {bank_grand - sett_grand_net:>14,.0f}  ({(bank_grand-sett_grand_net)/sett_grand_net*100:.2f}%)')
print('\n  Note: Settlement tables cover Dec25-Feb26; Bank file covers Apr25-Mar26.')
print('  Months Apr25-Nov25 have bank deposits but NO settlement data => explains positive gap.')

# ======================================================================
# PART B: MONTHLY RECONCILIATION PER GATEWAY
# ======================================================================
print('\n\n' + SEP)
print('PART B: MONTHLY RECONCILIATION PER GATEWAY')
print(SEP)

def monthly_pg_recon(pg_name, pg_col_value, sett_sql, bank_filter):
    """Returns (month, sett_net, bank_dep, gap) per month."""
    sett_monthly = con.execute(sett_sql).fetchall()
    bank_monthly = con.execute(f"""
        SELECT DATE_TRUNC('month', CAST("Transaction" AS DATE)) AS mo,
               COUNT(*) AS deps,
               SUM(CAST("Deposit Amt(INR)" AS DOUBLE)) AS bank_total
        FROM bank_receipt_from_pg
        WHERE "Payment Gateway" = '{pg_col_value}'
        GROUP BY 1 ORDER BY 1
    """).fetchall()

    # Merge
    sett_dict = {r[0]: r for r in sett_monthly}
    bank_dict  = {r[0]: r for r in bank_monthly}
    all_months = sorted(set(list(sett_dict.keys()) + list(bank_dict.keys())))

    rows = []
    for mo in all_months:
        s = sett_dict.get(mo)
        b = bank_dict.get(mo)
        snet = s[2] if s else 0
        bdep = b[2] if b else 0
        rows.append((mo, (s[1] if s else 0), snet, (b[1] if b else 0), bdep, bdep - snet))
    return rows

# ---- PAYTM ----
print('\n### PAYTM ###')
paytm_monthly = monthly_pg_recon(
    'Paytm', '01 Paytm-Wallet (WIOM Gold)',
    """
    SELECT DATE_TRUNC('month', settled_date) AS mo,
           COUNT(*) AS txns, SUM(settled_amount) AS net
    FROM paytm_settlements GROUP BY 1 ORDER BY 1
    """,
    "Payment Gateway = '01 Paytm-Wallet (WIOM Gold)'"
)
print(f'  {"Month":12s} {"Sett Txns":>10s} {"Sett Net (Rs)":>16s} {"Bank Deps":>10s} {"Bank Total (Rs)":>16s} {"Gap (Rs)":>12s} {"Gap%":>7s}')
print('  ' + '-'*88)
for r in paytm_monthly:
    gap_pct = r[5]/r[4]*100 if r[4] else 0
    flag = ' * ' if r[1] > 0 and r[3] > 0 else '   '
    print(f'{flag} {str(r[0])[:7]:12s} {r[1]:>10,} Rs {r[2]:>12,.0f} {r[3]:>10,} Rs {r[4]:>12,.0f} Rs {r[5]:>8,.0f} {gap_pct:>6.2f}%')
t_snet = sum(r[2] for r in paytm_monthly)
t_bdep = sum(r[4] for r in paytm_monthly)
print('  ' + '-'*88)
print(f'  {"TOTAL":12s} {sum(r[1] for r in paytm_monthly):>10,} Rs {t_snet:>12,.0f} {sum(r[3] for r in paytm_monthly):>10,} Rs {t_bdep:>12,.0f} Rs {t_bdep-t_snet:>8,.0f} {(t_bdep-t_snet)/t_bdep*100:>6.2f}%')
print('  * = months where BOTH settlement and bank data exist (reconcilable months)')

# ---- PHONEPE ----
print('\n### PHONEPE ###')
pp_monthly = monthly_pg_recon(
    'PhonePe', '05 PhonePe Wallet-2',
    """
    SELECT DATE_TRUNC('month', CAST("Settlement Date" AS DATE)) AS mo,
           COUNT(*) AS txns, SUM("Net Amount") AS net
    FROM phonepe_settlements
    WHERE "Transaction Type" NOT LIKE '%REVERSAL%'
      AND "Transaction Type" NOT LIKE '%REFUND%'
      AND "Transaction Type" IS NOT NULL
    GROUP BY 1 ORDER BY 1
    """,
    "Payment Gateway = '05 PhonePe Wallet-2'"
)
print(f'  {"Month":12s} {"Sett Rows":>10s} {"Sett Net (Rs)":>16s} {"Bank Deps":>10s} {"Bank Total (Rs)":>16s} {"Gap (Rs)":>12s} {"Gap%":>7s}')
print('  ' + '-'*88)
for r in pp_monthly:
    gap_pct = r[5]/r[4]*100 if r[4] else 0
    flag = ' * ' if r[1] > 0 and r[3] > 0 else '   '
    print(f'{flag} {str(r[0])[:7]:12s} {r[1]:>10,} Rs {r[2]:>12,.0f} {r[3]:>10,} Rs {r[4]:>12,.0f} Rs {r[5]:>8,.0f} {gap_pct:>6.2f}%')
t_snet = sum(r[2] for r in pp_monthly)
t_bdep = sum(r[4] for r in pp_monthly)
print('  ' + '-'*88)
print(f'  {"TOTAL":12s} {sum(r[1] for r in pp_monthly):>10,} Rs {t_snet:>12,.0f} {sum(r[3] for r in pp_monthly):>10,} Rs {t_bdep:>12,.0f} Rs {t_bdep-t_snet:>8,.0f} {(t_bdep-t_snet)/t_bdep*100 if t_bdep else 0:>6.2f}%')

# ---- PAYU ----
print('\n### PAYU ###')
pu_monthly = monthly_pg_recon(
    'PayU', '02 Payu-Wallet',
    """
    SELECT DATE_TRUNC('month', CAST("AddedOn" AS TIMESTAMP)) AS mo,
           COUNT(*) AS txns, SUM("Net Amount") AS net
    FROM payu_settlements
    GROUP BY 1 ORDER BY 1
    """,
    "Payment Gateway = '02 Payu-Wallet'"
)
print(f'  {"Month":12s} {"Sett Rows":>10s} {"Sett Net (Rs)":>16s} {"Bank Deps":>10s} {"Bank Total (Rs)":>16s} {"Gap (Rs)":>12s} {"Gap%":>7s}')
print('  ' + '-'*88)
for r in pu_monthly:
    gap_pct = r[5]/r[4]*100 if r[4] else 0
    flag = ' * ' if r[1] > 0 and r[3] > 0 else '   '
    print(f'{flag} {str(r[0])[:7]:12s} {r[1]:>10,} Rs {r[2]:>12,.0f} {r[3]:>10,} Rs {r[4]:>12,.0f} Rs {r[5]:>8,.0f} {gap_pct:>6.2f}%')
t_snet = sum(r[2] for r in pu_monthly)
t_bdep = sum(r[4] for r in pu_monthly)
print('  ' + '-'*88)
print(f'  {"TOTAL":12s} {sum(r[1] for r in pu_monthly):>10,} Rs {t_snet:>12,.0f} {sum(r[3] for r in pu_monthly):>10,} Rs {t_bdep:>12,.0f} Rs {t_bdep-t_snet:>8,.0f} {(t_bdep-t_snet)/t_bdep*100 if t_bdep else 0:>6.2f}%')

# ---- RAZORPAY ----
print('\n### RAZORPAY ###')
rzp_monthly = monthly_pg_recon(
    'Razorpay', '06 Razorpay Wallet',
    """
    SELECT DATE_TRUNC('month', settled_at) AS mo,
           COUNT(*) AS txns,
           SUM(amount)-SUM(fee)-SUM(tax) AS net
    FROM razorpay_transactions
    WHERE type = 'payment'
    GROUP BY 1 ORDER BY 1
    """,
    "Payment Gateway = '06 Razorpay Wallet'"
)
print(f'  {"Month":12s} {"Sett Rows":>10s} {"Sett Net (Rs)":>16s} {"Bank Deps":>10s} {"Bank Total (Rs)":>16s} {"Gap (Rs)":>12s} {"Gap%":>7s}')
print('  ' + '-'*88)
for r in rzp_monthly:
    gap_pct = r[5]/r[4]*100 if r[4] else 0
    flag = ' * ' if r[1] > 0 and r[3] > 0 else '   '
    print(f'{flag} {str(r[0])[:7]:12s} {r[1]:>10,} Rs {r[2]:>12,.0f} {r[3]:>10,} Rs {r[4]:>12,.0f} Rs {r[5]:>8,.0f} {gap_pct:>6.2f}%')
t_snet = sum(r[2] for r in rzp_monthly)
t_bdep = sum(r[4] for r in rzp_monthly)
print('  ' + '-'*88)
print(f'  {"TOTAL":12s} {sum(r[1] for r in rzp_monthly):>10,} Rs {t_snet:>12,.0f} {sum(r[3] for r in rzp_monthly):>10,} Rs {t_bdep:>12,.0f} Rs {t_bdep-t_snet:>8,.0f} {(t_bdep-t_snet)/t_bdep*100 if t_bdep else 0:>6.2f}%')

# ======================================================================
# PART C: JANUARY 2026 DEEP-DIVE  (all 4 gateways)
# ======================================================================
print('\n\n' + SEP)
print('PART C: JANUARY 2026 DEEP-DIVE  (settled in Jan 2026)')
print(SEP)

print('\n--- Paytm Jan 2026 daily (settlement net - refunds = bank) ---')
daily_paytm = con.execute("""
    WITH sett AS (
        SELECT settled_date AS dt,
               COUNT(*) AS txns,
               SUM(amount) AS gross,
               SUM(settled_amount) AS net,
               SUM(commission)+SUM(gst) AS fees
        FROM paytm_settlements
        WHERE YEAR(settled_date)=2026 AND MONTH(settled_date)=1
        GROUP BY settled_date
    ),
    ref AS (
        SELECT CAST(LEFT(TRIM(Settled_Date, chr(39)), 10) AS DATE) AS dt,
               COUNT(*) AS ref_txns,
               SUM(CAST(Settled_Amount AS DOUBLE)) AS ref_net
        FROM paytm_refunds
        WHERE YEAR(CAST(LEFT(TRIM(Settled_Date, chr(39)), 10) AS DATE))=2026
          AND MONTH(CAST(LEFT(TRIM(Settled_Date, chr(39)), 10) AS DATE))=1
        GROUP BY 1
    ),
    bank AS (
        SELECT CAST("Transaction" AS DATE) AS dt,
               SUM(CAST("Deposit Amt(INR)" AS DOUBLE)) AS bank_dep
        FROM bank_receipt_from_pg
        WHERE "Payment Gateway" = '01 Paytm-Wallet (WIOM Gold)'
          AND YEAR(CAST("Transaction" AS DATE))=2026
          AND MONTH(CAST("Transaction" AS DATE))=1
        GROUP BY 1
    )
    SELECT s.dt,
           s.txns, s.gross, s.net, s.fees,
           COALESCE(r.ref_txns,0) AS ref_txns,
           COALESCE(r.ref_net,0) AS ref_net,
           s.net - COALESCE(r.ref_net,0) AS expected,
           COALESCE(b.bank_dep,0) AS bank_dep,
           COALESCE(b.bank_dep,0) - (s.net - COALESCE(r.ref_net,0)) AS daily_diff
    FROM sett s
    LEFT JOIN ref r ON s.dt = r.dt
    LEFT JOIN bank b ON s.dt = b.dt
    ORDER BY s.dt
""").fetchall()

print(f'  {"Date":12s} {"Sett Txns":>10s} {"Sett Net":>14s} {"Refunds":>12s} {"Expected":>14s} {"Bank Dep":>14s} {"Diff":>12s}')
print('  ' + '-'*95)
for r in daily_paytm:
    flag = '*** ' if abs(r[9]) > 50000 else '    '
    print(f'  {flag}{str(r[0]):12s} {r[1]:>10,} Rs {r[3]:>10,.0f} Rs {r[6]:>8,.0f} Rs {r[7]:>10,.0f} Rs {r[8]:>10,.0f} Rs {r[9]:>8,.0f}')
print('  ' + '-'*95)
jan_net  = sum(r[3] for r in daily_paytm)
jan_ref  = sum(r[6] for r in daily_paytm)
jan_exp  = sum(r[7] for r in daily_paytm)
jan_bank = sum(r[8] for r in daily_paytm)
jan_diff = sum(r[9] for r in daily_paytm)
print(f'  {"JAN TOTAL":12s} {sum(r[1] for r in daily_paytm):>10,} Rs {jan_net:>10,.0f} Rs {jan_ref:>8,.0f} Rs {jan_exp:>10,.0f} Rs {jan_bank:>10,.0f} Rs {jan_diff:>8,.0f}')
print(f'\n  Residual gap after refunds: Rs {jan_diff:,.0f}  ({jan_diff/jan_bank*100:.3f}% of bank deposits)')

# PhonePe Jan26 daily
print('\n--- PhonePe Jan 2026 daily (settlement date = Jan26) ---')
daily_pp = con.execute("""
    WITH sett AS (
        SELECT CAST("Settlement Date" AS DATE) AS dt,
               COUNT(*) AS txns,
               SUM("Transaction Amount") AS gross,
               SUM("Net Amount") AS net,
               ABS(SUM("Total Fees")) AS fees
        FROM phonepe_settlements
        WHERE CAST("Settlement Date" AS DATE) >= '2026-01-01'
          AND CAST("Settlement Date" AS DATE) < '2026-02-01'
          AND ("Transaction Type" NOT LIKE '%REVERSAL%' OR "Transaction Type" IS NULL)
          AND ("Transaction Type" NOT LIKE '%REFUND%' OR "Transaction Type" IS NULL)
          AND "Transaction Type" IS NOT NULL
        GROUP BY 1
    ),
    bank AS (
        SELECT CAST("Transaction" AS DATE) AS dt,
               SUM(CAST("Deposit Amt(INR)" AS DOUBLE)) AS bank_dep
        FROM bank_receipt_from_pg
        WHERE "Payment Gateway" = '05 PhonePe Wallet-2'
          AND CAST("Transaction" AS DATE) >= '2026-01-01'
          AND CAST("Transaction" AS DATE) < '2026-02-01'
        GROUP BY 1
    )
    SELECT COALESCE(s.dt, b.dt) AS dt,
           COALESCE(s.txns,0), COALESCE(s.gross,0), COALESCE(s.net,0), COALESCE(s.fees,0),
           COALESCE(b.bank_dep,0),
           COALESCE(b.bank_dep,0) - COALESCE(s.net,0) AS diff
    FROM sett s FULL JOIN bank b ON s.dt = b.dt
    ORDER BY 1
""").fetchall()

print(f'  {"Date":12s} {"Sett Rows":>10s} {"Gross":>14s} {"Net":>14s} {"Bank Dep":>14s} {"Diff":>12s}')
print('  ' + '-'*82)
for r in daily_pp:
    flag = '*** ' if abs(r[6]) > 50000 else '    '
    print(f'  {flag}{str(r[0]):12s} {r[1]:>10,} Rs {r[2]:>10,.0f} Rs {r[3]:>10,.0f} Rs {r[5]:>10,.0f} Rs {r[6]:>8,.0f}')
print('  ' + '-'*82)
print(f'  {"JAN TOTAL":12s} {sum(r[1] for r in daily_pp):>10,} Rs {sum(r[2] for r in daily_pp):>10,.0f} Rs {sum(r[3] for r in daily_pp):>10,.0f} Rs {sum(r[5] for r in daily_pp):>10,.0f} Rs {sum(r[6] for r in daily_pp):>8,.0f}')
pp_gap = sum(r[6] for r in daily_pp)
pp_bank = sum(r[5] for r in daily_pp)
print(f'\n  PhonePe Jan26 gap: Rs {pp_gap:,.0f}  ({pp_gap/pp_bank*100 if pp_bank else 0:.3f}%)')
print('  Note: PhonePe settles in larger batches (not daily) -- gap may reflect batch timing')

# PayU Jan26 daily
print('\n--- PayU Jan 2026 daily (settlement date = Jan26) ---')
daily_pu = con.execute("""
    WITH sett AS (
        SELECT CAST(LEFT(CAST("AddedOn" AS VARCHAR), 10) AS DATE) AS dt,
               COUNT(*) AS rows,
               SUM("Amount") AS gross,
               SUM("Net Amount") AS net,
               SUM("Amount"-"Net Amount") AS fees
        FROM payu_settlements
        WHERE CAST(LEFT(CAST("AddedOn" AS VARCHAR), 10) AS DATE) >= '2026-01-01'
          AND CAST(LEFT(CAST("AddedOn" AS VARCHAR), 10) AS DATE) < '2026-02-01'
        GROUP BY 1
    ),
    bank AS (
        SELECT CAST("Transaction" AS DATE) AS dt,
               SUM(CAST("Deposit Amt(INR)" AS DOUBLE)) AS bank_dep
        FROM bank_receipt_from_pg
        WHERE "Payment Gateway" = '02 Payu-Wallet'
          AND CAST("Transaction" AS DATE) >= '2026-01-01'
          AND CAST("Transaction" AS DATE) < '2026-02-01'
        GROUP BY 1
    )
    SELECT COALESCE(s.dt, b.dt) AS dt,
           COALESCE(s.rows,0), COALESCE(s.gross,0), COALESCE(s.net,0), COALESCE(s.fees,0),
           COALESCE(b.bank_dep,0),
           COALESCE(b.bank_dep,0) - COALESCE(s.net,0) AS diff
    FROM sett s FULL JOIN bank b ON s.dt = b.dt
    ORDER BY 1
""").fetchall()

print(f'  {"Date":12s} {"Sett Rows":>10s} {"Gross":>14s} {"Net":>14s} {"Bank Dep":>14s} {"Diff":>12s}')
print('  ' + '-'*82)
for r in daily_pu:
    flag = '*** ' if abs(r[6]) > 50000 else '    '
    print(f'  {flag}{str(r[0]):12s} {r[1]:>10,} Rs {r[2]:>10,.0f} Rs {r[3]:>10,.0f} Rs {r[5]:>10,.0f} Rs {r[6]:>8,.0f}')
print('  ' + '-'*82)
print(f'  {"JAN TOTAL":12s} {sum(r[1] for r in daily_pu):>10,} Rs {sum(r[2] for r in daily_pu):>10,.0f} Rs {sum(r[3] for r in daily_pu):>10,.0f} Rs {sum(r[5] for r in daily_pu):>10,.0f} Rs {sum(r[6] for r in daily_pu):>8,.0f}')
pu_gap = sum(r[6] for r in daily_pu)
pu_bank = sum(r[5] for r in daily_pu)
print(f'\n  PayU Jan26 gap: Rs {pu_gap:,.0f}  ({pu_gap/pu_bank*100 if pu_bank else 0:.3f}%)')

# Razorpay Jan26 daily
print('\n--- Razorpay Jan 2026 daily (settled_at = Jan26) ---')
daily_rzp = con.execute("""
    WITH sett AS (
        SELECT settled_at AS dt,
               COUNT(*) AS rows,
               SUM(amount) AS gross,
               SUM(amount)-SUM(fee)-SUM(tax) AS net,
               SUM(fee)+SUM(tax) AS fees
        FROM razorpay_transactions
        WHERE type = 'payment'
          AND settled_at >= '2026-01-01' AND settled_at < '2026-02-01'
        GROUP BY 1
    ),
    bank AS (
        SELECT CAST("Transaction" AS DATE) AS dt,
               SUM(CAST("Deposit Amt(INR)" AS DOUBLE)) AS bank_dep
        FROM bank_receipt_from_pg
        WHERE "Payment Gateway" = '06 Razorpay Wallet'
          AND CAST("Transaction" AS DATE) >= '2026-01-01'
          AND CAST("Transaction" AS DATE) < '2026-02-01'
        GROUP BY 1
    )
    SELECT COALESCE(s.dt, b.dt) AS dt,
           COALESCE(s.rows,0), COALESCE(s.gross,0), COALESCE(s.net,0), COALESCE(s.fees,0),
           COALESCE(b.bank_dep,0),
           COALESCE(b.bank_dep,0) - COALESCE(s.net,0) AS diff
    FROM sett s FULL JOIN bank b ON s.dt = b.dt
    ORDER BY 1
""").fetchall()

print(f'  {"Date":12s} {"Sett Rows":>10s} {"Gross":>14s} {"Net":>14s} {"Bank Dep":>14s} {"Diff":>12s}')
print('  ' + '-'*82)
for r in daily_rzp:
    flag = '*** ' if abs(r[6]) > 50000 else '    '
    print(f'  {flag}{str(r[0]):12s} {r[1]:>10,} Rs {r[2]:>10,.0f} Rs {r[3]:>10,.0f} Rs {r[5]:>10,.0f} Rs {r[6]:>8,.0f}')
print('  ' + '-'*82)
print(f'  {"JAN TOTAL":12s} {sum(r[1] for r in daily_rzp):>10,} Rs {sum(r[2] for r in daily_rzp):>10,.0f} Rs {sum(r[3] for r in daily_rzp):>10,.0f} Rs {sum(r[5] for r in daily_rzp):>10,.0f} Rs {sum(r[6] for r in daily_rzp):>8,.0f}')
rzp_gap = sum(r[6] for r in daily_rzp)
rzp_bank = sum(r[5] for r in daily_rzp)
print(f'\n  Razorpay Jan26 gap: Rs {rzp_gap:,.0f}  ({rzp_gap/rzp_bank*100 if rzp_bank else 0:.3f}%)')

# ======================================================================
# PART D: REFUND IMPACT ANALYSIS
# ======================================================================
print('\n\n' + SEP)
print('PART D: REFUND IMPACT BY GATEWAY (how refunds affect bank deposit)')
print(SEP)

# Paytm refunds by month
ptm_ref_monthly = con.execute("""
    SELECT DATE_TRUNC('month', CAST(LEFT(TRIM(Settled_Date, chr(39)), 10) AS DATE)) AS mo,
           COUNT(*) AS ref_txns,
           SUM(CAST(Amount AS DOUBLE)) AS ref_gross,
           SUM(CAST(Settled_Amount AS DOUBLE)) AS ref_settled
    FROM paytm_refunds
    GROUP BY 1 ORDER BY 1
""").fetchall()

print('\n--- Paytm refunds (deducted from settlement batch before bank transfer) ---')
print(f'  {"Month":12s} {"Refund Txns":>12s} {"Refund Gross (Rs)":>18s} {"Refund Settled (Rs)":>20s}')
print('  ' + '-'*68)
for r in ptm_ref_monthly:
    print(f'  {str(r[0])[:7]:12s} {r[1]:>12,} Rs {r[2]:>14,.0f} Rs {r[3]:>16,.0f}')
print(f'  {"TOTAL":12s} {sum(r[1] for r in ptm_ref_monthly):>12,} Rs {sum(r[2] for r in ptm_ref_monthly):>14,.0f} Rs {sum(r[3] for r in ptm_ref_monthly):>16,.0f}')

# PhonePe refunds in settlement table (REVERSAL rows)
pp_ref = con.execute("""
    SELECT DATE_TRUNC('month', CAST("Settlement Date" AS DATE)) AS mo,
           COUNT(*) AS rows,
           SUM("Transaction Amount") AS gross,
           SUM("Net Amount") AS net
    FROM phonepe_settlements
    WHERE "Transaction Type" LIKE '%REVERSAL%' OR "Transaction Type" LIKE '%REFUND%'
    GROUP BY 1 ORDER BY 1
""").fetchall()

print('\n--- PhonePe refunds/reversals in settlement table ---')
print(f'  {"Month":12s} {"Rows":>8s} {"Gross (Rs)":>16s} {"Net (Rs)":>16s}')
print('  ' + '-'*56)
for r in pp_ref:
    print(f'  {str(r[0])[:7]:12s} {r[1]:>8,} Rs {r[2]:>12,.0f} Rs {r[3]:>12,.0f}')
print(f'  {"TOTAL":12s} {sum(r[1] for r in pp_ref):>8,} Rs {sum(r[2] for r in pp_ref):>12,.0f} Rs {sum(r[3] for r in pp_ref):>12,.0f}')

# PayU refunds in settlement table
pu_ref = con.execute("""
    SELECT DATE_TRUNC('month', CAST("AddedOn" AS TIMESTAMP)) AS mo,
           COUNT(*) AS rows,
           SUM("Amount") AS gross,
           SUM("Net Amount") AS net
    FROM payu_settlements
    WHERE "Status" IN ('Refunded','Chargebacked')
    GROUP BY 1 ORDER BY 1
""").fetchall()

print('\n--- PayU refunds/chargebacks in settlement table ---')
print(f'  {"Month":12s} {"Rows":>8s} {"Gross (Rs)":>16s} {"Net (Rs)":>16s}')
print('  ' + '-'*56)
for r in pu_ref:
    print(f'  {str(r[0])[:7]:12s} {r[1]:>8,} Rs {r[2]:>12,.0f} Rs {r[3]:>12,.0f}')
print(f'  {"TOTAL":12s} {sum(r[1] for r in pu_ref):>8,} Rs {sum(r[2] for r in pu_ref):>12,.0f} Rs {sum(r[3] for r in pu_ref):>12,.0f}')

# Razorpay refunds in transactions table
rzp_ref = con.execute("""
    SELECT DATE_TRUNC('month', settled_at) AS mo,
           COUNT(*) AS rows,
           SUM(debit) AS debit_amount,
           SUM(credit) AS credit_amount
    FROM razorpay_transactions
    WHERE type = 'refund'
    GROUP BY 1 ORDER BY 1
""").fetchall()

print('\n--- Razorpay refunds in transactions table (type=refund) ---')
print(f'  {"Month":12s} {"Rows":>8s} {"Debit (Rs)":>14s} {"Credit (Rs)":>14s}')
print('  ' + '-'*54)
for r in rzp_ref:
    print(f'  {str(r[0])[:7]:12s} {r[1]:>8,} Rs {r[2]:>10,.0f} Rs {r[3]:>10,.0f}')
print(f'  {"TOTAL":12s} {sum(r[1] for r in rzp_ref):>8,} Rs {sum(r[2] for r in rzp_ref):>10,.0f} Rs {sum(r[3] for r in rzp_ref):>10,.0f}')

# ======================================================================
# PART E: OVERALL SCORECARD  (all 4 gateways, Dec25-Feb26 overlap)
# ======================================================================
print('\n\n' + SEP)
print('PART E: OVERALL RECONCILIATION SCORECARD  (all 4 gateways)')
print(SEP)

overlap_months = ('2025-12','2026-01','2026-02')

def overlap_summary(monthly_rows):
    both = [r for r in monthly_rows if r[1] > 0 and r[3] > 0]
    snet = sum(r[2] for r in both)
    bdep = sum(r[4] for r in both)
    gap  = bdep - snet
    return len(both), snet, bdep, gap

ptm_mos, ptm_snet, ptm_bdep, ptm_gap = overlap_summary(paytm_monthly)
pp_mos,  pp_snet,  pp_bdep,  pp_gap  = overlap_summary(pp_monthly)
pu_mos,  pu_snet,  pu_bdep,  pu_gap  = overlap_summary(pu_monthly)
rzp_mos, rzp_snet, rzp_bdep, rzp_gap = overlap_summary(rzp_monthly)

print(f'\n  {"Gateway":14s} {"Matched Mos":>12s} {"Sett Net (Rs)":>18s} {"Bank Dep (Rs)":>18s} {"Gap (Rs)":>14s} {"Gap%":>8s}  Status')
print('  ' + '-'*105)
for gw, mos, snet, bdep, gap in [
    ('Paytm',   ptm_mos, ptm_snet, ptm_bdep, ptm_gap),
    ('PhonePe', pp_mos,  pp_snet,  pp_bdep,  pp_gap),
    ('PayU',    pu_mos,  pu_snet,  pu_bdep,  pu_gap),
    ('Razorpay',rzp_mos, rzp_snet, rzp_bdep, rzp_gap),
]:
    gap_pct = gap/bdep*100 if bdep else 0
    status = 'CLEAN' if abs(gap_pct) < 3 else ('REVIEW' if abs(gap_pct) < 10 else 'MISMATCH')
    print(f'  {gw:14s} {mos:>12,} Rs {snet:>14,.0f} Rs {bdep:>14,.0f} Rs {gap:>10,.0f} {gap_pct:>7.2f}%  [{status}]')

total_snet = ptm_snet + pp_snet + pu_snet + rzp_snet
total_bdep = ptm_bdep + pp_bdep + pu_bdep + rzp_bdep
total_gap  = total_bdep - total_snet
print('  ' + '-'*105)
print(f'  {"ALL PGs":14s} {"":>12s} Rs {total_snet:>14,.0f} Rs {total_bdep:>14,.0f} Rs {total_gap:>10,.0f} {total_gap/total_bdep*100:>7.2f}%')

print(f"""
  Key observations:
  - Paytm: Settlement net includes refund deductions; residual gap ~0.6% likely TDS/platform charges
  - PhonePe: Settlement table uses "Settlement Date"; bank deposits via NEFT in batches (not always daily)
  - PayU: Settlement table includes both forward txns and refunds in "Net Amount"
  - Razorpay: Settlement net = payment amounts - fee - tax (no separate settlement table, embedded in transactions)

  MONTHS WITH NO SETTLEMENT DATA (Apr25-Nov25):
    These months appear in the bank file but PG settlement CSVs only go from Dec25 onwards.
    Bank deposits in Apr25-Nov25:
""")

# Show Apr25-Nov25 bank deposits by gateway
no_sett_bank = con.execute("""
    SELECT "Payment Gateway",
           COUNT(*) AS deps,
           SUM(CAST("Deposit Amt(INR)" AS DOUBLE)) AS total
    FROM bank_receipt_from_pg
    WHERE CAST("Transaction" AS DATE) < '2025-12-01'
    GROUP BY 1 ORDER BY 1
""").fetchall()

for r in no_sett_bank:
    print(f'    {str(r[0]):32s} {r[1]:>5,} deposits  Rs {r[2]:>12,.0f}  (no settlement data)')

con.close()
print('\n' + SEP)
print('END OF SETTLEMENT <-> BANK RECEIPT RECONCILIATION  (ALL 4 GATEWAYS)')
print(SEP)
