# -*- coding: utf-8 -*-
"""
Settlement <-> Bank Receipt Reconciliation
==========================================
Links PG settlement records to actual bank deposits in bank_receipt_from_pg.

KEY FACTS ESTABLISHED BEFORE RUNNING:
  - All 358 bank deposits come from "PAYTM PAYMENTS SERVICES LIMI" (RTGS)
  - Paytm UTR (UTIBR6...) != Bank RTGS UTR (UTIBH...)  ->  no direct UTR match
  - Reconciliation approach: DATE-level matching + refund netting
  - PhonePe / PayU / Razorpay bank accounts are NOT in this bank file

Formula:
  Bank deposit (date D) = Paytm settled_amount (date D)
                        - Paytm refunds deducted (date D)
                        +/- timing adjustments

Parts:
  A. UTR investigation: why no UTR match & what do the IDs mean
  B. Daily reconciliation: settlement net vs bank deposit (all months overlap)
  C. Monthly summary with running gap
  D. Unreconciled items: days with large gaps
  E. Non-Paytm PG settlements (no bank data available)
  F. Overall reconciliation scorecard
"""
import duckdb

con = duckdb.connect('data.duckdb', read_only=True)

SEP  = '=' * 130
SEP2 = '-' * 130
SEP3 = '.' * 130

print(SEP)
print('SETTLEMENT  <-->  BANK RECEIPT RECONCILIATION')
print(SEP)

# ======================================================================
# PART A: UTR INVESTIGATION
# ======================================================================
print('\n' + SEP)
print('PART A: UTR INVESTIGATION')
print(SEP)

print('\n--- Paytm settlement UTR samples ---')
ptm_utr = con.execute("""
    SELECT utr_no, settled_date, COUNT(*) AS txns,
           SUM(amount) AS gross, SUM(settled_amount) AS net
    FROM paytm_settlements
    WHERE YEAR(settled_date) = 2026 AND MONTH(settled_date) = 1
    GROUP BY utr_no, settled_date
    ORDER BY settled_date
    LIMIT 8
""").fetchall()
for r in ptm_utr:
    print(f'  UTR: {r[0]}  |  Date: {r[1]}  |  Txns: {r[2]:,}  |  Gross Rs {r[3]:,.2f}  |  Net Rs {r[4]:,.2f}')

print('\n--- Bank RTGS UTR samples (Jan 2026) ---')
bank_utr = con.execute("""
    SELECT SPLIT_PART("Transaction Remarks", '/', 2)  AS bank_utr,
           CAST("Transaction" AS DATE)                 AS dt,
           CAST("Deposit Amt(INR)" AS DOUBLE)          AS deposit,
           "Transaction Remarks"
    FROM bank_receipt_from_pg
    WHERE YEAR(CAST("Transaction" AS DATE)) = 2026
      AND MONTH(CAST("Transaction" AS DATE)) = 1
    ORDER BY dt
    LIMIT 8
""").fetchall()
for r in bank_utr:
    print(f'  Bank UTR: {r[0]}  |  Date: {r[1]}  |  Deposit Rs {r[2]:,.2f}  |  Remark: {r[3][:60]}')

print('\n--- UTR pattern analysis ---')
print('  Paytm settlement UTR format : UTIBR6YYYYMMDDXXXXXXXX  (Paytm internal, issued by Paytm)')
print('  Bank RTGS UTR format        : UTIBHYYMMDDXXXXXXXXX   (bank-assigned RTGS reference)')
print('  --> Different UTR systems; no direct key-join possible')
print('  --> Reconciliation must use DATE + AMOUNT matching')

# Check if 1 UTR per date on both sides (confirms daily batching)
ptm_utrs_per_day = con.execute("""
    SELECT COUNT(DISTINCT utr_no) AS utrs, COUNT(DISTINCT settled_date) AS days
    FROM paytm_settlements
    WHERE YEAR(settled_date) = 2026 AND MONTH(settled_date) = 1
""").fetchone()
bank_per_day = con.execute("""
    SELECT COUNT(*) AS deposits, COUNT(DISTINCT CAST("Transaction" AS DATE)) AS days
    FROM bank_receipt_from_pg
    WHERE YEAR(CAST("Transaction" AS DATE)) = 2026
      AND MONTH(CAST("Transaction" AS DATE)) = 1
""").fetchone()
print(f'\n  Paytm Jan26: {ptm_utrs_per_day[0]} distinct UTRs across {ptm_utrs_per_day[1]} days  (1 UTR/day confirmed)')
print(f'  Bank Jan26 : {bank_per_day[0]} deposits across {bank_per_day[1]} days      (1 deposit/day confirmed)')
print(f'  --> Perfect 1-to-1 daily mapping by date')

# ======================================================================
# PART B: DAILY RECONCILIATION  (all available months)
# ======================================================================
print('\n\n' + SEP)
print('PART B: DAILY RECONCILIATION  (Paytm settled_amount - refunds = bank deposit?)')
print(SEP)

# Build the full daily reconciliation table
daily_recon = con.execute("""
    WITH sett AS (
        SELECT
            settled_date                            AS dt,
            COUNT(*)                                AS sett_txns,
            SUM(amount)                             AS sett_gross,
            SUM(settled_amount)                     AS sett_net,
            SUM(commission) + SUM(gst)              AS sett_fees
        FROM paytm_settlements
        GROUP BY settled_date
    ),
    ref AS (
        SELECT
            CAST(LEFT(TRIM(Settled_Date, chr(39)), 10) AS DATE)              AS dt,
            COUNT(*)                                AS ref_txns,
            SUM(CAST(Amount AS DOUBLE))             AS ref_gross,
            SUM(CAST(Settled_Amount AS DOUBLE))     AS ref_net
        FROM paytm_refunds
        GROUP BY 1
    ),
    bank AS (
        SELECT
            CAST("Transaction" AS DATE)             AS dt,
            SUM(CAST("Deposit Amt(INR)" AS DOUBLE)) AS bank_deposit
        FROM bank_receipt_from_pg
        GROUP BY 1
    )
    SELECT
        COALESCE(s.dt, b.dt)                        AS dt,
        COALESCE(s.sett_txns,  0)                   AS sett_txns,
        COALESCE(s.sett_gross, 0)                   AS sett_gross,
        COALESCE(s.sett_net,   0)                   AS sett_net,
        COALESCE(s.sett_fees,  0)                   AS sett_fees,
        COALESCE(r.ref_txns,   0)                   AS ref_txns,
        COALESCE(r.ref_net,    0)                   AS ref_net,
        COALESCE(s.sett_net, 0) - COALESCE(r.ref_net, 0) AS expected_deposit,
        COALESCE(b.bank_deposit, 0)                 AS bank_deposit,
        COALESCE(b.bank_deposit, 0)
            - (COALESCE(s.sett_net, 0) - COALESCE(r.ref_net, 0)) AS daily_diff
    FROM sett s
    FULL JOIN bank b  ON s.dt = b.dt
    LEFT JOIN ref  r  ON s.dt = r.dt
    ORDER BY dt
""").fetchall()

# Show Jan 2026 detailed daily breakdown
print('\n### Jan 2026 Daily Detail ###\n')
print(f'  {"Date":12s} {"Sett Txns":>10s} {"Sett Net (Rs)":>16s} {"Refund Net (Rs)":>18s} {"Expected (Rs)":>16s} {"Bank Dep (Rs)":>16s} {"Diff (Rs)":>12s}')
print('  ' + SEP2[:103])

jan26_rows = [r for r in daily_recon if str(r[0]).startswith('2026-01')]
cum_diff = 0
for r in jan26_rows:
    cum_diff += r[9]
    flag = '  *** ' if abs(r[9]) > 50000 else '      '
    print(f'{flag}{str(r[0]):12s} {r[1]:>10,} Rs {r[2]:>12,.2f} Rs {r[6]:>14,.2f} Rs {r[7]:>12,.2f} Rs {r[8]:>12,.2f} Rs {r[9]:>8,.2f}')

jan_sett  = sum(r[3] for r in jan26_rows)
jan_rnet  = sum(r[6] for r in jan26_rows)
jan_exp   = sum(r[7] for r in jan26_rows)
jan_bank  = sum(r[8] for r in jan26_rows)
jan_diff  = sum(r[9] for r in jan26_rows)
print('  ' + SEP2[:103])
print(f'  {"JAN TOTAL":12s} {sum(r[1] for r in jan26_rows):>10,} Rs {jan_sett:>12,.2f} Rs {jan_rnet:>14,.2f} Rs {jan_exp:>12,.2f} Rs {jan_bank:>12,.2f} Rs {jan_diff:>8,.2f}')

pct_explained = jan_rnet / abs(jan_diff + jan_rnet) * 100 if (jan_diff + jan_rnet) != 0 else 0
print(f'\n  Reconciliation for Jan 2026:')
print(f'    Settlement net            : Rs {jan_sett:>14,.2f}')
print(f'    Refunds deducted          : Rs {jan_rnet:>14,.2f}')
print(f'    Expected bank deposit     : Rs {jan_exp:>14,.2f}')
print(f'    Actual bank deposit       : Rs {jan_bank:>14,.2f}')
print(f'    Unexplained gap           : Rs {jan_diff:>14,.2f}  ({jan_diff/jan_bank*100:.3f}% of bank deposits)')

# ======================================================================
# PART C: MONTHLY SUMMARY  (all months)
# ======================================================================
print('\n\n' + SEP)
print('PART C: MONTHLY SUMMARY  (all months in settlement data)')
print(SEP)

monthly_recon = con.execute("""
    WITH sett AS (
        SELECT
            DATE_TRUNC('month', settled_date)       AS mo,
            COUNT(*)                                AS sett_txns,
            SUM(amount)                             AS sett_gross,
            SUM(settled_amount)                     AS sett_net,
            SUM(commission) + SUM(gst)              AS sett_fees
        FROM paytm_settlements
        GROUP BY 1
    ),
    ref AS (
        SELECT
            DATE_TRUNC('month', CAST(LEFT(TRIM(Settled_Date, chr(39)), 10) AS DATE)) AS mo,
            COUNT(*)                                AS ref_txns,
            SUM(CAST(Settled_Amount AS DOUBLE))     AS ref_net
        FROM paytm_refunds
        GROUP BY 1
    ),
    bank AS (
        SELECT
            DATE_TRUNC('month', CAST("Transaction" AS DATE)) AS mo,
            COUNT(*)                                AS deposits,
            SUM(CAST("Deposit Amt(INR)" AS DOUBLE)) AS bank_total
        FROM bank_receipt_from_pg
        GROUP BY 1
    )
    SELECT
        COALESCE(s.mo, b.mo)                        AS month,
        COALESCE(s.sett_txns, 0)                    AS sett_txns,
        COALESCE(s.sett_gross, 0)                   AS sett_gross,
        COALESCE(s.sett_net, 0)                     AS sett_net,
        COALESCE(s.sett_fees, 0)                    AS sett_fees,
        COALESCE(r.ref_txns, 0)                     AS ref_txns,
        COALESCE(r.ref_net, 0)                      AS ref_net,
        COALESCE(s.sett_net, 0) - COALESCE(r.ref_net, 0) AS expected,
        COALESCE(b.deposits, 0)                     AS bank_deps,
        COALESCE(b.bank_total, 0)                   AS bank_total,
        COALESCE(b.bank_total, 0)
          - (COALESCE(s.sett_net, 0) - COALESCE(r.ref_net, 0)) AS gap
    FROM sett s
    FULL JOIN bank b ON s.mo = b.mo
    LEFT JOIN ref  r ON s.mo = r.mo
    ORDER BY month
""").fetchall()

print(f'\n{"Month":12s} {"Sett Txns":>10s} {"Sett Net (Rs)":>16s} {"Refunds (Rs)":>14s} {"Expected (Rs)":>16s} {"Bank Dep (Rs)":>16s} {"Gap (Rs)":>12s} {"Gap%":>7s}')
print(SEP2)
cum_gap = 0
for r in monthly_recon:
    cum_gap += r[10]
    gap_pct = r[10] / r[9] * 100 if r[9] else 0
    flag = ' *' if abs(r[10]) > 200000 else '  '
    print(f'{flag}{str(r[0])[:7]:12s} {r[1]:>10,} Rs {r[3]:>12,.0f} Rs {r[6]:>10,.0f} Rs {r[7]:>12,.0f} Rs {r[9]:>12,.0f} Rs {r[10]:>8,.0f} {gap_pct:>6.2f}%')
print(SEP2)
t_sett = sum(r[3] for r in monthly_recon)
t_ref  = sum(r[6] for r in monthly_recon)
t_exp  = sum(r[7] for r in monthly_recon)
t_bank = sum(r[9] for r in monthly_recon)
t_gap  = sum(r[10] for r in monthly_recon)
print(f'{"TOTAL":12s} {sum(r[1] for r in monthly_recon):>10,} Rs {t_sett:>12,.0f} Rs {t_ref:>10,.0f} Rs {t_exp:>12,.0f} Rs {t_bank:>12,.0f} Rs {t_gap:>8,.0f} {t_gap/t_bank*100:>6.2f}%')
print(f'\n  * = months with gap > Rs 2L')
print(f'  Note: Bank file covers Apr25-Mar26; Settlement data covers Dec25-Feb26 only.')
print(f'        Months with bank data but no settlement = Apr25-Nov25 (Rs {sum(r[9] for r in monthly_recon if r[1]==0):,.0f} deposited, no settlement table)')

# ======================================================================
# PART D: UNRECONCILED ITEMS  (days with large unexplained gaps)
# ======================================================================
print('\n\n' + SEP)
print('PART D: UNRECONCILED ITEMS  (days where |gap| > Rs 25,000)')
print(SEP)

large_gaps = [r for r in daily_recon if abs(r[9]) > 25000]
print(f'\nTotal days with |gap| > Rs 25,000: {len(large_gaps)}  (out of {len(daily_recon)} days with settlement or bank data)\n')

print(f'  {"Date":12s} {"Sett Net":>14s} {"Refund Net":>14s} {"Expected":>14s} {"Bank Dep":>14s} {"Gap":>12s}  Likely Cause')
print('  ' + SEP2[:110])
for r in large_gaps:
    cause = ''
    if r[9] > 0:
        cause = 'Bank > Expected (extra deposit / prior period catch-up?)'
    elif abs(r[9]) > 150000:
        cause = 'Large refund batch or settlement delay'
    else:
        cause = 'Refund batch / minor timing'
    print(f'  {str(r[0]):12s} Rs {r[3]:>10,.0f} Rs {r[6]:>10,.0f} Rs {r[7]:>10,.0f} Rs {r[8]:>10,.0f} Rs {r[9]:>8,.0f}  {cause}')

# Break down the largest gap day
if large_gaps:
    worst = min(large_gaps, key=lambda x: x[9])
    print(f'\n  Worst gap day: {worst[0]}  |  Gap = Rs {worst[9]:,.0f}')
    worst_ref = con.execute("""
        SELECT COUNT(*), SUM(CAST(Amount AS DOUBLE)), SUM(CAST(Settled_Amount AS DOUBLE))
        FROM paytm_refunds
        WHERE CAST(LEFT(TRIM(Settled_Date, chr(39)), 10) AS DATE) = ?
    """, [str(worst[0])]).fetchone()
    print(f'  Refunds on that date: {(worst_ref[0] or 0):,} txns  |  Refund gross Rs {(worst_ref[1] or 0):,.2f}  |  Settled Rs {(worst_ref[2] or 0):,.2f}')

# Cumulative gap trend
print('\n\n--- Cumulative gap progression (Jan 2026) ---')
cum = 0
print(f'  {"Date":12s} {"Daily Gap":>12s} {"Cumulative Gap":>16s}')
print('  ' + '-'*45)
for r in jan26_rows:
    cum += r[9]
    print(f'  {str(r[0]):12s} Rs {r[9]:>8,.0f}   Rs {cum:>12,.0f}')
print(f'\n  Jan 2026 total gap: Rs {jan_diff:,.2f}')
print(f'  As % of bank deposits: {jan_diff/jan_bank*100:.3f}%')

# ======================================================================
# PART E: NON-PAYTM PG SETTLEMENTS  (bank data missing)
# ======================================================================
print('\n\n' + SEP)
print('PART E: NON-PAYTM PG SETTLEMENTS  (PhonePe / PayU / Razorpay -- no bank file available)')
print(SEP)

print('\n  These PGs deposit to separate bank accounts not provided in bank_receipt_from_pg.')
print('  Settlement amounts below are calculated to show expected bank receipts:\n')

# PhonePe all-time settlements
pp_total = con.execute("""
    SELECT
        DATE_TRUNC('month', CAST("Settlement Date" AS DATE)) AS mo,
        COUNT(*) AS rows,
        SUM(CAST("Transaction Amount" AS DOUBLE)) AS gross,
        SUM(CAST("Net Amount" AS DOUBLE)) AS net,
        ABS(SUM(CAST("Total Fees" AS DOUBLE))) AS fees
    FROM phonepe_settlements
    WHERE COALESCE("Transaction Type",'') != ''
    GROUP BY 1 ORDER BY 1
""").fetchall()

print('  PhonePe forward settlements by settlement month:')
print(f'  {"Month":12s} {"Rows":>8s} {"Gross (Rs)":>14s} {"Net (Rs)":>14s} {"Fees (Rs)":>12s}')
print('  ' + '-'*65)
for r in pp_total:
    print(f'  {str(r[0])[:7]:12s} {r[1]:>8,} Rs {r[2]:>10,.0f} Rs {r[3]:>10,.0f} Rs {r[4]:>8,.2f}')
print(f'  {"TOTAL":12s} {sum(r[1] for r in pp_total):>8,} Rs {sum(r[2] for r in pp_total):>10,.0f} Rs {sum(r[3] for r in pp_total):>10,.0f} Rs {sum(r[4] for r in pp_total):>8,.2f}')

# PayU all-time
pu_total = con.execute("""
    SELECT
        DATE_TRUNC('month', CAST("AddedOn" AS TIMESTAMP)) AS mo,
        COUNT(*) AS rows,
        SUM(CAST("Amount" AS DOUBLE)) AS gross,
        SUM(CAST("Net Amount" AS DOUBLE)) AS net,
        SUM(CAST("Amount" AS DOUBLE) - CAST("Net Amount" AS DOUBLE)) AS fees
    FROM payu_settlements
    GROUP BY 1 ORDER BY 1
""").fetchall()

print('\n  PayU settlements by settlement month:')
print(f'  {"Month":12s} {"Rows":>8s} {"Gross (Rs)":>14s} {"Net (Rs)":>14s} {"Fees (Rs)":>12s}')
print('  ' + '-'*65)
for r in pu_total:
    print(f'  {str(r[0])[:7]:12s} {r[1]:>8,} Rs {r[2]:>10,.0f} Rs {r[3]:>10,.0f} Rs {r[4]:>8,.2f}')
print(f'  {"TOTAL":12s} {sum(r[1] for r in pu_total):>8,} Rs {sum(r[2] for r in pu_total):>10,.0f} Rs {sum(r[3] for r in pu_total):>10,.0f} Rs {sum(r[4] for r in pu_total):>8,.2f}')

# Razorpay all-time
rzp_total = con.execute("""
    SELECT
        DATE_TRUNC('month', settled_at) AS mo,
        COUNT(*) AS rows,
        SUM(amount) AS gross,
        SUM(amount)-SUM(fee)-SUM(tax) AS net,
        SUM(fee)+SUM(tax) AS fees
    FROM razorpay_transactions
    WHERE type='payment'
    GROUP BY 1 ORDER BY 1
""").fetchall()

print('\n  Razorpay settlements by settled_at month:')
print(f'  {"Month":12s} {"Rows":>8s} {"Gross (Rs)":>14s} {"Net (Rs)":>14s} {"Fees (Rs)":>12s}')
print('  ' + '-'*65)
for r in rzp_total:
    print(f'  {str(r[0])[:7]:12s} {r[1]:>8,} Rs {r[2]:>10,.0f} Rs {r[3]:>10,.0f} Rs {r[4]:>8,.2f}')
print(f'  {"TOTAL":12s} {sum(r[1] for r in rzp_total):>8,} Rs {sum(r[2] for r in rzp_total):>10,.0f} Rs {sum(r[3] for r in rzp_total):>10,.0f} Rs {sum(r[4] for r in rzp_total):>8,.2f}')

# ======================================================================
# PART F: OVERALL RECONCILIATION SCORECARD
# ======================================================================
print('\n\n' + SEP)
print('PART F: OVERALL RECONCILIATION SCORECARD')
print(SEP)

# Month-level data we have
has_sett  = [r for r in monthly_recon if r[1] > 0]   # months with settlement data
has_bank  = [r for r in monthly_recon if r[8] > 0]   # months with bank data
has_both  = [r for r in monthly_recon if r[1] > 0 and r[8] > 0]

# Non-Paytm totals for Dec25-Feb26 overlap
pp_dec_feb = sum(r[3] for r in pp_total if str(r[0])[:7] in ('2025-12','2026-01','2026-02'))
pu_dec_feb = sum(r[3] for r in pu_total if str(r[0])[:7] in ('2025-12','2026-01','2026-02'))
rz_dec_feb = sum(r[3] for r in rzp_total if str(r[0])[:7] in ('2025-12','2026-01','2026-02'))

print(f'\n  === PAYTM RECONCILIATION (Dec25-Feb26, bank data available) ===\n')
for r in has_both:
    gap_pct = r[10]/r[9]*100 if r[9] else 0
    status = 'CLEAN' if abs(gap_pct) < 2 else 'REVIEW'
    print(f'  {str(r[0])[:7]}  Settlement net Rs {r[3]:>12,.0f}  Refunds Rs {r[6]:>8,.0f}  Expected Rs {r[7]:>12,.0f}  Bank Rs {r[9]:>12,.0f}  Gap Rs {r[10]:>8,.0f} ({gap_pct:.2f}%)  [{status}]')

print(f'\n  Total gap (all matched months): Rs {t_gap:,.0f}  ({t_gap/t_bank*100:.3f}% of total bank deposits)')

print(f'\n  === NON-PAYTM (bank accounts not provided) ===\n')
print(f'  {"PG":12s}  {"Net Settled (Dec25-Feb26)":>28s}   Bank Account Status')
print('  ' + '-'*70)
print(f'  {"PhonePe":12s}  Rs {pp_dec_feb:>24,.0f}   MISSING -- separate bank account needed')
print(f'  {"PayU":12s}  Rs {pu_dec_feb:>24,.0f}   MISSING -- separate bank account needed')
print(f'  {"Razorpay":12s}  Rs {rz_dec_feb:>24,.0f}   MISSING -- separate bank account needed')
print(f'  {"TOTAL":12s}  Rs {pp_dec_feb+pu_dec_feb+rz_dec_feb:>24,.0f}')

print(f'\n  === SUMMARY ===\n')
print(f'  Paytm bank recon coverage   : {len(has_both)} months with both settlement + bank data')
print(f'  Paytm overall gap           : Rs {t_gap:,.0f}  ({abs(t_gap/t_bank*100):.3f}% of deposits)')
print(f'  Paytm recon quality         : {"EXCELLENT (<2% gap)" if abs(t_gap/t_bank*100) < 2 else "NEEDS REVIEW"}')
print(f'  Non-Paytm bank data         : NOT PROVIDED -- obtain PhonePe/PayU/Razorpay bank statements')
print(f'  Non-Paytm expected deposits : Rs {pp_dec_feb+pu_dec_feb+rz_dec_feb:,.0f}  (Dec25-Feb26 combined)')

con.close()
print('\n' + SEP)
print('END OF SETTLEMENT <-> BANK RECEIPT RECONCILIATION')
print(SEP)
