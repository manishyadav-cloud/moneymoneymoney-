import duckdb
con = duckdb.connect('data.duckdb', read_only=True)

# First understand date formats across tables
print('='*80)
print('JAN 2026 RECONCILIATION: WIOM DB <-> JUSPAY')
print('='*80)

# Juspay Jan 2026 universe
print('\n--- JUSPAY JAN 2026 UNIVERSE ---')
j_jan = con.execute("""SELECT COUNT(*), SUM(amount)
    FROM juspay_transactions
    WHERE order_date_created >= '2026-01-01' AND order_date_created < '2026-02-01'""").fetchone()
print(f'Total: {j_jan[0]:,}  Amount: {j_jan[1]:,.0f}')

j_jan_success = con.execute("""SELECT COUNT(*), SUM(amount)
    FROM juspay_transactions
    WHERE order_date_created >= '2026-01-01' AND order_date_created < '2026-02-01'
    AND payment_status='SUCCESS'""").fetchone()
print(f'SUCCESS: {j_jan_success[0]:,}  Amount: {j_jan_success[1]:,.0f}')

print('\n--- Juspay Jan by gateway ---')
r = con.execute("""SELECT payment_gateway, COUNT(*) as cnt,
    SUM(CASE WHEN payment_status='SUCCESS' THEN 1 ELSE 0 END) as success,
    SUM(CASE WHEN payment_status='SUCCESS' THEN amount ELSE 0 END) as amt
    FROM juspay_transactions
    WHERE order_date_created >= '2026-01-01' AND order_date_created < '2026-02-01'
    GROUP BY payment_gateway ORDER BY cnt DESC""").fetchall()
for x in r:
    print(f'  {str(x[0]):15s} total={x[1]:>8,}  success={x[2]:>8,}  amt={x[3]:>12,.0f}')

print('\n--- Juspay Jan by order_id pattern ---')
r = con.execute("""SELECT
    CASE
        WHEN order_id LIKE 'custGen_%' THEN 'custGen_*'
        WHEN order_id LIKE 'custWgSubs_%' THEN 'custWgSubs_*'
        WHEN order_id LIKE 'w_%' THEN 'w_*'
        WHEN order_id LIKE 'sd_%' THEN 'sd_*'
        WHEN order_id LIKE 'cusSubs_%' THEN 'cusSubs_*'
        WHEN order_id LIKE 'mr_%' THEN 'mr_*'
        WHEN order_id LIKE 'cxTeam_%' THEN 'cxTeam_*'
        ELSE 'other'
    END as pattern, COUNT(*) as cnt,
    SUM(CASE WHEN payment_status='SUCCESS' THEN amount ELSE 0 END) as amt
    FROM juspay_transactions
    WHERE order_date_created >= '2026-01-01' AND order_date_created < '2026-02-01'
    GROUP BY pattern ORDER BY cnt DESC""").fetchall()
for x in r:
    print(f'  {x[0]:20s} {x[1]:>10,}  amt={x[2]:>12,.0f}')


# =====================================================================
# WIOM TABLES — filter to Jan 2026
# Date formats vary: "Jan 15, 2026", "2026-01", timestamps, etc.
# =====================================================================

print('\n\n' + '='*80)
print('WIOM TABLE JAN 2026 COUNTS')
print('='*80)

# wiom_booking_transactions: CREATED_ON like "Jan %, 2026"
wb_jan = con.execute("""SELECT COUNT(*) FROM wiom_booking_transactions
    WHERE CREATED_ON LIKE 'Jan%2026'""").fetchone()[0]
print(f'wiom_booking_transactions Jan 2026: {wb_jan:,}')

# wiom_primary_revenue: RECHARGE_DT like "Jan %, 2026"
wpr_jan = con.execute("""SELECT COUNT(*), SUM(TOTALPAID) FROM wiom_primary_revenue
    WHERE RECHARGE_DT LIKE 'Jan%2026'""").fetchone()
print(f'wiom_primary_revenue Jan 2026: {wpr_jan[0]:,}  TOTALPAID={wpr_jan[1]:,}')
wpr_jan_online = con.execute("""SELECT COUNT(*), SUM(TOTALPAID) FROM wiom_primary_revenue
    WHERE RECHARGE_DT LIKE 'Jan%2026' AND MODE='online'""").fetchone()
print(f'  (online only): {wpr_jan_online[0]:,}  TOTALPAID={wpr_jan_online[1]:,}')

# wiom_net_income: YR_MNTH = '2026-01'
wni_jan = con.execute("""SELECT COUNT(*), SUM(AMOUNT) FROM wiom_net_income
    WHERE YR_MNTH = '2026-01'""").fetchone()
print(f'wiom_net_income Jan 2026: {wni_jan[0]:,}  AMOUNT={wni_jan[1]:,}')

# wiom_topup_income: DATETIME like "%Jan%2026%" or "%2026-01%"
wti_jan = con.execute("""SELECT COUNT(*), SUM(AMOUNT) FROM wiom_topup_income
    WHERE DATETIME LIKE '%Jan%2026%' OR DATETIME LIKE '%2026-01%'""").fetchone()
print(f'wiom_topup_income Jan 2026: {wti_jan[0]:,}  AMOUNT={wti_jan[1]:,.0f}')

# wiom_customer_security_deposit: CREATED_ON like "Jan%2026"
wsd_jan = con.execute("""SELECT COUNT(*) FROM wiom_customer_security_deposit
    WHERE CREATED_ON LIKE 'Jan%2026'""").fetchone()[0]
print(f'wiom_customer_security_deposit Jan 2026: {wsd_jan:,}')

# wiom_ott_transactions: CREATEDATE in Jan 2026
wott_jan = con.execute("""SELECT COUNT(*) FROM wiom_ott_transactions
    WHERE CREATEDATE >= '2026-01-01' AND CREATEDATE < '2026-02-01'""").fetchone()[0]
print(f'wiom_ott_transactions Jan 2026: {wott_jan:,}')


# =====================================================================
print('\n\n' + '='*80)
print('SECTION A: WIOM BOOKING (Jan) vs JUSPAY (Jan)')
print('='*80)

# Matched
matched = con.execute("""SELECT COUNT(*)
    FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE w.CREATED_ON LIKE 'Jan%2026'
    AND j.order_date_created >= '2026-01-01' AND j.order_date_created < '2026-02-01'""").fetchone()[0]
print(f'\nBooking Jan matched to Juspay Jan: {matched:,} / {wb_jan:,}')

# Wiom booking Jan NOT in Juspay at all
w_not_j = con.execute("""SELECT COUNT(*)
    FROM wiom_booking_transactions w
    LEFT JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE w.CREATED_ON LIKE 'Jan%2026' AND j.order_id IS NULL""").fetchone()[0]
print(f'Booking Jan NOT in Juspay (any month): {w_not_j:,}')

# Wiom booking Jan matched to Juspay but different month
w_diff_month = con.execute("""SELECT COUNT(*)
    FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE w.CREATED_ON LIKE 'Jan%2026'
    AND (j.order_date_created < '2026-01-01' OR j.order_date_created >= '2026-02-01')""").fetchone()[0]
print(f'Booking Jan matched to Juspay but DIFFERENT month: {w_diff_month:,}')

# Status cross-tab
print('\n--- Status cross-tab (Jan matched) ---')
r = con.execute("""SELECT w.RESULTSTATUS, j.payment_status, COUNT(*) as cnt
    FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE w.CREATED_ON LIKE 'Jan%2026'
    GROUP BY w.RESULTSTATUS, j.payment_status ORDER BY cnt DESC""").fetchall()
for x in r:
    print(f'  wiom={str(x[0]):20s} juspay={str(x[1]):12s} {x[2]:>6,}')

# Amount comparison
print('\n--- Amount comparison (Jan matched, both SUCCESS) ---')
r = con.execute("""SELECT COUNT(*) as total,
    SUM(CASE WHEN w.BOOKING_FEE = j.amount THEN 1 ELSE 0 END) as match,
    SUM(CASE WHEN w.BOOKING_FEE != j.amount THEN 1 ELSE 0 END) as mismatch,
    SUM(w.BOOKING_FEE) as wiom_total,
    SUM(j.amount) as juspay_total
    FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE w.CREATED_ON LIKE 'Jan%2026'
    AND w.RESULTSTATUS = 'TXN_SUCCESS' AND j.payment_status = 'SUCCESS'""").fetchdf()
print(r.to_string())

# Amount mismatch details
r2 = con.execute("""SELECT w.BOOKING_TXN_ID, w.BOOKING_FEE, j.amount as juspay_amt,
    j.amount - w.BOOKING_FEE as diff, j.payment_gateway
    FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE w.CREATED_ON LIKE 'Jan%2026'
    AND w.RESULTSTATUS = 'TXN_SUCCESS' AND j.payment_status = 'SUCCESS'
    AND w.BOOKING_FEE != j.amount
    LIMIT 10""").fetchdf()
if len(r2) > 0:
    print('\n--- Amount mismatch samples ---')
    print(r2.to_string())
else:
    print('  No amount mismatches!')


# =====================================================================
print('\n\n' + '='*80)
print('SECTION B: WIOM PRIMARY_REVENUE (Jan, online) vs JUSPAY (Jan)')
print('='*80)

matched_pr = con.execute("""SELECT COUNT(*)
    FROM wiom_primary_revenue pr
    INNER JOIN juspay_transactions j ON pr.TRANSACTION_ID = j.order_id
    WHERE pr.RECHARGE_DT LIKE 'Jan%2026' AND pr.MODE='online'
    AND j.order_date_created >= '2026-01-01' AND j.order_date_created < '2026-02-01'""").fetchone()[0]
print(f'\nPrimary rev Jan online matched to Juspay Jan: {matched_pr:,} / {wpr_jan_online[0]:,}')

# Not in Juspay at all
pr_not_j = con.execute("""SELECT COUNT(*)
    FROM wiom_primary_revenue pr
    LEFT JOIN juspay_transactions j ON pr.TRANSACTION_ID = j.order_id
    WHERE pr.RECHARGE_DT LIKE 'Jan%2026' AND pr.MODE='online' AND j.order_id IS NULL""").fetchone()[0]
print(f'Primary rev Jan online NOT in Juspay: {pr_not_j:,}')

# Breakdown of unmatched by pattern
print('\n--- Unmatched Jan online by ID pattern ---')
r = con.execute("""SELECT
    CASE
        WHEN pr.TRANSACTION_ID LIKE 'BILL_PAID_%' THEN 'BILL_PAID_*'
        WHEN pr.TRANSACTION_ID LIKE 'custGen_%' THEN 'custGen_*'
        WHEN pr.TRANSACTION_ID LIKE 'custWgSubs_%' THEN 'custWgSubs_*'
        WHEN pr.TRANSACTION_ID LIKE 'w_%' THEN 'w_*'
        WHEN pr.TRANSACTION_ID LIKE 'WIFI_SRVC_%' THEN 'WIFI_SRVC_*'
        WHEN pr.TRANSACTION_ID LIKE 'BOOKING_PAYMENT%' THEN 'BOOKING_PAYMENT*'
        ELSE 'other: ' || LEFT(pr.TRANSACTION_ID, 20)
    END as pattern, COUNT(*) as cnt, SUM(pr.TOTALPAID) as total_paid
    FROM wiom_primary_revenue pr
    LEFT JOIN juspay_transactions j ON pr.TRANSACTION_ID = j.order_id
    WHERE pr.RECHARGE_DT LIKE 'Jan%2026' AND pr.MODE='online' AND j.order_id IS NULL
    GROUP BY pattern ORDER BY cnt DESC""").fetchall()
for x in r:
    print(f'  {x[0]:30s} {x[1]:>8,}  amt={x[2]:>12,}')

# Amount comparison (matched)
print('\n--- Amount comparison (Jan matched, Juspay SUCCESS) ---')
r = con.execute("""SELECT COUNT(*) as total,
    SUM(CASE WHEN pr.TOTALPAID = j.amount THEN 1 ELSE 0 END) as match,
    SUM(CASE WHEN pr.TOTALPAID != j.amount THEN 1 ELSE 0 END) as mismatch,
    SUM(pr.TOTALPAID) as wiom_total,
    SUM(j.amount) as juspay_total,
    SUM(j.amount) - SUM(pr.TOTALPAID) as diff
    FROM wiom_primary_revenue pr
    INNER JOIN juspay_transactions j ON pr.TRANSACTION_ID = j.order_id
    WHERE pr.RECHARGE_DT LIKE 'Jan%2026' AND pr.MODE='online'
    AND j.payment_status='SUCCESS'""").fetchdf()
print(r.to_string())

# Amount mismatch details
r2 = con.execute("""SELECT pr.TRANSACTION_ID, pr.TOTALPAID, j.amount as juspay_amt,
    j.amount - pr.TOTALPAID as diff, j.payment_gateway, pr.PLAN_TYPE
    FROM wiom_primary_revenue pr
    INNER JOIN juspay_transactions j ON pr.TRANSACTION_ID = j.order_id
    WHERE pr.RECHARGE_DT LIKE 'Jan%2026' AND pr.MODE='online'
    AND j.payment_status='SUCCESS' AND pr.TOTALPAID != j.amount
    LIMIT 10""").fetchdf()
if len(r2) > 0:
    print('\n--- Amount mismatch samples ---')
    print(r2.to_string())


# =====================================================================
print('\n\n' + '='*80)
print('SECTION C: WIOM NET_INCOME (Jan) vs JUSPAY (Jan)')
print('='*80)

wni_jan_cnt = con.execute("SELECT COUNT(*) FROM wiom_net_income WHERE YR_MNTH='2026-01'").fetchone()[0]

matched_ni = con.execute("""SELECT COUNT(*)
    FROM wiom_net_income ni
    INNER JOIN juspay_transactions j ON ni.TXN_ID = j.order_id
    WHERE ni.YR_MNTH = '2026-01'
    AND j.order_date_created >= '2026-01-01' AND j.order_date_created < '2026-02-01'""").fetchone()[0]
print(f'\nNet income Jan matched to Juspay Jan: {matched_ni:,} / {wni_jan_cnt:,}')

# Cross-month matches
ni_cross = con.execute("""SELECT COUNT(*)
    FROM wiom_net_income ni
    INNER JOIN juspay_transactions j ON ni.TXN_ID = j.order_id
    WHERE ni.YR_MNTH = '2026-01'
    AND (j.order_date_created < '2026-01-01' OR j.order_date_created >= '2026-02-01')""").fetchone()[0]
print(f'Net income Jan matched to Juspay OTHER month: {ni_cross:,}')

# Not in Juspay
ni_not_j = con.execute("""SELECT COUNT(*)
    FROM wiom_net_income ni
    LEFT JOIN juspay_transactions j ON ni.TXN_ID = j.order_id
    WHERE ni.YR_MNTH = '2026-01' AND j.order_id IS NULL""").fetchone()[0]
print(f'Net income Jan NOT in Juspay: {ni_not_j:,}')

# Amount comparison
print('\n--- Amount comparison (Jan matched, Juspay SUCCESS) ---')
r = con.execute("""SELECT COUNT(*) as total,
    SUM(CASE WHEN ni.AMOUNT = j.amount THEN 1 ELSE 0 END) as match,
    SUM(CASE WHEN ni.AMOUNT != j.amount THEN 1 ELSE 0 END) as mismatch,
    SUM(ni.AMOUNT) as wiom_total,
    SUM(j.amount) as juspay_total,
    SUM(j.amount) - SUM(ni.AMOUNT) as diff
    FROM wiom_net_income ni
    INNER JOIN juspay_transactions j ON ni.TXN_ID = j.order_id
    WHERE ni.YR_MNTH = '2026-01' AND j.payment_status='SUCCESS'""").fetchdf()
print(r.to_string())


# =====================================================================
print('\n\n' + '='*80)
print('SECTION D: JUSPAY JAN ORPHANS (not in any Wiom table)')
print('='*80)

orphans = con.execute("""
    SELECT COUNT(*) FROM juspay_transactions j
    WHERE j.order_date_created >= '2026-01-01' AND j.order_date_created < '2026-02-01'
    AND j.order_id NOT IN (SELECT BOOKING_TXN_ID FROM wiom_booking_transactions)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_primary_revenue)
    AND j.order_id NOT IN (SELECT TXN_ID FROM wiom_net_income)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_topup_income)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_customer_security_deposit)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_ott_transactions)
""").fetchone()[0]
j_jan_total = con.execute("""SELECT COUNT(*) FROM juspay_transactions
    WHERE order_date_created >= '2026-01-01' AND order_date_created < '2026-02-01'""").fetchone()[0]
print(f'\nJuspay Jan orphans: {orphans:,} / {j_jan_total:,} ({orphans/j_jan_total*100:.1f}%)')

print('\n--- Orphan breakdown by pattern + status ---')
r = con.execute("""
    SELECT
        CASE
            WHEN j.order_id LIKE 'custGen_%' THEN 'custGen_*'
            WHEN j.order_id LIKE 'custWgSubs_%' THEN 'custWgSubs_*'
            WHEN j.order_id LIKE 'w_%' THEN 'w_*'
            WHEN j.order_id LIKE 'cusSubs_%' THEN 'cusSubs_*'
            WHEN j.order_id LIKE 'mr_%' THEN 'mr_*'
            WHEN j.order_id LIKE 'cxTeam_%' THEN 'cxTeam_*'
            ELSE 'other'
        END as pattern, j.payment_status, COUNT(*) as cnt,
        SUM(j.amount) as total_amt
    FROM juspay_transactions j
    WHERE j.order_date_created >= '2026-01-01' AND j.order_date_created < '2026-02-01'
    AND j.order_id NOT IN (SELECT BOOKING_TXN_ID FROM wiom_booking_transactions)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_primary_revenue)
    AND j.order_id NOT IN (SELECT TXN_ID FROM wiom_net_income)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_topup_income)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_customer_security_deposit)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_ott_transactions)
    GROUP BY pattern, j.payment_status ORDER BY cnt DESC
""").fetchall()
for x in r:
    print(f'  {x[0]:20s} status={str(x[1]):10s} {x[2]:>8,}  amt={x[3]:>12,.0f}')

# Sample orphan custGen Jan
print('\n--- Sample orphan custGen Jan ---')
r = con.execute("""
    SELECT j.order_id, j.payment_gateway, j.amount, j.order_date_created, j.payment_status
    FROM juspay_transactions j
    WHERE j.order_date_created >= '2026-01-01' AND j.order_date_created < '2026-02-01'
    AND j.order_id LIKE 'custGen_%' AND j.payment_status='SUCCESS'
    AND j.order_id NOT IN (SELECT BOOKING_TXN_ID FROM wiom_booking_transactions)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_primary_revenue)
    AND j.order_id NOT IN (SELECT TXN_ID FROM wiom_net_income)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_topup_income)
    LIMIT 5
""").fetchdf()
print(r.to_string())


# =====================================================================
print('\n\n' + '='*80)
print('SECTION E: SUMMARY — JAN 2026 RECONCILIATION SCORECARD')
print('='*80)

# Totals
j_jan_total = con.execute("""SELECT COUNT(*), SUM(amount) FROM juspay_transactions
    WHERE order_date_created >= '2026-01-01' AND order_date_created < '2026-02-01'
    AND payment_status='SUCCESS'""").fetchone()

print(f"""
JUSPAY JAN 2026 (SUCCESS only):
  Transactions: {j_jan_total[0]:>12,}
  Total Amount: {j_jan_total[1]:>12,.0f} INR

WIOM COVERAGE (matched to Juspay Jan):
  net_income:     {matched_ni:>12,} matched
  primary_rev:    {matched_pr:>12,} matched (online)
  booking:        {matched:>12,} matched

JUSPAY JAN ORPHANS: {orphans:>12,} ({orphans/j_jan_total[0]*100:.2f}%)
""")

con.close()
