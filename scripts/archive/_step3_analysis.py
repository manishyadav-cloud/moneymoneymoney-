import duckdb
con = duckdb.connect('data.duckdb', read_only=True)

print('='*80)
print('STEP 3: JUSPAY RECORDS NOT IN ANY WIOM TABLE')
print('='*80)

# Build a unified view of all Wiom transaction IDs
# wiom_booking -> BOOKING_TXN_ID
# wiom_primary_revenue -> TRANSACTION_ID
# wiom_net_income -> TXN_ID
# wiom_topup_income -> TRANSACTION_ID
# wiom_customer_security_deposit -> TRANSACTION_ID
# wiom_ott_transactions -> TRANSACTION_ID

print('\n--- Juspay matched to EACH Wiom table ---')
for tbl, col in [
    ('wiom_net_income', 'TXN_ID'),
    ('wiom_primary_revenue', 'TRANSACTION_ID'),
    ('wiom_topup_income', 'TRANSACTION_ID'),
    ('wiom_booking_transactions', 'BOOKING_TXN_ID'),
    ('wiom_customer_security_deposit', 'TRANSACTION_ID'),
    ('wiom_ott_transactions', 'TRANSACTION_ID'),
]:
    cnt = con.execute(f"""SELECT COUNT(DISTINCT j.order_id) FROM juspay_transactions j
        INNER JOIN {tbl} w ON j.order_id = w.{col}""").fetchone()[0]
    print(f'  Juspay matched to {tbl:40s} via {col:20s} = {cnt:>10,}')

# Juspay not in ANY wiom table
print('\n--- Juspay NOT in any Wiom table ---')
cnt = con.execute("""
    SELECT COUNT(*) FROM juspay_transactions j
    WHERE j.order_id NOT IN (SELECT BOOKING_TXN_ID FROM wiom_booking_transactions)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_primary_revenue)
    AND j.order_id NOT IN (SELECT TXN_ID FROM wiom_net_income)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_topup_income)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_customer_security_deposit)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_ott_transactions)
""").fetchone()[0]
j_total = con.execute("SELECT COUNT(*) FROM juspay_transactions").fetchone()[0]
print(f'  Juspay orphans (not in any Wiom table): {cnt:,} / {j_total:,}')

# What are these orphans?
print('\n--- Orphan Juspay by order_id pattern ---')
r = con.execute("""
    SELECT
        CASE
            WHEN j.order_id LIKE 'custGen_%' THEN 'custGen_*'
            WHEN j.order_id LIKE 'custWgSubs_%' THEN 'custWgSubs_*'
            WHEN j.order_id LIKE 'w_%' THEN 'w_*'
            WHEN j.order_id LIKE 'sd_%' THEN 'sd_*'
            WHEN j.order_id LIKE 'cusSubs_%' THEN 'cusSubs_*'
            WHEN j.order_id LIKE 'cxTeam_%' THEN 'cxTeam_*'
            WHEN j.order_id LIKE 'mr_%' THEN 'mr_*'
            ELSE 'other'
        END as pattern,
        j.payment_status,
        COUNT(*) as cnt,
        SUM(j.amount) as total_amt
    FROM juspay_transactions j
    WHERE j.order_id NOT IN (SELECT BOOKING_TXN_ID FROM wiom_booking_transactions)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_primary_revenue)
    AND j.order_id NOT IN (SELECT TXN_ID FROM wiom_net_income)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_topup_income)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_customer_security_deposit)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_ott_transactions)
    GROUP BY pattern, j.payment_status ORDER BY cnt DESC
""").fetchall()
for x in r:
    print(f'  {x[0]:20s} status={str(x[1]):10s} {x[2]:>10,}  amt={x[3]:>12,.0f}')

# Sample orphan custGen records
print('\n--- Sample orphan custGen (SUCCESS, not in any Wiom) ---')
r = con.execute("""
    SELECT j.order_id, j.payment_gateway, j.amount, j.order_date_created, j.payment_status
    FROM juspay_transactions j
    WHERE j.order_id LIKE 'custGen_%'
    AND j.payment_status = 'SUCCESS'
    AND j.order_id NOT IN (SELECT BOOKING_TXN_ID FROM wiom_booking_transactions)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_primary_revenue)
    AND j.order_id NOT IN (SELECT TXN_ID FROM wiom_net_income)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_topup_income)
    LIMIT 10
""").fetchdf()
print(r.to_string())

# Sample orphan w_* records
print('\n--- Sample orphan w_* (SUCCESS, not in any Wiom) ---')
r = con.execute("""
    SELECT j.order_id, j.payment_gateway, j.amount, j.order_date_created, j.payment_status
    FROM juspay_transactions j
    WHERE j.order_id LIKE 'w_%'
    AND j.payment_status = 'SUCCESS'
    AND j.order_id NOT IN (SELECT BOOKING_TXN_ID FROM wiom_booking_transactions)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_primary_revenue)
    AND j.order_id NOT IN (SELECT TXN_ID FROM wiom_net_income)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_topup_income)
    LIMIT 10
""").fetchdf()
print(r.to_string())

# Date distribution of orphans
print('\n--- Orphan Juspay by month ---')
r = con.execute("""
    SELECT
        STRFTIME(j.order_date_created::TIMESTAMP, '%Y-%m') as month,
        COUNT(*) as cnt
    FROM juspay_transactions j
    WHERE j.order_id NOT IN (SELECT BOOKING_TXN_ID FROM wiom_booking_transactions)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_primary_revenue)
    AND j.order_id NOT IN (SELECT TXN_ID FROM wiom_net_income)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_topup_income)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_customer_security_deposit)
    AND j.order_id NOT IN (SELECT TRANSACTION_ID FROM wiom_ott_transactions)
    GROUP BY month ORDER BY month
""").fetchall()
for x in r:
    print(f'  {str(x[0]):10s} {x[1]:>10,}')

print()
print('='*80)
print('STEP 4: STATUS + AMOUNT MISMATCHES ON MATCHED RECORDS')
print('='*80)

# Use wiom_net_income as the broadest Wiom table (647K matched)
print('\n--- net_income vs juspay: status cross-tab ---')
r = con.execute("""SELECT ni.MODE, j.payment_status, COUNT(*) as cnt,
    SUM(ni.AMOUNT) as wiom_amt, SUM(j.amount) as juspay_amt
    FROM wiom_net_income ni
    INNER JOIN juspay_transactions j ON ni.TXN_ID = j.order_id
    GROUP BY ni.MODE, j.payment_status ORDER BY cnt DESC""").fetchall()
for x in r:
    print(f'  mode={str(x[0]):8s} juspay_status={str(x[1]):10s} cnt={x[2]:>10,}  wiom_amt={x[3]:>15,}  juspay_amt={x[4]:>15,.0f}')

# Amount mismatches
print('\n--- net_income vs juspay: amount mismatches ---')
r = con.execute("""SELECT COUNT(*) as total,
    SUM(CASE WHEN ni.AMOUNT != j.amount THEN 1 ELSE 0 END) as mismatches,
    SUM(CASE WHEN ni.AMOUNT = j.amount THEN 1 ELSE 0 END) as matches
    FROM wiom_net_income ni
    INNER JOIN juspay_transactions j ON ni.TXN_ID = j.order_id
    WHERE j.payment_status='SUCCESS'""").fetchdf()
print(r.to_string())

print('\n--- Amount mismatch samples ---')
r = con.execute("""SELECT ni.TXN_ID, ni.AMOUNT as wiom_amt, j.amount as juspay_amt,
    j.amount - ni.AMOUNT as diff, j.payment_gateway
    FROM wiom_net_income ni
    INNER JOIN juspay_transactions j ON ni.TXN_ID = j.order_id
    WHERE j.payment_status='SUCCESS' AND ni.AMOUNT != j.amount
    ORDER BY ABS(j.amount - ni.AMOUNT) DESC
    LIMIT 20""").fetchdf()
print(r.to_string())

# primary_revenue amount comparison
print('\n--- primary_revenue vs juspay: amount mismatches ---')
r = con.execute("""SELECT COUNT(*) as total,
    SUM(CASE WHEN pr.TOTALPAID != j.amount THEN 1 ELSE 0 END) as mismatches,
    SUM(CASE WHEN pr.TOTALPAID = j.amount THEN 1 ELSE 0 END) as matches
    FROM wiom_primary_revenue pr
    INNER JOIN juspay_transactions j ON pr.TRANSACTION_ID = j.order_id
    WHERE j.payment_status='SUCCESS' AND pr.MODE='online'""").fetchdf()
print(r.to_string())

print('\n--- primary_revenue amount mismatch samples ---')
r = con.execute("""SELECT pr.TRANSACTION_ID, pr.TOTALPAID as wiom_amt, j.amount as juspay_amt,
    j.amount - pr.TOTALPAID as diff, j.payment_gateway, pr.PLAN_TYPE
    FROM wiom_primary_revenue pr
    INNER JOIN juspay_transactions j ON pr.TRANSACTION_ID = j.order_id
    WHERE j.payment_status='SUCCESS' AND pr.MODE='online' AND pr.TOTALPAID != j.amount
    ORDER BY ABS(j.amount - pr.TOTALPAID) DESC
    LIMIT 15""").fetchdf()
print(r.to_string())

# booking vs juspay amount
print('\n--- booking vs juspay: amount comparison ---')
r = con.execute("""SELECT COUNT(*) as total,
    SUM(CASE WHEN w.BOOKING_FEE != j.amount THEN 1 ELSE 0 END) as mismatches,
    SUM(CASE WHEN w.BOOKING_FEE = j.amount THEN 1 ELSE 0 END) as matches
    FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE j.payment_status='SUCCESS'""").fetchdf()
print(r.to_string())

con.close()
