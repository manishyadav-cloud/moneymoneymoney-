# -*- coding: utf-8 -*-
"""Wiom <-> Juspay reconciliation deep analysis."""
import duckdb

con = duckdb.connect('data.duckdb', read_only=True)

print("=== LAYER 1: WIOM BOOKING vs JUSPAY TRANSACTIONS ===\n")

w_total = con.execute("SELECT COUNT(*) FROM wiom_booking_transactions").fetchone()[0]
j_total = con.execute("SELECT COUNT(*) FROM juspay_transactions").fetchone()[0]
print(f"wiom_booking_transactions: {w_total:,}")
print(f"juspay_transactions: {j_total:,}")

matched = con.execute("""SELECT COUNT(*) FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id""").fetchone()[0]
print(f"\nMatched (BOOKING_TXN_ID = order_id): {matched:,}")

w_only = con.execute("""SELECT COUNT(*) FROM wiom_booking_transactions w
    LEFT JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE j.order_id IS NULL""").fetchone()[0]
print(f"Wiom only (not in Juspay): {w_only:,}")

j_only = j_total - matched
print(f"Juspay only (not in Wiom booking): {j_only:,}")

# Unmatched Wiom detail
print(f"\n--- Unmatched Wiom records ({w_only}) by status/gateway ---")
r = con.execute("""SELECT w.RESULTSTATUS, w.PAYMENT_GATEWAY_NAME, COUNT(*) as cnt
    FROM wiom_booking_transactions w
    LEFT JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE j.order_id IS NULL
    GROUP BY w.RESULTSTATUS, w.PAYMENT_GATEWAY_NAME ORDER BY cnt DESC""").fetchall()
for x in r:
    print(f"  {str(x[0]):20s} {str(x[1]):20s} {x[2]:>6,}")

# Sample unmatched
print("\n--- Sample unmatched Wiom records ---")
r = con.execute("""SELECT w.BOOKING_TXN_ID, w.RESULTSTATUS, w.PAYMENT_GATEWAY_NAME, w.CREATED_ON
    FROM wiom_booking_transactions w
    LEFT JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE j.order_id IS NULL LIMIT 10""").fetchdf()
print(r.to_string())

# Status cross-tab (matched)
print("\n\n--- Status cross-tab (matched records) ---")
r = con.execute("""SELECT w.RESULTSTATUS as wiom_status, j.payment_status as juspay_status, COUNT(*) as cnt
    FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    GROUP BY w.RESULTSTATUS, j.payment_status ORDER BY cnt DESC""").fetchall()
for x in r:
    print(f"  wiom={str(x[0]):20s} juspay={str(x[1]):25s} {x[2]:>8,}")

# Amount comparison (SUCCESS/CHARGED)
print("\n--- Amount comparison (Wiom SUCCESS + Juspay CHARGED) ---")
r = con.execute("""SELECT
    COUNT(*) as cnt,
    SUM(w.BOOKING_FEE) as wiom_total,
    SUM(j.amount) as juspay_total,
    SUM(w.BOOKING_FEE) - SUM(j.amount) as diff,
    SUM(CASE WHEN w.BOOKING_FEE != j.amount THEN 1 ELSE 0 END) as amount_mismatches
    FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE w.RESULTSTATUS = 'TXN_SUCCESS' AND j.payment_status = 'CHARGED'
""").fetchdf()
print(r.to_string())

# Amount mismatch samples
print("\n--- Amount mismatch samples (fee != amount) ---")
r = con.execute("""SELECT w.BOOKING_TXN_ID, w.BOOKING_FEE, j.amount, j.amount - w.BOOKING_FEE as diff
    FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE w.RESULTSTATUS = 'TXN_SUCCESS' AND j.payment_status = 'CHARGED'
    AND w.BOOKING_FEE != j.amount
    LIMIT 20""").fetchdf()
if len(r) > 0:
    print(r.to_string())
else:
    print("  No amount mismatches found!")

# Status mismatches
print("\n--- Wiom=SUCCESS but Juspay != CHARGED ---")
r = con.execute("""SELECT j.payment_status, COUNT(*) as cnt
    FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE w.RESULTSTATUS = 'TXN_SUCCESS' AND j.payment_status != 'CHARGED'
    GROUP BY j.payment_status ORDER BY cnt DESC""").fetchall()
if r:
    for x in r: print(f"  juspay={str(x[0]):25s} {x[1]:>6,}")
else:
    print("  None! All SUCCESS = CHARGED")

print("\n--- Juspay=CHARGED but Wiom != SUCCESS ---")
r = con.execute("""SELECT w.RESULTSTATUS, COUNT(*) as cnt
    FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE j.payment_status = 'CHARGED' AND w.RESULTSTATUS != 'TXN_SUCCESS'
    GROUP BY w.RESULTSTATUS ORDER BY cnt DESC""").fetchall()
if r:
    for x in r: print(f"  wiom={str(x[0]):25s} {x[1]:>6,}")
else:
    print("  None! All CHARGED = SUCCESS")

# === REFUNDS ===
print("\n\n=== REFUND RECONCILIATION ===\n")
jr_total = con.execute("SELECT COUNT(*) FROM juspay_refunds").fetchone()[0]
print(f"juspay_refunds total: {jr_total:,}")

jr_matched = con.execute("""SELECT COUNT(*) FROM juspay_refunds jr
    INNER JOIN wiom_booking_transactions w ON jr.order_id = w.BOOKING_TXN_ID""").fetchone()[0]
print(f"Matched to wiom booking: {jr_matched:,}")
print(f"NOT in wiom booking: {jr_total - jr_matched:,}")

print("\n--- Juspay refunds by gateway ---")
r = con.execute("""SELECT payment_gateway, COUNT(*) as cnt,
    SUM(refund_amount) as total_refund,
    SUM(CASE WHEN refund_status='SUCCESS' THEN 1 ELSE 0 END) as success_cnt,
    SUM(CASE WHEN refund_status='SUCCESS' THEN refund_amount ELSE 0 END) as success_amt
    FROM juspay_refunds
    GROUP BY payment_gateway ORDER BY cnt DESC""").fetchall()
for x in r:
    print(f"  {str(x[0]):20s} {x[1]:>8,} txns  refund_total={x[2]:>12,.0f}  success={x[3]:>6,} / {x[4]:>12,.0f}")

# === SECURITY DEPOSIT ===
print("\n\n=== SECURITY DEPOSIT LINKING ===")
sd_total = con.execute("SELECT COUNT(*) FROM wiom_customer_security_deposit").fetchone()[0]

sd_j1 = con.execute("""SELECT COUNT(*) FROM wiom_customer_security_deposit sd
    INNER JOIN juspay_transactions j ON sd.PAYMENT_GATEWAY_TXN_ID = j.juspay_txn_id""").fetchone()[0]
sd_j2 = con.execute("""SELECT COUNT(*) FROM wiom_customer_security_deposit sd
    INNER JOIN juspay_transactions j ON sd.SD_TXN_ID = j.order_id""").fetchone()[0]
print(f"Total: {sd_total:,}")
print(f"  SD PG_TXN_ID = juspay.juspay_txn_id: {sd_j1:,}")
print(f"  SD SD_TXN_ID = juspay.order_id: {sd_j2:,}")

# === PRIMARY REVENUE ===
print("\n\n=== PRIMARY REVENUE LINKING ===")
pr_online = con.execute("SELECT COUNT(*) FROM wiom_primary_revenue WHERE MODE='online'").fetchone()[0]
pr_match_j = con.execute("""SELECT COUNT(*) FROM wiom_primary_revenue pr
    INNER JOIN juspay_transactions j ON pr.TRANSACTION_ID = j.order_id
    WHERE pr.MODE='online'""").fetchone()[0]
pr_match_b = con.execute("""SELECT COUNT(*) FROM wiom_primary_revenue pr
    INNER JOIN wiom_booking_transactions w ON pr.TRANSACTION_ID = w.BOOKING_TXN_ID
    WHERE pr.MODE='online'""").fetchone()[0]
print(f"Online transactions: {pr_online:,}")
print(f"  Matched to juspay (TRANSACTION_ID = order_id): {pr_match_j:,}")
print(f"  Matched to booking (TRANSACTION_ID = BOOKING_TXN_ID): {pr_match_b:,}")

# ID pattern distribution
print("\n--- primary_revenue TRANSACTION_ID patterns (online) ---")
r = con.execute("""SELECT
    CASE
        WHEN TRANSACTION_ID LIKE 'BILL_PAID_%' THEN 'BILL_PAID_*'
        WHEN TRANSACTION_ID LIKE 'custGen_%' THEN 'custGen_*'
        WHEN TRANSACTION_ID LIKE 'custWgSubs_%' THEN 'custWgSubs_*'
        WHEN TRANSACTION_ID LIKE 'w_%' THEN 'w_*'
        ELSE 'other'
    END as pattern, COUNT(*) as cnt
    FROM wiom_primary_revenue WHERE MODE='online'
    GROUP BY pattern ORDER BY cnt DESC""").fetchall()
for x in r:
    print(f"  {x[0]:30s} {x[1]:>10,}")

# Juspay order_id pattern distribution (for comparison)
print("\n--- juspay order_id patterns ---")
r = con.execute("""SELECT
    CASE
        WHEN order_id LIKE 'custGen_%' THEN 'custGen_*'
        WHEN order_id LIKE 'custWgSubs_%' THEN 'custWgSubs_*'
        WHEN order_id LIKE 'w_%' THEN 'w_*'
        WHEN order_id LIKE 'sd_%' THEN 'sd_*'
        ELSE 'other'
    END as pattern, COUNT(*) as cnt
    FROM juspay_transactions
    GROUP BY pattern ORDER BY cnt DESC""").fetchall()
for x in r:
    print(f"  {x[0]:30s} {x[1]:>10,}")

# What does Juspay have that wiom_booking doesn't? (the 1M+ gap)
print("\n\n=== WHY JUSPAY HAS 1M+ MORE THAN WIOM BOOKING (20K)? ===")
print("--- Juspay payment_status distribution ---")
r = con.execute("""SELECT payment_status, COUNT(*) as cnt
    FROM juspay_transactions
    GROUP BY payment_status ORDER BY cnt DESC""").fetchall()
for x in r:
    print(f"  {str(x[0]):30s} {x[1]:>10,}")

print("\n--- Juspay order_status distribution ---")
r = con.execute("""SELECT order_status, COUNT(*) as cnt
    FROM juspay_transactions
    GROUP BY order_status ORDER BY cnt DESC""").fetchall()
for x in r:
    print(f"  {str(x[0]):30s} {x[1]:>10,}")

con.close()
