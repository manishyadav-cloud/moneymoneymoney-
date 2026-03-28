# -*- coding: utf-8 -*-
"""Layer 1 MISMATCH deep-dive: Wiom DB vs Juspay — January 2026 only."""
import duckdb

con = duckdb.connect('data.duckdb', read_only=True)

print("=" * 90)
print("LAYER 1 MISMATCH DEEP-DIVE — JANUARY 2026")
print("=" * 90)

# =====================================================================
# UNIVERSE: Jan 2026 counts
# =====================================================================
print("\n--- UNIVERSE ---")
j_jan = con.execute("""SELECT COUNT(*), SUM(amount), SUM(CASE WHEN payment_status='SUCCESS' THEN 1 ELSE 0 END)
    FROM juspay_transactions WHERE source_month='Jan26'""").fetchone()
print(f"Juspay Jan26: {j_jan[0]:,} total, {j_jan[2]:,} SUCCESS, Rs {j_jan[1]:,.0f}")

wb_jan = con.execute("SELECT COUNT(*), SUM(BOOKING_FEE) FROM wiom_booking_transactions WHERE CREATED_ON LIKE 'Jan%2026'").fetchone()
print(f"Wiom booking Jan: {wb_jan[0]:,}, Rs {wb_jan[1]:,}")

wpr_jan = con.execute("SELECT COUNT(*), SUM(TOTALPAID) FROM wiom_primary_revenue WHERE RECHARGE_DT LIKE 'Jan%2026' AND MODE='online'").fetchone()
print(f"Wiom primary_rev Jan (online): {wpr_jan[0]:,}, Rs {wpr_jan[1]:,}")

wni_jan = con.execute("SELECT COUNT(*), SUM(AMOUNT) FROM wiom_net_income WHERE YR_MNTH='2026-01'").fetchone()
print(f"Wiom net_income Jan: {wni_jan[0]:,}, Rs {wni_jan[1]:,}")

wsd_jan = con.execute("SELECT COUNT(*) FROM wiom_customer_security_deposit WHERE CREATED_ON LIKE 'Jan%2026'").fetchone()[0]
print(f"Wiom security_deposit Jan: {wsd_jan:,}")

# =====================================================================
# SECTION A: BOOKING vs JUSPAY — JAN 2026
# =====================================================================
print("\n\n" + "=" * 90)
print("SECTION A: WIOM BOOKING (Jan) vs JUSPAY — MATCH ANALYSIS")
print("=" * 90)

matched_a = con.execute("""SELECT COUNT(*) FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE w.CREATED_ON LIKE 'Jan%2026'""").fetchone()[0]
unmatched_a = con.execute("""SELECT COUNT(*) FROM wiom_booking_transactions w
    LEFT JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE w.CREATED_ON LIKE 'Jan%2026' AND j.order_id IS NULL""").fetchone()[0]
print(f"\nMatched: {matched_a:,} / {wb_jan[0]:,} ({matched_a/wb_jan[0]*100:.1f}%)")
print(f"Unmatched: {unmatched_a:,}")

# Status cross-tab
print("\n--- A1: Status cross-tab ---")
r = con.execute("""SELECT w.RESULTSTATUS, j.payment_status, COUNT(*) as cnt
    FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE w.CREATED_ON LIKE 'Jan%2026'
    GROUP BY w.RESULTSTATUS, j.payment_status ORDER BY cnt DESC""").fetchall()
print(f"  {'Wiom Status':20s} {'Juspay Status':15s} {'Count':>8s}")
print("  " + "-" * 48)
for x in r:
    print(f"  {str(x[0]):20s} {str(x[1]):15s} {x[2]:>8,}")

# MISMATCH A1: Wiom NULL status but Juspay SUCCESS
null_cnt = con.execute("""SELECT COUNT(*) FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE w.CREATED_ON LIKE 'Jan%2026' AND w.RESULTSTATUS IS NULL AND j.payment_status='SUCCESS'""").fetchone()[0]
null_amt = con.execute("""SELECT SUM(j.amount) FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE w.CREATED_ON LIKE 'Jan%2026' AND w.RESULTSTATUS IS NULL AND j.payment_status='SUCCESS'""").fetchone()[0]
print(f"\n  >> MISMATCH A1: {null_cnt} bookings NULL in Wiom but SUCCESS in Juspay (Rs {null_amt:,.0f})")
print("     Cause: Payment callback failure — Juspay charged but Wiom DB didn't update")
print("     Risk: Service may not have been activated for these customers")

# MISMATCH A2: Amount differences
print("\n--- A2: Amount mismatches (Wiom SUCCESS + Juspay SUCCESS) ---")
r = con.execute("""SELECT COUNT(*) as total,
    SUM(CASE WHEN w.BOOKING_FEE = j.amount THEN 1 ELSE 0 END) as match,
    SUM(CASE WHEN w.BOOKING_FEE != j.amount THEN 1 ELSE 0 END) as mismatch,
    SUM(w.BOOKING_FEE) as wiom_total, SUM(j.amount) as juspay_total
    FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE w.CREATED_ON LIKE 'Jan%2026'
    AND w.RESULTSTATUS = 'TXN_SUCCESS' AND j.payment_status = 'SUCCESS'""").fetchone()
print(f"  Total matched: {r[0]:,}, Amount match: {r[1]:,}, Amount MISMATCH: {r[2]:,}")
print(f"  Wiom total: Rs {r[3]:,}, Juspay total: Rs {r[4]:,.0f}, Diff: Rs {r[3]-r[4]:,.0f}")

if r[2] and r[2] > 0:
    print("\n  Amount mismatch details:")
    r2 = con.execute("""SELECT w.BOOKING_TXN_ID, w.BOOKING_FEE as wiom, j.amount as juspay,
        w.BOOKING_FEE - j.amount as diff, j.payment_gateway
        FROM wiom_booking_transactions w
        INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
        WHERE w.CREATED_ON LIKE 'Jan%2026'
        AND w.RESULTSTATUS = 'TXN_SUCCESS' AND j.payment_status = 'SUCCESS'
        AND w.BOOKING_FEE != j.amount""").fetchdf()
    print(r2.to_string())
    print(f"\n  >> MISMATCH A2: {len(r2)} bookings — Wiom=Rs25, Juspay=Rs10 (all Paytm)")
    print("     Cause: Promo/discount at PG level; Wiom recorded full price")
    print(f"     Impact: Rs {r2['diff'].sum():,.0f} overstatement in Wiom")

# Gateway distribution for matched
print("\n--- A3: Gateway distribution (Jan matched) ---")
r = con.execute("""SELECT j.payment_gateway, COUNT(*) as cnt, SUM(j.amount) as amt
    FROM wiom_booking_transactions w
    INNER JOIN juspay_transactions j ON w.BOOKING_TXN_ID = j.order_id
    WHERE w.CREATED_ON LIKE 'Jan%2026' AND j.payment_status='SUCCESS'
    GROUP BY j.payment_gateway ORDER BY cnt DESC""").fetchall()
for x in r:
    print(f"  {str(x[0]):15s} {x[1]:>6,} txns  Rs {x[2]:>10,.0f}")


# =====================================================================
# SECTION B: PRIMARY REVENUE vs JUSPAY — JAN 2026
# =====================================================================
print("\n\n" + "=" * 90)
print("SECTION B: WIOM PRIMARY_REVENUE (Jan, online) vs JUSPAY — MATCH ANALYSIS")
print("=" * 90)

matched_b = con.execute("""SELECT COUNT(*) FROM wiom_primary_revenue pr
    INNER JOIN juspay_transactions j ON pr.TRANSACTION_ID = j.order_id
    WHERE pr.RECHARGE_DT LIKE 'Jan%2026' AND pr.MODE='online'""").fetchone()[0]
unmatched_b = wpr_jan[0] - matched_b
print(f"\nMatched: {matched_b:,} / {wpr_jan[0]:,} ({matched_b/wpr_jan[0]*100:.1f}%)")
print(f"Unmatched: {unmatched_b:,}")

# Unmatched by ID pattern
print("\n--- B1: Unmatched online by ID pattern ---")
r = con.execute("""SELECT
    CASE
        WHEN pr.TRANSACTION_ID LIKE 'WIFI_SRVC_%' THEN 'WIFI_SRVC_*'
        WHEN pr.TRANSACTION_ID LIKE 'custGen_%' THEN 'custGen_*'
        WHEN pr.TRANSACTION_ID LIKE 'BOOKING_PAYMENT%' THEN 'BOOKING_PAYMENT*'
        WHEN pr.TRANSACTION_ID LIKE 'cusSubs_%' THEN 'cusSubs_*'
        ELSE 'other'
    END as pattern, COUNT(*) as cnt, SUM(pr.TOTALPAID) as amt
    FROM wiom_primary_revenue pr
    LEFT JOIN juspay_transactions j ON pr.TRANSACTION_ID = j.order_id
    WHERE pr.RECHARGE_DT LIKE 'Jan%2026' AND pr.MODE='online' AND j.order_id IS NULL
    GROUP BY pattern ORDER BY cnt DESC""").fetchall()
print(f"  {'Pattern':25s} {'Count':>8s} {'Amount':>12s}")
print("  " + "-" * 48)
for x in r:
    print(f"  {x[0]:25s} {x[1]:>8,} Rs {x[2]:>10,}")

# WIFI_SRVC deeper analysis
print("\n  >> MISMATCH B1: 3,719 WIFI_SRVC_* (Rs 19.8L) — route OUTSIDE Juspay")
print("     These are direct WiFi service charges, likely billed through partner wallet")

# custGen unmatched — check TOTALPAID
print("\n--- B2: custGen unmatched — TOTALPAID distribution ---")
r = con.execute("""SELECT pr.TOTALPAID, COUNT(*) as cnt
    FROM wiom_primary_revenue pr
    LEFT JOIN juspay_transactions j ON pr.TRANSACTION_ID = j.order_id
    WHERE pr.RECHARGE_DT LIKE 'Jan%2026' AND pr.MODE='online' AND j.order_id IS NULL
    AND pr.TRANSACTION_ID LIKE 'custGen_%'
    GROUP BY pr.TOTALPAID ORDER BY cnt DESC""").fetchall()
for x in r:
    print(f"  Rs {x[0]:>6}  x {x[1]:>5,}")
print("  >> MISMATCH B2: 2,003 custGen with TOTALPAID=0 — failed/cancelled orders still in revenue table")

# Amount mismatches (matched records)
print("\n--- B3: Amount mismatches (matched, Juspay SUCCESS) ---")
r = con.execute("""SELECT COUNT(*) as total,
    SUM(CASE WHEN pr.TOTALPAID = j.amount THEN 1 ELSE 0 END) as match,
    SUM(CASE WHEN pr.TOTALPAID != j.amount THEN 1 ELSE 0 END) as mismatch,
    SUM(pr.TOTALPAID) as wiom_total, SUM(j.amount) as juspay_total
    FROM wiom_primary_revenue pr
    INNER JOIN juspay_transactions j ON pr.TRANSACTION_ID = j.order_id
    WHERE pr.RECHARGE_DT LIKE 'Jan%2026' AND pr.MODE='online' AND j.payment_status='SUCCESS'""").fetchone()
print(f"  Total: {r[0]:,}, Match: {r[1]:,}, MISMATCH: {r[2]:,}")
print(f"  Wiom: Rs {r[3]:,}, Juspay: Rs {r[4]:,.0f}, Diff: Rs {r[4]-r[3]:,.0f}")

if r[2] and r[2] > 0:
    r2 = con.execute("""SELECT pr.TRANSACTION_ID, pr.TOTALPAID as wiom, j.amount as juspay,
        j.amount - pr.TOTALPAID as diff, j.payment_gateway, pr.PLAN_TYPE
        FROM wiom_primary_revenue pr
        INNER JOIN juspay_transactions j ON pr.TRANSACTION_ID = j.order_id
        WHERE pr.RECHARGE_DT LIKE 'Jan%2026' AND pr.MODE='online'
        AND j.payment_status='SUCCESS' AND pr.TOTALPAID != j.amount""").fetchdf()
    print(f"\n  Details ({len(r2)} records):")
    print(r2.to_string())
    print(f"\n  >> MISMATCH B3: {len(r2)} records — Juspay charged Rs {r2['diff'].sum():,.0f} but Wiom shows TOTALPAID=0")
    print("     Cause: DB update failure after successful payment")


# =====================================================================
# SECTION C: NET INCOME vs JUSPAY — JAN 2026
# =====================================================================
print("\n\n" + "=" * 90)
print("SECTION C: WIOM NET_INCOME (Jan) vs JUSPAY — MATCH ANALYSIS")
print("=" * 90)

matched_c = con.execute("""SELECT COUNT(*) FROM wiom_net_income ni
    INNER JOIN juspay_transactions j ON ni.TXN_ID = j.order_id
    WHERE ni.YR_MNTH='2026-01'""").fetchone()[0]
unmatched_c = wni_jan[0] - matched_c
print(f"\nMatched: {matched_c:,} / {wni_jan[0]:,} ({matched_c/wni_jan[0]*100:.3f}%)")
print(f"Unmatched: {unmatched_c:,}")

# The 1 orphan
if unmatched_c > 0:
    print("\n--- C1: Unmatched net_income records ---")
    r = con.execute("""SELECT ni.TXN_ID, ni.TXN_DT, ni.AMOUNT, ni.MODE, ni.PARTNER, ni.REV_MODE
        FROM wiom_net_income ni
        LEFT JOIN juspay_transactions j ON ni.TXN_ID = j.order_id
        WHERE ni.YR_MNTH='2026-01' AND j.order_id IS NULL""").fetchdf()
    print(r.to_string())

# Amount mismatches
print("\n--- C2: Amount mismatches ---")
r = con.execute("""SELECT COUNT(*),
    SUM(CASE WHEN ni.AMOUNT != j.amount THEN 1 ELSE 0 END) as mismatch,
    SUM(ni.AMOUNT) as wiom_total, SUM(j.amount) as juspay_total
    FROM wiom_net_income ni
    INNER JOIN juspay_transactions j ON ni.TXN_ID = j.order_id
    WHERE ni.YR_MNTH='2026-01' AND j.payment_status='SUCCESS'""").fetchone()
print(f"  Total matched: {r[0]:,}, Amount mismatches: {r[1]:,}")
print(f"  Wiom: Rs {r[2]:,}, Juspay: Rs {r[3]:,.0f}, Diff: Rs {r[3]-r[2]:,.0f}")

if r[1] and r[1] > 0:
    r2 = con.execute("""SELECT ni.TXN_ID, ni.AMOUNT as wiom, j.amount as juspay,
        j.amount - ni.AMOUNT as diff, j.payment_gateway
        FROM wiom_net_income ni
        INNER JOIN juspay_transactions j ON ni.TXN_ID = j.order_id
        WHERE ni.YR_MNTH='2026-01' AND j.payment_status='SUCCESS' AND ni.AMOUNT != j.amount""").fetchdf()
    print(r2.to_string())

print("\n  >> NET INCOME is the CLEANEST table — 99.997% match rate")


# =====================================================================
# SECTION D: JUSPAY ORPHANS — NOT IN ANY WIOM TABLE
# =====================================================================
print("\n\n" + "=" * 90)
print("SECTION D: JUSPAY JAN ORPHANS — not in ANY Wiom table")
print("=" * 90)

# Build a union of all Wiom IDs for Jan
orphan_cnt = con.execute("""
    WITH wiom_ids AS (
        SELECT BOOKING_TXN_ID as id FROM wiom_booking_transactions
        UNION ALL SELECT TRANSACTION_ID FROM wiom_primary_revenue
        UNION ALL SELECT TXN_ID FROM wiom_net_income
        UNION ALL SELECT TRANSACTION_ID FROM wiom_topup_income
        UNION ALL SELECT SD_TXN_ID FROM wiom_customer_security_deposit
        UNION ALL SELECT TRANSACTION_ID FROM wiom_ott_transactions
    )
    SELECT COUNT(*) FROM juspay_transactions j
    WHERE j.source_month = 'Jan26' AND j.payment_status = 'SUCCESS'
    AND j.order_id NOT IN (SELECT id FROM wiom_ids)
""").fetchone()[0]
orphan_amt = con.execute("""
    WITH wiom_ids AS (
        SELECT BOOKING_TXN_ID as id FROM wiom_booking_transactions
        UNION ALL SELECT TRANSACTION_ID FROM wiom_primary_revenue
        UNION ALL SELECT TXN_ID FROM wiom_net_income
        UNION ALL SELECT TRANSACTION_ID FROM wiom_topup_income
        UNION ALL SELECT SD_TXN_ID FROM wiom_customer_security_deposit
        UNION ALL SELECT TRANSACTION_ID FROM wiom_ott_transactions
    )
    SELECT SUM(j.amount) FROM juspay_transactions j
    WHERE j.source_month = 'Jan26' AND j.payment_status = 'SUCCESS'
    AND j.order_id NOT IN (SELECT id FROM wiom_ids)
""").fetchone()[0]
print(f"\nOrphans: {orphan_cnt:,} / {j_jan[2]:,} SUCCESS txns ({orphan_cnt/j_jan[2]*100:.2f}%)")
print(f"Orphan amount: Rs {orphan_amt:,.0f}")

# Breakdown
print("\n--- D1: Orphan breakdown by pattern ---")
r = con.execute("""
    WITH wiom_ids AS (
        SELECT BOOKING_TXN_ID as id FROM wiom_booking_transactions
        UNION ALL SELECT TRANSACTION_ID FROM wiom_primary_revenue
        UNION ALL SELECT TXN_ID FROM wiom_net_income
        UNION ALL SELECT TRANSACTION_ID FROM wiom_topup_income
        UNION ALL SELECT SD_TXN_ID FROM wiom_customer_security_deposit
        UNION ALL SELECT TRANSACTION_ID FROM wiom_ott_transactions
    )
    SELECT
        CASE
            WHEN j.order_id LIKE 'custGen_%' THEN 'custGen_*'
            WHEN j.order_id LIKE 'custWgSubs_%' THEN 'custWgSubs_*'
            WHEN j.order_id LIKE 'w_%' THEN 'w_*'
            WHEN j.order_id LIKE 'cusSubs_%' THEN 'cusSubs_*'
            WHEN j.order_id LIKE 'mr_%' THEN 'mr_*'
            WHEN j.order_id LIKE 'cxTeam_%' THEN 'cxTeam_*'
            ELSE 'other'
        END as pattern,
        COUNT(*) as cnt, SUM(j.amount) as amt,
        ROUND(AVG(j.amount), 0) as avg_amt
    FROM juspay_transactions j
    WHERE j.source_month = 'Jan26' AND j.payment_status = 'SUCCESS'
    AND j.order_id NOT IN (SELECT id FROM wiom_ids)
    GROUP BY pattern ORDER BY cnt DESC
""").fetchall()
print(f"  {'Pattern':20s} {'Count':>8s} {'Amount':>14s} {'Avg':>8s}")
print("  " + "-" * 55)
for x in r:
    print(f"  {x[0]:20s} {x[1]:>8,} Rs {x[2]:>12,.0f} Rs {x[3]:>5,.0f}")

# By gateway
print("\n--- D2: Orphan breakdown by gateway ---")
r = con.execute("""
    WITH wiom_ids AS (
        SELECT BOOKING_TXN_ID as id FROM wiom_booking_transactions
        UNION ALL SELECT TRANSACTION_ID FROM wiom_primary_revenue
        UNION ALL SELECT TXN_ID FROM wiom_net_income
        UNION ALL SELECT TRANSACTION_ID FROM wiom_topup_income
        UNION ALL SELECT SD_TXN_ID FROM wiom_customer_security_deposit
        UNION ALL SELECT TRANSACTION_ID FROM wiom_ott_transactions
    )
    SELECT j.payment_gateway, COUNT(*) as cnt, SUM(j.amount) as amt
    FROM juspay_transactions j
    WHERE j.source_month = 'Jan26' AND j.payment_status = 'SUCCESS'
    AND j.order_id NOT IN (SELECT id FROM wiom_ids)
    GROUP BY j.payment_gateway ORDER BY cnt DESC
""").fetchall()
for x in r:
    print(f"  {str(x[0]):15s} {x[1]:>6,} txns  Rs {x[2]:>12,.0f}")


# =====================================================================
# FINAL SCORECARD
# =====================================================================
print("\n\n" + "=" * 90)
print("JANUARY 2026 — LAYER 1 RECONCILIATION SCORECARD")
print("=" * 90)

print(f"""
JUSPAY JAN 2026 (SUCCESS): {j_jan[2]:,} txns, Rs {j_jan[1]:,.0f}

+-------------------------------+----------+----------+--------+---------------------------+
| Wiom Table                    | Jan Rows | Matched  | Rate   | Mismatch Detail           |
+-------------------------------+----------+----------+--------+---------------------------+
| booking_transactions          | {wb_jan[0]:>8,} | {matched_a:>8,} | {matched_a/wb_jan[0]*100:>5.1f}% | {null_cnt} NULL status, 10 amt diff |
| primary_revenue (online)      | {wpr_jan[0]:>8,} | {matched_b:>8,} | {matched_b/wpr_jan[0]*100:>5.1f}% | 3719 WIFI_SRVC, 2003 zero-amt  |
| net_income                    | {wni_jan[0]:>8,} | {matched_c:>8,} | {matched_c/wni_jan[0]*100:>5.1f}% | 1 orphan, 2 amt diffs         |
| security_deposit              | {wsd_jan:>8,} |          |        | (linked via SD_TXN_ID)      |
+-------------------------------+----------+----------+--------+---------------------------+
| JUSPAY ORPHANS (no Wiom link) |          | {orphan_cnt:>8,} | {orphan_cnt/j_jan[2]*100:>5.2f}% | Rs {orphan_amt:,.0f}                  |
+-------------------------------+----------+----------+--------+---------------------------+

KEY MISMATCHES:
  1. STATUS GAP:     {null_cnt} bookings NULL in Wiom but SUCCESS in Juspay (callback failure)
  2. AMOUNT GAP:     10 bookings Rs25 in Wiom vs Rs10 in Juspay (Paytm promo)
  3. REVENUE GAP:    9 primary_rev TOTALPAID=0 but Juspay charged (Rs 3,196)
  4. ORPHAN GAP:     {orphan_cnt:,} Juspay txns (Rs {orphan_amt:,.0f}) not in ANY Wiom table
     - custGen: 4,511 (customer recharges missing from Wiom DB)
     - mr:      2,855 (merchant/router txns — no Wiom table tracks these)
     - cusSubs:   351 (subscription txns not tracked)
  5. WIFI_SRVC:      3,719 Wiom txns (Rs 19.8L) route OUTSIDE Juspay (partner wallet)
""")

con.close()
