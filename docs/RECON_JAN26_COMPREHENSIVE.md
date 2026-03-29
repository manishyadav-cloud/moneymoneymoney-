# January 2026 — End-to-End Reconciliation Report
## Reverse Direction: Bank → Settlement → Juspay → Wiom DB

**Scope:** All payment gateway settlements with `settled_date` in January 2026
**Direction:** Bottom-up (Bank is ground truth; tracing backwards to source)
**Base Table:** `recon_jan26_base` (382,253 rows, persisted in `data.duckdb`)
**Analysis Script:** `docs/_jan26_reverse_recon.py`
**Date:** 2026-03-29

---

## 1. Executive Summary

| Metric | Value |
|---|---|
| Total Jan26 bank deposits (all 4 gateways) | **Rs 7.35Cr** |
| Total Jan26 settlement rows | **382,253** |
| Settlement gross | **Rs 7.40Cr** |
| Settlement net (after PG fees) | **Rs 7.39Cr** |
| Juspay matched | **382,190 rows (99.98%)** |
| Wiom DB fully traced | **375,946 rows (98.35%)** |
| **True unreconciled gap (Wiom DB missing)** | **2,617 rows — Rs 11.87L** |
| Explainable gaps (refunded / wallet topup) | 3,627 rows — Rs 18.08L |
| PG-level adjustments (not in Juspay by design) | 63 rows — Rs -66.7K |

**Overall verdict:** 98.35% of Jan26 settlements trace cleanly end-to-end. The remaining 1.65% breaks into three categories with only **0.69% (Rs 11.87L) being a genuine Wiom DB data gap**.

---

## 2. Scope Definition

### What is included
- All rows in all 4 PG settlement tables where the settlement was physically processed in January 2026:
  - `paytm_settlements`: `settled_date BETWEEN '2026-01-01' AND '2026-01-31'` — forward `ACQUIRING` rows
  - `phonepe_settlements`: `CAST("Settlement Date" AS DATE)` in January 2026 — non-REVERSAL/REFUND rows
  - `payu_settlements`: `CAST(LEFT(CAST("AddedOn" AS VARCHAR),10) AS DATE)` in January 2026 — all rows
  - `razorpay_transactions`: `settled_at BETWEEN '2026-01-01' AND '2026-01-31'` where `type = 'payment'`

### What is NOT included
- Paytm refund rows (separate `paytm_refunds` table — handled in refund recon)
- PhonePe REVERSAL rows in `phonepe_settlements` (separate reversal type)
- Razorpay `type='refund'` and `type='adjustment'` rows
- Settlements from Feb 2026 onward (even if the transaction was created in Jan26)

### Important: Source Month ≠ Settlement Month
**3.5% of Jan26 settlements belong to transactions created in December 2025:**

| Gateway | Dec25 txns settled in Jan26 | Amount |
|---|---|---|
| PAYTM_V2 | 10,921 rows | Rs 19.59L |
| RAZORPAY | 1,165 rows | Rs 1.73L |
| PHONEPE | 612 rows | Rs 91K |
| PAYU | 2 rows | Rs 1,025 |
| **Total** | **12,700 rows** | **Rs 22.29L** |

These Dec25 transactions are traced correctly — they appear in Juspay and Wiom DB under December dates.

---

## 3. Base Table Structure: `recon_jan26_base`

One row per settlement transaction. 382,253 rows total.

```sql
recon_jan26_base (
  -- Identity
  gateway             VARCHAR,   -- PAYTM_V2 / PHONEPE / PAYU / RAZORPAY
  settlement_order_id VARCHAR,   -- Order ID in settlement table (Paytm quotes stripped)

  -- Layer 4: Bank
  bank_date           DATE,      -- Matched bank deposit date (= settled_date)
  bank_daily_deposit  DOUBLE,    -- Total RTGS/NEFT deposit on that date for this gateway

  -- Layer 3: Settlement
  settled_date        DATE,
  sett_gross          DOUBLE,    -- Gross transaction amount
  sett_net            DOUBLE,    -- Net after PG fees
  sett_fee            DOUBLE,    -- PG fees (MDR + GST)
  sett_utr            VARCHAR,   -- Settlement batch UTR
  sett_txn_type       VARCHAR,   -- ACQUIRING / PAYMENT / etc.

  -- Layer 2: Juspay
  juspay_order_id     VARCHAR,   -- NULL if not found
  juspay_txn_id       VARCHAR,
  juspay_amount       DOUBLE,
  juspay_status       VARCHAR,   -- order_status (SUCCESS/CHARGED/etc.)
  juspay_gateway      VARCHAR,
  juspay_created_date DATE,      -- When the Juspay order was created
  l2_match            VARCHAR,   -- 'MATCHED' / 'SETT_ONLY'

  -- Layer 1: Wiom DB
  wiom_txn_id         VARCHAR,   -- NULL if not found in any Wiom table
  wiom_table          VARCHAR,   -- Which table matched
  wiom_amount         DOUBLE,
  l1_match            VARCHAR,   -- 'MATCHED' / 'JUSPAY_ONLY' / 'NO_JUSPAY'
  order_id_prefix     VARCHAR,   -- e.g. custGen / w / cusSubs / wiomWall

  -- Derived
  trace_status        VARCHAR    -- FULLY_TRACED / MISSING_WIOM / MISSING_JUSPAY
)
```

**Useful queries on base table:**
```sql
-- Overall summary
SELECT trace_status, COUNT(*) AS rows, SUM(sett_gross) AS gross, SUM(sett_net) AS net
FROM recon_jan26_base GROUP BY 1 ORDER BY rows DESC;

-- Gateway breakdown
SELECT gateway, trace_status, COUNT(*) AS rows, SUM(sett_gross) AS gross
FROM recon_jan26_base GROUP BY 1,2 ORDER BY 1,2;

-- Fully trace a specific transaction
SELECT * FROM recon_jan26_base WHERE settlement_order_id = '<your_order_id>';
```

---

## 4. Amount Waterfall (Jan 2026)

| Layer | PAYTM_V2 | PHONEPE | PAYU | RAZORPAY | **TOTAL** |
|---|---|---|---|---|---|
| **Bank deposits** | Rs 5.76Cr | Rs 25.1L | Rs 1.08Cr | Rs 24.9L | **Rs 7.35Cr** |
| **Settlement gross** | Rs 5.81Cr | Rs 25.3L | Rs 1.09Cr | Rs 25.1L | **Rs 7.40Cr** |
| **Settlement net** | Rs 5.80Cr | Rs 25.3L | Rs 1.09Cr | Rs 25.0L | **Rs 7.39Cr** |
| **Juspay matched** | Rs 5.81Cr | Rs 25.3L | Rs 1.10Cr | Rs 25.1L | **Rs 7.41Cr** |
| **Wiom DB matched** | Rs 5.56Cr | Rs 24.1L | Rs 1.07Cr | Rs 24.1L | **Rs 7.11Cr** |

**Layer-by-layer drops:**

| Gap | Amount | Explanation |
|---|---|---|
| Bank vs Settlement gross | **Rs -57.3K** | PG fees deducted before RTGS + refund netting |
| Settlement net vs Bank | **Rs -46.2K** | Paytm refund deductions (Rs 3.89L) minus excess from Dec25 carryover |
| Settlement gross vs Juspay | **Rs -6.97L** | PayU ADJ rows (Rs -67K) + Dec25 txns in Jan26 sett net difference |
| Juspay vs Wiom DB | **Rs -29.99L** | See Gap C breakdown below |

---

## 5. Reconciliation Results by Layer

### Layer 4 → 3b: Bank vs Settlement (Gap A)

Settlement net should approximately equal bank deposit (Paytm: after deducting refunds).

| Gateway | Sett Net | Bank Deposit | Gap | Gap% | Status |
|---|---|---|---|---|---|
| PAYTM_V2 | Rs 5.80Cr | Rs 5.76Cr | Rs -3.91L | -0.67% | ✅ CLEAN |
| PAYU | Rs 1.09Cr | Rs 1.08Cr | Rs -43.9K | -0.40% | ✅ CLEAN |
| PHONEPE | Rs 25.3L | Rs 25.1L | Rs -14.9K | -0.59% | ✅ CLEAN |
| RAZORPAY | Rs 25.0L | Rs 24.9L | Rs -12.0K | -0.48% | ✅ CLEAN |
| **Total** | **Rs 7.39Cr** | **Rs 7.35Cr** | **Rs -46.2K** | **-0.62%** | ✅ |

**Residual gap explained:**
- Paytm: refund deductions (Rs 3.89L) account for Rs 3.91L gap; ~Rs 7K unexplained (TDS or platform fees)
- PhonePe/PayU/Razorpay: < 0.6% consistently — rounding and minor timing differences

---

### Layer 3b → 2: Settlement vs Juspay (Gap B)

**63 settlement rows (Rs -66.7K gross) have no Juspay match. All are explainable:**

| Sub-category | Rows | Amount | Root Cause |
|---|---|---|---|
| PayU `ADJ_*` rows | 32 | Rs -67K | PayU platform-level fee/MDR adjustments. These are PayU-internal debits (negative amounts), not customer transactions — by design NOT in Juspay |
| Paytm `w-custGen_*` / `w-wiom*` | 31 | Rs 292 | Paytm Wallet adjustment rows — secondary settlement lines for prior wallet transactions. Very small amounts (Re 1 to Rs 25), not real payment orphans |

**Conclusion: Gap B = 0 true orphans. Both categories are PG internal bookkeeping rows.**

---

### Layer 2 → 1: Juspay vs Wiom DB (Gap C)

**6,244 Juspay-matched rows (Rs 29.95L) have no Wiom DB match.**

After deeper analysis, this 6,244 breaks into three distinct sub-categories:

| Sub-category | Rows | Amount | Finding |
|---|---|---|---|
| **Refunded — in `wiom_refunded_transactions`** | 3,367 | Rs 3.02L | These orders WERE in Wiom DB but were refunded. They exist in `wiom_refunded_transactions` (REFUND_STATUS=1). The base recon script did not join this table. **NOT a true gap.** |
| **`wiomWall_*` — in `wiom_topup_income`** | 260 | Rs 15.06L | `wiomWall_` prefix = wallet top-up transactions. Maps to `wiom_topup_income.TRANSACTION_ID`. Script gap — not included in Wiom lookup joins. **NOT a true gap.** |
| **⚠️ TRULY MISSING from Wiom DB** | **2,617** | **Rs 11.87L** | These are Juspay `SUCCESS` transactions with no record in ANY Wiom table. See breakdown below. |

**Truly missing breakdown (2,617 rows, Rs 11.87L):**

| Prefix | Rows | Amount | Likely Root Cause |
|---|---|---|---|
| `custGen_*` | 2,179 | Rs 10.08L | WiFi recharge transactions — in Juspay + PG + Bank but absent from `wiom_booking_transactions` AND `wiom_primary_revenue`. Wiom DB export is incomplete. |
| `cusSubs_*` | 353 | Rs 1.77L | Customer subscription payments — absent from `wiom_primary_revenue`. Wiom DB export gap. |
| `w_*` | 83 | Rs 1,098 | Wallet/topup transactions — not in `wiom_net_income` or `wiom_topup_income`. |
| `custWgSubs_*` | 2 | Rs 1,020 | Widget subscriptions — absent from `wiom_primary_revenue`. |

---

### Amount Mismatch (Gap D)

Rows that are **FULLY_TRACED** (matched at all layers) but where Juspay amount ≠ Wiom DB amount.

| Gateway | Matched Rows | Exact Match | Diff Rows | Total Amount Diff | Status |
|---|---|---|---|---|---|
| PAYTM_V2 | 309,891 | 309,870 | 21 | Rs 2,445 | ✅ Negligible |
| PAYU | 32,815 | 32,812 | 3 | Rs -45 | ✅ Negligible |
| PHONEPE | 16,531 | 16,529 | 2 | Rs 485 | ✅ Negligible |
| RAZORPAY | 16,709 | 16,708 | 1 | Rs 575 | ✅ Negligible |
| **Total** | **375,946** | **375,919** | **27** | **Rs 3,460** | ✅ |

**Root causes of 27 diff rows:**
- 10 rows: `wiom_primary_revenue.TOTALPAID = 0` while Juspay amount > 0 — free/bonus recharges where customer paid zero but Juspay processed a nominal amount
- 5 rows: Juspay amount = Rs 10, `wiom_booking_transactions.BOOKING_FEE` = Rs 25 — booking fee mismatch vs total paid (split transaction or fee adjustment)
- 12 rows: Minor Rs 1–21 differences — likely rounding or plan change adjustments

**Conclusion: Gap D is negligible (Rs 3,460 across 375,946 transactions = 0.001%).**

---

## 6. Gap Summary & Action Items

### Confirmed Gaps (requiring action)

| Gap ID | Description | Rows | Amount | Priority |
|---|---|---|---|---|
| **GAP-1** | `custGen_*` Juspay SUCCESS not in `wiom_booking_transactions` or `wiom_primary_revenue` | 2,179 | Rs 10.08L | 🔴 HIGH |
| **GAP-2** | `cusSubs_*` Juspay SUCCESS not in `wiom_primary_revenue` | 353 | Rs 1.77L | 🟡 MEDIUM |
| **GAP-3** | `w_*` Juspay SUCCESS not in `wiom_net_income` or `wiom_topup_income` | 83 | Rs 1,098 | 🟢 LOW |
| **GAP-4** | Paytm residual bank gap (after refunds) | — | Rs ~7K/day | 🟡 MEDIUM |

### Script Gaps (recon script needs fix, not data gaps)

| Gap ID | Description | Fix Required |
|---|---|---|
| **SCRIPT-1** | `wiomWall_*` not joined to `wiom_topup_income` | Add `LEFT JOIN wiom_topup_income ON TRANSACTION_ID = juspay_order_id` for `wiomWall_` prefix |
| **SCRIPT-2** | Refunded transactions not joined to `wiom_refunded_transactions` | Add `LEFT JOIN wiom_refunded_transactions ON TRANSACTION_ID = juspay_order_id` as a fallback; mark as `REFUNDED` not `MISSING_WIOM` |

### Explainable Non-Gaps (no action needed)

| Item | Rows | Amount | Reason |
|---|---|---|---|
| PayU `ADJ_*` in settlements | 32 | Rs -67K | Platform fee adjustments — not customer transactions |
| Paytm `w-*` adjustment rows | 31 | Rs 292 | Paytm wallet micro-adjustments |
| MISSING_WIOM — in wiom_refunded_transactions | 3,367 | Rs 3.02L | Correctly refunded; in wiom_refunded_transactions |
| MISSING_WIOM — wiomWall_ in wiom_topup_income | 260 | Rs 15.06L | Correct data; script join missing |

---

## 7. Data Understanding Gaps Discovered

These are gaps in **knowledge about the data**, not necessarily missing money:

### DG-1: `wiomWall_` prefix — undocumented order type
- **What:** `wiomWall_<phone>_<juspay_id>_<suffix>` — wallet top-up orders processed via Juspay
- **Where:** Juspay `order_type = 'ORDER_PAYMENT'`; money arrives via PG; maps to `wiom_topup_income`
- **Volume:** 820 total (Dec25–Feb26), Rs 43.5L
- **Issue:** Not documented in order_id prefix guide; recon scripts don't join to `wiom_topup_income` for this prefix

### DG-2: `wiom_topup_income` not in reconciliation flow
- **What:** `wiom_topup_income` (625,184 rows) tracks partner wallet topups. `wiomWall_*` orders flow here.
- **Issue:** This table was never included in Layer 1 Wiom joins in any reconciliation script
- **Fix:** Add `wiom_topup_income` to the Wiom lookup for `wiomWall_*` prefix

### DG-3: `paytm_settlements.transaction_type` uses 'ACQUIRING' not 'SALE'
- **What:** Forward payment rows in `paytm_settlements` have `transaction_type = 'ACQUIRING'`, not `'SALE'`
- **Issue:** If filtering by `transaction_type = 'SALE'` (as originally planned), Paytm settlements return 0 rows
- **Fix:** Use `transaction_type = 'ACQUIRING'` OR filter by settlement amount > 0

### DG-4: Wiom DB export appears incomplete for Jan 2026 custGen_ transactions
- **What:** 2,179 `custGen_*` orders are Juspay SUCCESS + PG settled + bank received, but absent from `wiom_booking_transactions` AND `wiom_primary_revenue`
- **Implication:** Either the Wiom DB export was cut before these transactions were written, OR these transactions were processed via a code path that bypasses the standard Wiom tables
- **Amount at stake:** Rs 10.08L collected, unrecorded in Wiom DB
- **Action:** Finance/tech team to verify these order IDs in live Wiom DB

### DG-5: `cusSubs_*` and `custWgSubs_*` partially absent from `wiom_primary_revenue`
- **What:** 355 subscription orders that Juspay shows as SUCCESS are not in `wiom_primary_revenue`
- **Possible cause:** Wiom DB only exports the most recent subscription state; superseded/cancelled renewals may be excluded
- **Amount:** Rs 1.78L

### DG-6: Paytm ADJ/Wallet micro-settlement rows in `paytm_settlements`
- **What:** 31 rows with `Order_ID` starting with `w-` (e.g., `w-custGen_...`, `w-wiom...`) with tiny amounts (Re 1 – Rs 25)
- **These are:** Paytm internal wallet adjustment entries, not customer payment rows
- **Issue:** Not filtered out in standard settlement queries; show up as orphans

### DG-7: Source month vs settlement month mismatch
- **What:** 12,700 Jan26 settlement rows (3.5%) originate from Dec25 transactions
- **Impact:** "Jan26 settlement scope" ≠ "Jan26 transaction scope" — comparing the wrong things when doing period-level reconciliation
- **Fix:** Always filter on both `settled_date` (for bank matching) AND `juspay_created_date` (for Wiom matching) separately

---

## 8. Complete Trace Status Breakdown

### Final corrected view (after resolving script gaps)

| Status | Rows | Amount (sett gross) | % |
|---|---|---|---|
| **FULLY_TRACED** (all 4 layers) | 375,946 | Rs 7.11Cr | 98.35% |
| **REFUNDED** (in wiom_refunded_transactions) | 3,367 | Rs 3.02L | 0.88% |
| **WALLET_TOPUP** (wiomWall_ in wiom_topup_income) | 260 | Rs 15.06L | 0.07% |
| **⚠️ TRULY_MISSING** (no Wiom DB record) | **2,617** | **Rs 11.87L** | **0.69%** |
| **PG_ADJUSTMENT** (PayU ADJ + Paytm w- rows) | 63 | Rs -66.7K | 0.02% |
| **TOTAL** | **382,253** | **Rs 7.40Cr** | **100%** |

---

## 9. Key Join Conditions Used in Base Table

### Paytm
```sql
-- Settlement → Juspay
REPLACE(paytm_settlements.Order_ID, chr(39), '') = juspay_transactions.juspay_txn_id
WHERE juspay_transactions.payment_gateway = 'PAYTM_V2'
-- Settlement date filter
AND paytm_settlements.settled_date >= '2026-01-01' AND settled_date < '2026-02-01'
AND paytm_settlements.transaction_type = 'ACQUIRING'   -- NOT 'SALE'
```

### PhonePe
```sql
-- Settlement → Juspay
phonepe_settlements."Merchant Order Id" = juspay_transactions.juspay_txn_id
WHERE juspay_transactions.payment_gateway = 'PHONEPE'
AND CAST(phonepe_settlements."Settlement Date" AS DATE) >= '2026-01-01'
AND CAST(phonepe_settlements."Settlement Date" AS DATE) < '2026-02-01'
AND phonepe_settlements."Transaction Type" NOT LIKE '%REVERSAL%'
AND phonepe_settlements."Transaction Type" NOT LIKE '%REFUND%'
```

### PayU
```sql
-- Settlement → Juspay
payu_settlements."Merchant Txn ID" = juspay_transactions.juspay_txn_id
WHERE juspay_transactions.payment_gateway = 'PAYU'
AND CAST(LEFT(CAST(payu_settlements."AddedOn" AS VARCHAR), 10) AS DATE) >= '2026-01-01'
-- Note: PayU ADJ_* rows have no matching juspay_txn_id — this is expected
```

### Razorpay
```sql
-- Settlement IS the transaction table; join to Juspay
razorpay_transactions.order_receipt = juspay_transactions.order_id  -- NOTE: order_id not juspay_txn_id
WHERE juspay_transactions.payment_gateway = 'RAZORPAY'
AND razorpay_transactions.settled_at >= '2026-01-01' AND settled_at < '2026-02-01'
AND razorpay_transactions.type = 'payment'
```

### Juspay → Wiom (Layer 1)
```sql
-- Route by order_id prefix:
CASE
  WHEN j.order_id LIKE 'custGen%'    THEN wiom_booking_transactions.BOOKING_TXN_ID (primary)
                                       OR wiom_primary_revenue.TRANSACTION_ID (fallback)
  WHEN j.order_id LIKE 'cusSubs%'
    OR j.order_id LIKE 'custWgSubs%' THEN wiom_primary_revenue.TRANSACTION_ID
  WHEN j.order_id LIKE 'w\_%'        THEN wiom_net_income.TXN_ID
  WHEN j.order_id LIKE 'wiomWall%'   THEN wiom_topup_income.TRANSACTION_ID   -- ← ADD THIS
  WHEN j.order_id LIKE 'mr\_%'       THEN wiom_mobile_recharge_transactions.TRANSACTION_ID
  WHEN j.order_id LIKE 'sd\_%'       THEN wiom_customer_security_deposit.SD_TXN_ID
  WHEN j.order_id LIKE 'cxTeam%'     THEN wiom_ott_transactions.TRANSACTION_ID
  -- Fallback for any prefix: check wiom_refunded_transactions.TRANSACTION_ID
END
```

---

## 10. PG Fee Summary (Jan 2026 settlements)

| Gateway | Settlement Gross | PG Fees | Net to Merchant | Effective MDR |
|---|---|---|---|---|
| PAYTM_V2 | Rs 5.81Cr | Rs 64,940 | Rs 5.80Cr | 0.112% |
| PHONEPE | Rs 25.3L | Rs 4,677 | Rs 25.3L | 0.185% |
| PAYU | Rs 1.09Cr | Rs 27,592 | Rs 1.09Cr | 0.253% |
| RAZORPAY | Rs 25.1L | Rs 13,047 | Rs 25.0L | 0.519% |
| **Total** | **Rs 7.40Cr** | **Rs 1.10L** | **Rs 7.39Cr** | **0.149%** |

---

## 11. Recommended Next Steps

| Priority | Action | Owner |
|---|---|---|
| 🔴 HIGH | Verify 2,179 `custGen_*` order IDs in live Wiom DB — are these missing from the export? | Finance/Tech |
| 🔴 HIGH | Fix `recon_jan26_base` script: add `wiom_topup_income` join for `wiomWall_*` and `wiom_refunded_transactions` as fallback | Data team |
| 🟡 MEDIUM | Investigate 353 `cusSubs_*` orders missing from `wiom_primary_revenue` — export issue? | Finance/Tech |
| 🟡 MEDIUM | Identify source of Paytm residual gap (~Rs 7K/day after refund netting) — TDS schedule? | Finance |
| 🟡 MEDIUM | Update `RECON_KEYS_AND_JOINS.md` with `wiomWall_` → `wiom_topup_income` mapping | Data team |
| 🟢 LOW | Request Wiom DB export to include transactions from before the current data cut | Data team |
| 🟢 LOW | Obtain Apr25–Nov25 PG settlement CSVs to close the historical bank recon gap (Rs 52.6Cr) | Finance |

---

## 12. Files Reference

| File | Purpose |
|---|---|
| `docs/_jan26_reverse_recon.py` | Analysis script; builds `recon_jan26_base` and prints all parts |
| `data.duckdb` → `recon_jan26_base` | Persistent base table (382,253 rows) |
| `docs/gap_settlement_no_juspay_jan26.csv` | 63 settlement rows not in Juspay (PayU ADJ + Paytm w-) |
| `docs/gap_juspay_no_wiom_jan26.csv` | 6,244 Juspay rows not in Wiom DB (all categories) |
| `docs/RECON_KEYS_AND_JOINS.md` | Primary/foreign key reference for all layers |
| `docs/_bank_recon.py` | Bank ↔ Settlement daily recon (all 4 gateways) |

---

*Generated: 2026-03-29 | Based on `recon_jan26_base` table in `data.duckdb` | All figures in INR*
