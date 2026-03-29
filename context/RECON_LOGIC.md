# Reconciliation Logic & Process

> **Purpose:** How the end-to-end reconciliation works — layer by layer, gap categories, formulas, and the standard monthly process.

---

## Money Flow (Left to Right)

```
Customer   →   Wiom DB    →   Juspay       →   Payment Gateway   →   PG Settlement   →   Bank
pays Rs X      records         orchestrates      processes             batches daily       receives
               the order       the payment        the payment           nets fees           RTGS/NEFT
                                                                        deducts refunds
```

**Reverse recon (our approach):** Start from Bank (ground truth) and trace backwards.

```
Bank deposit  →  PG Settlement  →  Juspay  →  Wiom DB
(what arrived)   (what PG sent)    (what was    (what was
                                    ordered)     recorded)
```

---

## The 4 Layers

### Layer 1: Wiom DB ↔ Juspay
**Question:** Did every Wiom order reach Juspay? Did every Juspay payment have a Wiom record?

- **Join:** Wiom txn ID = `juspay_transactions.order_id`
- **Direction:** Bidirectional — check both sides
- **Key insight:** Different Wiom tables for different order_id prefixes (see JOIN_KEYS.md)
- **Expected match rate:** ~98-99% (gap = refunded txns + Wiom export gaps)

### Layer 2: Juspay ↔ PG Transactions
**Question:** Did every Juspay SUCCESS payment have a matching PG record?

- **Join:** Per gateway (see JOIN_KEYS.md — Razorpay uses different key!)
- **Direction:** Juspay → PG
- **Expected match rate:** 100% (Jan26 verified)

### Layer 3: PG Transactions ↔ PG Settlements
**Question:** Was every PG transaction settled in a batch?

- **Join:** Order ID within same gateway
- **Direction:** PG txn → PG settlement
- **Includes:** Fee analysis (MDR + GST per gateway)
- **Expected match rate:** 100% (Jan26 verified)

### Layer 4: PG Settlements ↔ Bank Receipts
**Question:** Does the settlement net match what arrived in the bank?

- **Join:** DATE-level only (no UTR join possible)
- **Direction:** Settlement aggregate by date → bank deposit on same date
- **Formula (Paytm):** `Bank deposit = Settlement net − Refund deductions`
- **Formula (others):** `Bank deposit ≈ Settlement net` (small residual gap)
- **Expected gap:** < 1.5% (0.3-0.7% for PhonePe/PayU/Razorpay; 0.7-1.3% for Paytm due to refund netting)

---

## Gap Categories

Every row in `recon_{month}_base` gets classified:

| trace_status | Meaning | Expected? | Jan26 % | Action |
|-------------|---------|-----------|---------|--------|
| `FULLY_TRACED` | Matched at all 4 layers | ✅ Yes | 98.35% | None |
| `REFUNDED` | Found in `wiom_refunded_transactions` | ✅ Yes | 0.88% | None — money was returned to customer |
| `WALLET_TOPUP` | `wiomWall_*` in `wiom_topup_income` | ✅ Yes | 0.07% | None — maps to topup table |
| `PG_ADJUSTMENT` | PayU `ADJ_*` or Paytm `w-*` micro rows | ✅ Yes | 0.02% | None — PG internal bookkeeping |
| `MISSING_WIOM` | In Juspay + PG + Bank, NOT in any Wiom table | ⚠️ No | 0.69% | Investigate — possible Wiom DB export gap |
| `MISSING_JUSPAY` | In PG settlement, NOT in Juspay | ⚠️ No | 0.02% | Investigate — usually PG adjustment rows |
| `BANK_GAP` | Settlement net ≠ bank deposit (date level) | ⚠️ Varies | ~0.6% | Investigate — TDS, platform fees, timing |

---

## Key Formulas

### Settlement Net
```
Paytm:    settled_amount = amount - commission - gst
PhonePe:  "Net Amount" = "Transaction Amount" + "Total Fees" + "Total Tax"   (fees are negative!)
PayU:     "Net Amount" = "Amount" - "Total Processing fees" - "Service Tax"
Razorpay: net = amount - fee - tax
```

### Bank Deposit vs Settlement
```
Paytm:    bank_deposit = SUM(settled_amount) - SUM(refund_settled_amount)   for that date
PhonePe:  bank_deposit ≈ SUM("Net Amount")   for forward txns on that date
PayU:     bank_deposit ≈ SUM("Net Amount")   for that date
Razorpay: bank_deposit ≈ SUM(amount - fee - tax)   for payments on that date
```

### Blended MDR (Jan26)
```
Total fees: Rs 1.11L on Rs 7.43Cr gross = 0.149% blended MDR
  Paytm:    0.111%
  PhonePe:  0.184%
  PayU:     0.251%
  Razorpay: 0.494%
```

---

## Source Month vs Settlement Month

**Critical concept:** A transaction created in December 2025 may be settled in January 2026.

```
Transaction created:  Dec 30, 2025  (in Juspay, Wiom DB)
PG processes:         Dec 30, 2025  (in paytm_transactions)
Settlement batch:     Jan 02, 2026  (in paytm_settlements)  ← DIFFERENT MONTH
Bank deposit:         Jan 02, 2026  (in bank_receipt_from_pg)
```

**Impact on recon:**
- When filtering by `settled_date` in Jan 2026, ~3.5% of rows are Dec 2025 transactions
- These Dec25 txns trace correctly to Juspay and Wiom DB — they're NOT gaps
- The `source_month` column in the base table captures this distinction

---

## Order ID Prefix → Wiom Table Routing

| Prefix | Wiom Table | Key Column | What It Is |
|--------|-----------|------------|------------|
| `custGen_*` | `wiom_booking_transactions` | `BOOKING_TXN_ID` | Customer WiFi recharge |
| `custGen_*` | `wiom_primary_revenue` | `TRANSACTION_ID` | Revenue record (same txn) |
| `cusSubs_*` | `wiom_primary_revenue` | `TRANSACTION_ID` | Subscription payment |
| `custWgSubs_*` | `wiom_primary_revenue` | `TRANSACTION_ID` | Widget subscription |
| `w_*` | `wiom_net_income` | `TXN_ID` | Partner wallet / net income topup |
| `wiomWall_*` | `wiom_topup_income` | `TRANSACTION_ID` | Wallet top-up via PG |
| `mr_*` | `wiom_mobile_recharge_transactions` | `TRANSACTION_ID` | Mobile recharge |
| `sd_*` | `wiom_customer_security_deposit` | `SD_TXN_ID` | Security deposit |
| `cxTeam_*` | `wiom_ott_transactions` | `TRANSACTION_ID` | CX team OTT order |
| *(any — fallback)* | `wiom_refunded_transactions` | `TRANSACTION_ID` | Refunded order (REFUND_STATUS=1) |

**Prefixes NOT in Juspay (cash / partner wallet):**
| Prefix | Table | Why Not in Juspay |
|--------|-------|-------------------|
| `WIFI_SRVC_*` | `wiom_primary_revenue` | Partner wallet deduction |
| `BILL_PAID_*` | `wiom_primary_revenue` | Cash / partner wallet |

---

## Monthly Recon Process (Step by Step)

### Prerequisites
- All source data for the month loaded into DuckDB (PG txns, settlements, refunds, Juspay, Wiom DB, bank)
- Previous month's recon completed (for comparison)

### Step 1: Scope the month
```sql
-- Define settlement date range (e.g., Feb 2026)
-- Paytm:    settled_date >= '2026-02-01' AND settled_date < '2026-03-01'
-- PhonePe:  CAST("Settlement Date" AS DATE) >= '2026-02-01' AND < '2026-03-01'
-- PayU:     CAST(LEFT(CAST("AddedOn" AS VARCHAR),10) AS DATE) >= '2026-02-01'
-- Razorpay: settled_at >= '2026-02-01' AND settled_at < '2026-03-01' AND type='payment'
```

### Step 2: Build base table (reverse direction)
1. Pull all settlement rows for target month (all 4 gateways)
2. LEFT JOIN to `juspay_transactions` using Layer 2 keys
3. LEFT JOIN to Wiom tables using Layer 1 routing (by prefix)
4. LEFT JOIN to `wiom_refunded_transactions` as fallback
5. LEFT JOIN to `bank_receipt_from_pg` by date + gateway
6. Classify each row into `trace_status`
7. Persist as `recon_{month}_base` in DuckDB

### Step 3: Generate summary
- Count by trace_status
- Amount waterfall: Bank → Settlement → Juspay → Wiom
- Per-gateway breakdown
- Fee summary

### Step 4: Investigate gaps
- MISSING_WIOM: check order_id prefixes, cross-check wiom_refunded_transactions
- MISSING_JUSPAY: check if PG adjustment rows
- BANK_GAP: check refund netting, TDS timing

### Step 5: Compare to previous month
- Is the gap % improving or worsening?
- Any new gap categories appearing?
- MDR rate changes?

---

## Known Gaps (Jan 2026 Baseline)

| Gap | Rows | Amount | Root Cause | Status |
|-----|------|--------|-----------|--------|
| `custGen_*` not in Wiom DB | 2,179 | Rs 10.08L | Wiom DB export incomplete | ⚠️ Open — finance/tech to verify |
| `cusSubs_*` not in Wiom DB | 353 | Rs 1.77L | Wiom DB export gap | ⚠️ Open |
| Paytm bank residual (~0.6%) | — | Rs ~7K/day | Likely TDS or platform charges | ⚠️ Under investigation |
| Amount mismatch (all GW) | 27 | Rs 3,460 | Rounding, free plans | ✅ Negligible |

---

*Updated: 2026-03-30 | Based on Jan 2026 end-to-end reconciliation*
