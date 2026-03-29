# Project Progress

## Phase 1: Project Setup
- [x] Create project structure (CLAUDE.md, docs/, csv/)
- [x] Initialize DuckDB database (data.duckdb)
- [x] Create db_manager.py (CLI tool)
- [x] Inventory all CSV/data files

## Phase 2: Data Loading (Step-by-Step)
_Load each group, verify, then move to next._

### Group A — Wiom Internal DB Exports (8 tables) ✅ DONE
- [x] `booking-transactions-wiom-db.csv` → `wiom_booking_transactions` (20,785 rows, 22 cols)
- [x] `customer-security-deposit-wiom-db.csv` → `wiom_customer_security_deposit` (1,501 rows, 16 cols)
- [x] `ott-transactions-wiom-db.csv` → `wiom_ott_transactions` (381 rows, 34 cols) — multiline JSON handled
- [x] `primary-revenue-wiom-db.csv` → `wiom_primary_revenue` (660,895 rows, 44 cols) — MOBILE forced to VARCHAR
- [x] `topup-income-wiom-db.csv` → `wiom_topup_income` (625,184 rows, 10 cols)
- [x] `wiom-net-income-wiom-db.csv` → `wiom_net_income` (744,685 rows, 18 cols)
- [x] `mobile-recharge-transactions-wiom-db.csv` → `wiom_mobile_recharge_transactions` (5,780 rows, 34 cols) — mr_* mobile recharges
- [x] `transactions-which-were-refunded-wiom-db.csv` → `wiom_refunded_transactions` (18,189 rows, 34 cols) — refunded txns (REFUND_STATUS=1)

### Group B — PG Transactions (4 tables) ✅ DONE
- [x] `PhonePe_transaction_dec_to_feb26.csv` → `phonepe_transactions` (120,926 rows, 54 cols)
- [x] `Razorpay_transactions_dec_to_feb26.csv` → `razorpay_transactions` (53,644 rows → **51,835 rows** after dedup, 27 cols)
  - **Data cleanup (2026-03-29):** Removed 1,809 exact duplicate rows (634 order_receipts had 2 identical copies each) — CSV export artifact from Razorpay. Fixed via `SELECT DISTINCT *`.
- [x] `payu_transactions_dec_to_feb26.csv` → `payu_transactions` (295,095 rows, 85 cols)
- [x] Paytm transactions (3 CSVs → 1 table) → `paytm_transactions` (907,342 rows, 124 cols)

### Group C — PG Settlements (3 combined tables) ✅ DONE
- [x] PhonePe settlements (3 CSVs → 1 table) → `phonepe_settlements` (49,832 rows, 70 cols)
- [x] PayU settlements (1 CSV + 2 XLSX → 1 table) → `payu_settlements` (94,663 rows, 92 cols) — XLSX read via openpyxl+pandas
- [x] Paytm settlements (3 CSVs → 1 table) → `paytm_settlements` (927,025 rows, 101 cols)

### Group D — Refunds (2 combined tables) ✅ DONE
- [x] PhonePe refunds (3 CSVs → 1 table) → `phonepe_refunds` (982 rows, 24 cols)
- [x] Paytm refunds (3 CSVs → 1 table) → `paytm_refunds` (16,914 rows, 120 cols)

### Group E — Bank Receipt & Juspay (3 tables) ✅ DONE
- [x] `Bank-Receipt-from-PG.xlsx` → `bank_receipt_from_pg` (358 rows, 6 cols) — header on row 2
- [x] Juspay forward txns (3 xlsb → 1 table) → `juspay_transactions` (1,096,610 rows, 39 cols) — via pyxlsb+pandas
- [x] Juspay refunds (1 xlsb) → `juspay_refunds` (20,179 rows, 30 cols) — via pyxlsb+pandas

## Phase 3: Data Validation & QA
- [ ] Verify row counts match source files
- [ ] Check column types are correct
- [ ] Spot-check sample records
- [ ] Manual QA checkpoint with user

## Phase 4: Reconciliation Analysis
_Three-layer reconciliation: Wiom DB → Juspay → PGs → Bank_

### Layer 1 — Wiom DB vs Juspay (Jan 2026) ✅ DONE
- [x] Initial full-range analysis → `docs/_jan26_recon.py`
- [x] Mismatch deep-dive (Jan 2026) → `docs/_step2_analysis.py`
- Key findings (Jan 2026):
  - **20 tables in DB** (added `wiom_mobile_recharge_transactions` + `wiom_refunded_transactions`)
  - `wiom_mobile_recharge_transactions` resolved 2,853 of 8,149 Juspay orphans (mr_* pattern)
  - `wiom_refunded_transactions` resolved 2,941 more orphans (refunded custGen/cusSubs)
  - booking: 100% match (3,809/3,809), but 603 NULL status + 10 amount diffs
  - primary_revenue (online): 95.8% match (134,848/140,776), 3,719 WIFI_SRVC outside Juspay
  - net_income: 99.997% match (234,002/234,003), cleanest table
  - **Remaining Juspay orphans: 2,355 (0.62%), Rs 10.65L** — down from 5,296
  - Total inflow: Juspay Rs 7.40Cr vs Wiom Rs 7.31Cr (gap Rs 9.68L)
  - Matched amounts extremely clean: PR diff Rs 3,196 across 9 records; NI diff Rs 11 across 2
- [x] Mismatch CSVs: `docs/mismatch_wiom_to_juspay_jan26_v2.csv`, `docs/mismatch_juspay_to_wiom_jan26_v2.csv`
- [x] Total inflow analysis → `docs/_total_inflow_jan26.py`
- [ ] Layer 1 reconciliation HTML visualization

### Layer 2 — Juspay vs PG Gateways (Paytm/PhonePe/PayU/Razorpay) ✅ DONE
- [x] Match juspay_transactions → PG transaction tables by gateway → `docs/_layer2_recon.py`
- [x] Mismatch analysis per gateway (all 4 gateways)
- [x] Refund reconciliation (juspay_refunds vs phonepe_refunds + paytm_refunds)
- Key findings (Jan 2026):
  - **100% transaction match rate across all 4 gateways** — every Juspay SUCCESS txn found in PG table
  - PAYTM_V2 (314,833 txns): exact amount match, 35 Paytm-only txns (Rs 2,787) — date edge cases
  - PHONEPE (16,792 txns): exact match on both count and amount
  - PAYU (33,242 txns): count match but amount diff Rs 54,160 (Juspay > PG) — needs investigation
  - RAZORPAY (16,494 txns): 100% match, Rs 0 diff after deduplicating 1,809 duplicate rows in razorpay_transactions
  - **No Juspay-only orphans** — all Juspay Jan26 SUCCESS txns appear in PG tables
  - Join keys confirmed: PAYTM/PhonePe/PayU use juspay_txn_id; Razorpay uses order_id = order_receipt
  - Refunds: 3,196 Juspay refunds — Paytm (2,656/2,656 matched), PhonePe (158/158 matched), PayU/Razorpay (no separate PG table)

### Layer 3 — PG Transactions vs PG Settlements ✅ DONE
- [x] Reconcile PG transactions → PG settlement records → `docs/_layer3_recon.py`
- [x] Per-gateway settlement analysis (Paytm, PhonePe, PayU, Razorpay)
- [x] Settlement batch / UTR breakdown per gateway
- Key findings (Jan 2026):
  - **100% settlement rate across all 4 gateways** — all Juspay Jan26 SUCCESS txns are settled
  - PAYTM_V2: 314,833 txns, gross Rs 5.84Cr, fees Rs 64,858 (commission+GST), 58 UTR batches (daily)
  - PHONEPE: 16,792 txns, gross Rs 25.46L, fees Rs -4,677 (negative sign = Rs 4,677 MDR charged)
  - PAYU: 33,242 txns, gross Rs 1.10Cr, fees Rs 27,592, 32 UTR batches
  - RAZORPAY: 16,494 txns, gross Rs 24.44L, **fees Rs 13,732 (MDR Rs 12,084 + GST Rs 1,648)** — fee/tax embedded in razorpay_transactions
  - **Total net settled across all gateways: Rs 7.43Cr | Total fees deducted: Rs ~1.02L**
  - **CORRECTION:** Razorpay Layer 3 originally reported Rs 0 fees — fixed after discovering fee/tax columns exist in razorpay_transactions

### PG Fee Deep-Dives
- [x] PhonePe negative fees investigation → `docs/_phonepe_fee_deepdive.py`
  - Root cause: PhonePe stores MDR as NEGATIVE numbers (sign convention). `-Rs 4,677` = normal Rs 4,677 fee
  - 7,929 fee-bearing txns: standard UPI = 0% MDR (RBI mandate); Bank Account + RuPay Credit via UPI = MDR charged
  - Effective MDR: 0.190%; largest single fee Rs 1,150 on Rs 50K wallet top-up (RuPay Credit Card)
  - Exported: `docs/phonepe_fee_charged_trace.csv` (7,929 rows, traced to Wiom DB)
- [x] Razorpay fee investigation → `docs/_razorpay_fee_deepdive.py`
  - **ALL 16,494 Jan26 Razorpay txns have fee > 0** — Razorpay charges MDR on ALL payment methods incl. UPI
  - Total: Rs 12,084 MDR + Rs 1,648 GST = Rs 13,732 total fees; Effective MDR: 0.4944%
  - Methods: UPI (99.2% of txns, 0.4566% MDR), Visa/MC/RuPay credit cards (~1.85-1.94% MDR), netbanking (1.95%)
  - By order pattern: custGen_* (WiFi recharges, Rs 2.31Cr gross, Rs 12,452 fee) + w_* (wallet topups, Rs 1.04L gross)
  - Exported: `docs/razorpay_fee_trace_jan26.csv` (16,495 rows, traced to Wiom DB)

### Layer 3b — PG Settlements vs Bank Receipts ✅ DONE
- [x] **DATA FIX (2026-03-29):** `bank_receipt_from_pg` was loaded with only the Paytm sheet (358 rows). Reloaded with all 4 sheets → **1,098 rows** covering Paytm/PayU/PhonePe/Razorpay
- [x] UTR investigation → Paytm UTR (`UTIBR6*`) ≠ bank RTGS UTR (`UTIBH*`) — different systems, no key join; use date-level matching
- [x] All 4 gateways reconciled: settlement net vs bank deposits by date → `docs/_bank_recon.py`
- Key findings (Dec25–Feb26, months with both settlement + bank data):
  - **Paytm: -1.28% gap** (Rs 2.20Cr over 3 months) — refund deductions account for majority; ~0.6% residual likely TDS
  - **PhonePe: -0.53% gap** → CLEAN; worst month Rs -16.8K
  - **PayU: -0.40% to -1.70% gap** → CLEAN (Jan26 Rs -43.9K, Dec25 Rs -1.79L, Feb26 +Rs 2.16L)
  - **Razorpay: -0.32% to -0.60% gap** → CLEAN; daily match near-perfect
  - Bank file gateway mapping: `01 Paytm-Wallet (WIOM Gold)` | `02 Payu-Wallet` | `05 PhonePe Wallet-2` | `06 Razorpay Wallet`
  - Apr25–Nov25: bank deposits exist (Rs 52.6Cr total) but PG settlement CSVs not available for that period
- [ ] Investigate Paytm residual gap (~0.6%/month): TDS deductions, platform charges not in paytm_refunds table

## Phase 5: Data Dictionary
- [x] Profile all 916 columns (nulls, distinct counts, sample values) → `docs/_column_stats.csv`
- [x] Generate draft data dictionary (916/916 defined) → `docs/DATA_DICTIONARY_DRAFT.csv`
- [~] Send to finance team for review and corrections
- [ ] Incorporate finance team feedback
- [ ] Finalize data dictionary → `docs/DATA_DICTIONARY.md`

---

## Legend
- [x] Completed
- [~] In progress
- [ ] Pending
