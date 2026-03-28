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
- [x] `Razorpay_transactions_dec_to_feb26.csv` → `razorpay_transactions` (53,644 rows, 27 cols)
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

### Layer 2 — Juspay vs PG Gateways (Paytm/PhonePe/PayU/Razorpay)
- [ ] Match juspay_transactions → PG transaction tables by gateway
- [ ] Mismatch analysis per gateway
- [ ] Refund reconciliation (juspay_refunds vs PG refund tables)

### Layer 3 — PG Settlements vs Bank Receipts
- [ ] Match PG settlement reports → bank_receipt_from_pg
- [ ] Settlement amount reconciliation

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
