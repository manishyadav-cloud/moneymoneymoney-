# Project Progress

## Phase 1: Project Setup
- [x] Create project structure (CLAUDE.md, docs/, csv/)
- [x] Initialize DuckDB database (data.duckdb)
- [x] Create db_manager.py (CLI tool)
- [x] Inventory all CSV/data files

## Phase 2: Data Loading (Step-by-Step)
_Load each group, verify, then move to next._

### Group A — Wiom Internal DB Exports (6 tables) ✅ DONE
- [x] `booking-transactions-wiom-db.csv` → `wiom_booking_transactions` (20,785 rows, 22 cols)
- [x] `customer-security-deposit-wiom-db.csv` → `wiom_customer_security_deposit` (1,501 rows, 16 cols)
- [x] `ott-transactions-wiom-db.csv` → `wiom_ott_transactions` (381 rows, 34 cols) — multiline JSON handled
- [x] `primary-revenue-wiom-db.csv` → `wiom_primary_revenue` (660,895 rows, 44 cols) — MOBILE forced to VARCHAR
- [x] `topup-income-wiom-db.csv` → `wiom_topup_income` (625,184 rows, 10 cols)
- [x] `wiom-net-income-wiom-db.csv` → `wiom_net_income` (744,685 rows, 18 cols)

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

## Phase 4: Data Dictionary
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
