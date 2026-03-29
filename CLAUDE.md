# MoneyMoney!!! - Project Context

## Project Overview
Local database project for loading and working with large CSV datasets using DuckDB.

## Key References
- **Progress tracker:** `docs/PROGRESS.md` — always update after every stage
- **Database:** `data.duckdb` (DuckDB, file-based, no server)
- **DB manager:** `db_manager.py` — CLI tool for loading CSVs, listing tables, running queries

## Directory Structure
```
moneymoney!!!/
├── CLAUDE.md          # This file - project context
├── docs/              # Progress files, documentation, artifacts
│   └── PROGRESS.md    # Plan & progress tracker
├── db_manager.py      # Database management script
├── data.duckdb        # DuckDB database file
└── (code folders)     # Added as project grows
```

## Execution Rules (MUST FOLLOW)
1. **Build in testable units** — every feature/change must be small and independently testable
2. **Run tests after every implementation** — verify correctness before moving on
3. **Ask for manual audit/QA when needed** — pause and request user review at checkpoints
4. **Then proceed** — only continue after tests pass and QA is cleared

## Database: 20 tables in data.duckdb
- **Group A — Wiom Internal (8 tables):** booking_transactions, customer_security_deposit, ott_transactions, primary_revenue, topup_income, net_income, mobile_recharge_transactions, refunded_transactions
- **Group B — PG Transactions (4):** phonepe, razorpay, payu, paytm transactions
- **Group C — PG Settlements (3):** phonepe, payu, paytm settlements
- **Group D — Refunds (2):** phonepe_refunds, paytm_refunds
- **Group E — Bank & Juspay (3):** bank_receipt_from_pg (1,098 rows, ALL 4 gateways), juspay_transactions, juspay_refunds

## bank_receipt_from_pg — Gateway Mapping
- `01 Paytm-Wallet (WIOM Gold)` — 357 rows, Rs 63.2Cr (Apr25–Mar26)
- `02 Payu-Wallet` — 263 rows, Rs 8.23Cr (Apr25–Mar26)
- `05 PhonePe Wallet-2` — 240 rows, Rs 4.03Cr (May25–Mar26)
- `06 Razorpay Wallet` — 238 rows, Rs 4.04Cr (Apr25–Mar26)
- Source: `csv/Bank-Receipt-from-PG.xlsx` (4 sheets — originally only Paytm sheet was loaded; fixed 2026-03-29)

## Reconciliation Flow (4 layers)
1. **Wiom DB ↔ Juspay** — link via BOOKING_TXN_ID/TRANSACTION_ID/TXN_ID = juspay.order_id
2. **Juspay ↔ PG Gateways** — link by gateway (Paytm/PhonePe/PayU/Razorpay)
3. **PG Transactions ↔ PG Settlements** — 100% settled across all 4 gateways (Jan26)
4. **PG Settlements ↔ Bank Receipts** — ALL 4 gateways CLEAN (<1.3% gap, Dec25–Feb26)

## Reconciliation Join Keys
- **Layer 1:** `wiom_booking_transactions.BOOKING_TXN_ID` = `juspay_transactions.order_id`
- **Layer 2 (Paytm):** `juspay.juspay_txn_id` = `REPLACE(paytm.Order_ID, "'", "")`
- **Layer 2 (PhonePe):** `juspay.juspay_txn_id` = `phonepe."Merchant Order Id"`
- **Layer 2 (PayU):** `juspay.juspay_txn_id` = `payu.txnid`
- **Layer 2 (Razorpay):** `juspay.order_id` = `razorpay.order_receipt` ← different key!
- **Layer 3 (Paytm):** `REPLACE(paytm_transactions.Order_ID,"'","")` = `REPLACE(paytm_settlements.Order_ID,"'","")`
- **Layer 3 (PhonePe):** `phonepe_transactions."Merchant Order Id"` = `phonepe_settlements."Merchant Order Id"`
- **Layer 3 (PayU):** `payu_transactions.txnid` = `payu_settlements."Merchant Txn ID"`
- **Layer 3 (Razorpay):** settlement embedded in `razorpay_transactions` (settlement_id, settled_at columns)
- **Layer 4:** DATE-level matching only (UTR systems differ; 1 deposit/day per gateway confirmed)

## Key Context
- Cash transactions handled via partner wallet deduction (not through Juspay/PGs)
- `mr_*` order IDs = mobile recharges (wiom_mobile_recharge_transactions)
- `custGen_*` = customer-generated WiFi recharges
- `w_*` = wallet/net-income topups
- `WIFI_SRVC_*` in primary_revenue routes outside Juspay (partner wallet)
- `wiom_refunded_transactions` = transactions that were refunded (REFUND_STATUS=1), links to juspay via TRANSACTION_ID=order_id
- Paytm Order_ID has embedded single quotes — always use `REPLACE(Order_ID, chr(39), '')`
- PhonePe "Total Fees" stored as NEGATIVE numbers (sign convention: -Rs 4,677 = Rs 4,677 charged)
- Razorpay charges MDR on ALL payment methods incl. UPI (unlike PhonePe which is 0% for standard UPI)

## Analysis Scripts (docs/)
- `_jan26_recon.py` — Layer 1: Wiom DB vs Juspay (Jan 2026)
- `_step2_analysis.py` — Layer 1 mismatch deep-dive
- `_total_inflow_jan26.py` — Total inflow summary Jan 2026
- `_layer2_recon.py` — Layer 2: Juspay vs PG gateways
- `_layer3_recon.py` — Layer 3: PG transactions vs PG settlements
- `_phonepe_fee_deepdive.py` — PhonePe MDR analysis
- `_razorpay_fee_deepdive.py` — Razorpay MDR analysis (0.49% blended, all methods incl. UPI)
- `_settlement_fullrecon.py` — Full settlement universe reconciliation (all months)
- `_settlement_jan26_recon.py` — Jan 2026 scoped settlement reconciliation
- `_bank_recon.py` — Layer 4: PG settlements vs bank receipts (all 4 gateways)
- `_jan26_reverse_recon.py` — Reverse recon Bank→Settlement→Juspay→Wiom (builds `recon_jan26_base`)

## Key Data Findings (from Jan26 reverse recon)
- `wiomWall_*` order prefix = wallet topups → maps to `wiom_topup_income.TRANSACTION_ID` (NOT net_income)
- `paytm_settlements.transaction_type` = `'ACQUIRING'` for forward payments (NOT 'SALE')
- 2,617 `custGen_*/cusSubs_*` Juspay SUCCESS txns genuinely absent from Wiom DB export (Rs 11.87L)
- 3,367 MISSING_WIOM rows are actually in `wiom_refunded_transactions` — refunded, not lost
- PayU `ADJ_*` settlement rows = platform fee debits, NOT customer transactions (never in Juspay)
- Base recon table: `recon_jan26_base` (382,253 rows) persisted in data.duckdb

## Mismatch / Export CSVs (docs/)
- `mismatch_wiom_to_juspay_jan26_v2.csv`, `mismatch_juspay_to_wiom_jan26_v2.csv`
- `phonepe_fee_charged_trace.csv` (7,929 rows)
- `razorpay_fee_trace_jan26.csv` (16,495 rows)

## Workflow
- Update `docs/PROGRESS.md` at every stage with completed and pending items
- Keep this file updated with new context documents and references as they are created
