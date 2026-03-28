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
- **Group E — Bank & Juspay (3):** bank_receipt_from_pg, juspay_transactions, juspay_refunds

## Reconciliation Flow (3 layers)
1. **Wiom DB ↔ Juspay** — link via BOOKING_TXN_ID/TRANSACTION_ID/TXN_ID = juspay.order_id
2. **Juspay ↔ PG Gateways** — link by gateway (Paytm/PhonePe/PayU/Razorpay)
3. **PG Settlements ↔ Bank Receipts** — settlement vs actual bank deposit

## Key Context
- Cash transactions handled via partner wallet deduction (not through Juspay/PGs)
- `mr_*` order IDs = mobile recharges (wiom_mobile_recharge_transactions)
- `custGen_*` = customer-generated WiFi recharges
- `w_*` = wallet/net-income topups
- `WIFI_SRVC_*` in primary_revenue routes outside Juspay (partner wallet)
- `wiom_refunded_transactions` = transactions that were refunded (REFUND_STATUS=1), links to juspay via TRANSACTION_ID=order_id
- Analysis scripts: `docs/_jan26_recon.py`, `docs/_step2_analysis.py`, `docs/_total_inflow_jan26.py`
- Mismatch CSVs: `docs/mismatch_wiom_to_juspay_jan26.csv`, `docs/mismatch_juspay_to_wiom_jan26.csv`

## Workflow
- Update `docs/PROGRESS.md` at every stage with completed and pending items
- Keep this file updated with new context documents and references as they are created
