# MoneyMoney!!! - Payment Reconciliation System

A local-first payment reconciliation tool for Wiom, built on [DuckDB](https://duckdb.org/). Loads transaction data from Wiom's internal DB, Juspay, 4 payment gateways (Paytm, PhonePe, PayU, Razorpay), and bank receipts into a single analytical database for 3-layer reconciliation.

## Database Overview

**20 tables | 5.8M+ rows | Dec 2025 - Feb 2026**

| Group | Tables | Description |
|---|---|---|
| **A - Wiom Internal** (8) | booking_transactions, primary_revenue, net_income, topup_income, customer_security_deposit, ott_transactions, mobile_recharge_transactions, refunded_transactions | Wiom's own DB exports |
| **B - PG Transactions** (4) | paytm, phonepe, payu, razorpay transactions | Raw transaction logs from each payment gateway |
| **C - PG Settlements** (3) | paytm, phonepe, payu settlements | Settlement reports from PGs |
| **D - Refunds** (2) | phonepe_refunds, paytm_refunds | Refund records from PGs |
| **E - Bank & Juspay** (3) | bank_receipt_from_pg, juspay_transactions, juspay_refunds | Juspay orchestrator data + bank deposit receipts |

## Reconciliation Flow (3 Layers)

```
Layer 1: Wiom DB  <-->  Juspay         (TRANSACTION_ID = order_id)
Layer 2: Juspay   <-->  PG Gateways    (juspay_txn_id = PG order/txn IDs)
Layer 3: PG Settlements <--> Bank      (settlement amounts vs bank deposits)
```

Open `docs/reconciliation_flow.html` in a browser for the full interactive column-level link map.

## Prerequisites

- Python 3.10+
- pip packages:

```bash
pip install duckdb pandas openpyxl pyxlsb
```

## Project Structure

```
moneymoney!!!/
├── README.md                  # This file
├── CLAUDE.md                  # AI assistant context & execution rules
├── db_manager.py              # CLI tool for DB operations
├── data.duckdb                # DuckDB database (not in git, ~340MB)
├── csv/                       # Source CSV/XLSX/XLSB files (not in git)
│   ├── wiom-db/               # 8 Wiom internal exports
│   ├── Juspay_Dec-Feb26-Txns_and_Refund/  # 4 Juspay xlsb files
│   ├── PG-settlements/        # PhonePe & PayU settlement reports
│   ├── phonepe-refunds/       # PhonePe refund CSVs (Dec-Feb)
│   ├── PhonePe_transaction_dec_to_feb26.csv
│   ├── Razorpay_transactions_dec_to_feb26.csv
│   ├── payu_transactions_dec_to_feb26.csv
│   └── Bank-Receipt-from-PG.xlsx
└── docs/
    ├── PROGRESS.md                        # Project plan & progress tracker
    ├── reconciliation_flow.html           # Interactive column-level link map
    ├── DATA_DICTIONARY_DRAFT.csv          # 984 column definitions across 20 tables
    ├── _column_stats.csv                  # Column profiling (nulls, distincts, samples)
    ├── mismatch_wiom_to_juspay_jan26_v2.csv   # Wiom->Juspay mismatches (Jan 2026)
    ├── mismatch_juspay_to_wiom_jan26_v2.csv   # Juspay->Wiom orphans (Jan 2026)
    └── _*.py                              # Analysis scripts
```

> **Note:** `data.duckdb` (~340MB) and `csv/` are excluded from git via `.gitignore`. You need the source CSV files to rebuild the database.

## Quick Start

### 1. Rebuild the database from CSVs

Place all source CSV/XLSX/XLSB files in the `csv/` folder matching the structure above, then use the db_manager:

```bash
# List tables
python db_manager.py list

# Load a CSV
python db_manager.py load csv/wiom-db/booking-transactions-wiom-db.csv wiom_booking_transactions

# Run a query
python db_manager.py query "SELECT COUNT(*) FROM wiom_booking_transactions"
```

Or run the analysis scripts to load all tables at once (they use `CREATE OR REPLACE TABLE`).

### 2. Run reconciliation analysis

```bash
# Layer 1: Wiom DB vs Juspay (Jan 2026)
python docs/_jan26_recon.py
python docs/_step2_analysis.py

# Total inflow comparison
python docs/_total_inflow_jan26.py

# Generate mismatch CSVs
python docs/_gen_mismatch_csvs.py
```

### 3. View the link map

Open `docs/reconciliation_flow.html` in any browser - it shows all 20 tables with column-level join keys and match counts.

## db_manager.py Usage

```bash
# List all tables with row counts
python db_manager.py list

# Load a CSV into a table
python db_manager.py load <csv_path> [table_name]

# Run any SQL query
python db_manager.py query "SELECT * FROM wiom_booking_transactions LIMIT 5"

# Describe a table schema
python db_manager.py describe <table_name>
```

## Key ID Patterns

| Pattern | Source Table | Description |
|---|---|---|
| `custGen_*` | primary_revenue, booking | Customer-generated WiFi recharges |
| `custWgSubs_*` | primary_revenue | Customer WiFi gateway subscriptions |
| `w_*` | net_income | Wallet/net-income topups |
| `mr_*` | mobile_recharge_transactions | Mobile recharges |
| `sd_*` | customer_security_deposit | Security deposits |
| `WIFI_SRVC_*` | primary_revenue | Partner wallet transactions (outside Juspay) |
| `BILL_PAID_*` | primary_revenue | Bill payment confirmations |

## Key Join Rules

```
Wiom TRANSACTION_ID = Juspay order_id = Razorpay order_receipt
Juspay juspay_txn_id = 'wiom-' + order_id + '-1' = PhonePe/PayU/Paytm Order ID
```

> Paytm IDs have embedded single quotes - use `TRIM(col, chr(39))` for joins.

## Layer 1 Results (Jan 2026)

| Metric | Value |
|---|---|
| Juspay SUCCESS total | Rs 7.40 Cr (381,361 txns) |
| Wiom DB online total | Rs 7.31 Cr (375,745 txns) |
| Gap | Rs 9.68L (1.3%) |
| Matched amount accuracy | Rs 3,196 diff across 134,840 PR matches |
| Juspay orphans (after all tables) | 2,355 txns (0.62%), Rs 10.65L |
| Status mismatches | 603 (Wiom NULL, Juspay SUCCESS) |

## Current Status

- [x] Phase 1: Project setup
- [x] Phase 2: All 20 tables loaded
- [x] Phase 4: Layer 1 reconciliation (Wiom vs Juspay) complete
- [x] Phase 5: Data dictionary draft (984 columns defined)
- [ ] Phase 4: Layer 2 (Juspay vs PG Gateways)
- [ ] Phase 4: Layer 3 (PG Settlements vs Bank)
- [ ] Phase 5: Finance team review of data dictionary

See `docs/PROGRESS.md` for detailed progress tracking.
