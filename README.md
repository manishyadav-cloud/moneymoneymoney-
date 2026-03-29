# Wiom Payment Reconciliation Tool

Traces every rupee from customer payment to bank deposit. Built on [DuckDB](https://duckdb.org/) + [Claude Code](https://claude.ai/claude-code) — the finance team talks in English, Claude Code does the SQL.

## How It Works

```
Customer pays → Wiom DB → Juspay → Payment Gateway → PG Settlement → Bank
                (app)     (orchestrator)  (Paytm/PhonePe    (batch payout)   (RTGS/NEFT)
                                           PayU/Razorpay)
```

Claude Code reads the `context/` docs, understands all join keys and data quirks, writes scripts, runs queries, and explains the results.

## Database

**20 tables | 5.8M+ rows | Dec 2025 – Feb 2026 | DuckDB (file-based, no server)**

| Group | Tables | Role |
|-------|--------|------|
| **A: Wiom Internal (8)** | booking_transactions, primary_revenue, net_income, topup_income, mobile_recharge, refunded, security_deposit, ott | Source — where the order originates |
| **B: PG Transactions (4)** | paytm, phonepe, payu, razorpay | Processing — PG acknowledges payment |
| **C: PG Settlements (3)** | paytm, phonepe, payu settlements | Batching — PG sends to bank |
| **D: PG Refunds (2)** | phonepe_refunds, paytm_refunds | Returns — money back to customer |
| **E: Bank & Juspay (3)** | juspay_transactions, juspay_refunds, bank_receipt_from_pg | Orchestrator + bank deposits |

## Project Structure

```
wiom-recon/
├── CLAUDE.md                    # Master context (Claude reads this first)
├── data.duckdb                  # DuckDB database (~340MB, not in git)
├── db_manager.py                # CLI: load CSVs, list tables, run queries
│
├── context/                     # 📚 Knowledge base — the core of the tool
│   ├── DATA_DICTIONARY.md       #   All 20 tables, key columns, relationships
│   ├── COLUMN_STATS.csv         #   916 columns with nulls, distinct counts, samples
│   ├── DATA_DICTIONARY_DRAFT.csv#   Full column-level definitions
│   ├── JOIN_KEYS.md             #   Every join condition across all 4 recon layers
│   ├── RECON_LOGIC.md           #   How recon works, gap categories, formulas
│   ├── GATEWAY_QUIRKS.md        #   ⚠️ Data traps per gateway (read before querying!)
│   └── BASE_TABLE_SCHEMA.md     #   Monthly recon base table schema
│
├── scripts/                     # Analysis scripts (Claude writes new ones here)
│   └── archive/                 #   Historical scripts from Jan26 exploration
│
├── output/                      # CSV/XLSX exports, gap reports
├── csv/                         # Source data files (not in git)
└── docs/                        # Progress tracking, architecture, reports
```

> `data.duckdb` and `csv/` are excluded from git via `.gitignore`.

## Quick Start

### Prerequisites
```bash
pip install duckdb pandas openpyxl pyxlsb
```

### Using with Claude Code
Open Claude Code in the project folder and talk naturally:
```
"Run reconciliation for January 2026"
"Show me the gap between Juspay and Wiom DB"
"Trace order custGen_abc123 end-to-end"
"Export all unmatched Paytm transactions to CSV"
"What MDR did we pay to Razorpay last month?"
```

### Using db_manager.py directly
```bash
python db_manager.py tables                    # List all tables with schemas
python db_manager.py load file.csv table_name  # Load a CSV
python db_manager.py query "SELECT ..."        # Run SQL
```

## Reconciliation Results (Jan 2026)

### 4-Layer Match Rates
| Layer | What | Match Rate |
|-------|------|-----------|
| **1: Wiom DB ↔ Juspay** | Did Wiom record every payment? | 98.35% fully traced |
| **2: Juspay ↔ PG Transactions** | Did Juspay → PG match? | 100% all 4 gateways |
| **3: PG Txn ↔ PG Settlements** | Was every txn settled? | 100% all 4 gateways |
| **4: Settlements ↔ Bank** | Did settlement reach bank? | <1.3% gap (CLEAN) |

### Amount Waterfall
```
Bank deposits:     Rs 7.35Cr
Settlement gross:  Rs 7.40Cr
Settlement net:    Rs 7.39Cr  (fees: Rs 1.11L, 0.149% blended MDR)
Juspay matched:    Rs 7.41Cr
Wiom DB matched:   Rs 7.11Cr
```

### Gap Summary
| Category | Rows | Amount | Status |
|----------|------|--------|--------|
| Fully traced (all layers) | 375,946 | Rs 7.11Cr | ✅ |
| Refunded (in wiom_refunded_transactions) | 3,367 | Rs 3.02L | ✅ Expected |
| Wallet topup (wiomWall_ → wiom_topup_income) | 260 | Rs 15.06L | ✅ Expected |
| Missing from Wiom DB | 2,617 | Rs 11.87L | ⚠️ Under investigation |
| PG adjustments (PayU ADJ, Paytm w-) | 63 | Rs -66.7K | ✅ PG internal |

### MDR by Gateway
| Gateway | Gross | Fees | MDR Rate |
|---------|-------|------|----------|
| Paytm | Rs 5.84Cr | Rs 64,858 | 0.111% |
| PhonePe | Rs 25.46L | Rs 4,677 | 0.184% |
| PayU | Rs 1.10Cr | Rs 27,592 | 0.251% |
| Razorpay | Rs 24.44L | Rs 13,732 | 0.494% |

## Monthly Workflow

1. **Load data** — Drop new month's CSVs in `csv/`, ask Claude to load them
2. **Run recon** — "Run full reconciliation for [month]"
3. **Review gaps** — "Show me the gap report"
4. **Investigate** — "Why is Paytm gap higher this month?"
5. **Export** — "Export unmatched transactions to CSV"

## Key Data Quirks

- **Paytm Order_ID** has embedded single quotes → `REPLACE(Order_ID, chr(39), '')`
- **PhonePe fees** stored as negative numbers → use `ABS()`
- **Razorpay** uses `order_id` (not `juspay_txn_id`) to join Juspay — different from all other gateways
- **Razorpay** has no separate settlement table — settlement data embedded in transactions
- **Bank receipts** can only be matched by date (UTR systems differ between PGs and bank)

Full details in `context/GATEWAY_QUIRKS.md`.

## Current Status

See `docs/PROGRESS.md` for detailed tracking.

- ✅ All 20 tables loaded
- ✅ Jan 2026 end-to-end recon complete (4 layers)
- ✅ Data dictionary (916 columns)
- ✅ Project restructured for finance team use
- ⬜ Feb 2026 / Dec 2025 recon
- ⬜ Finance team handoff
