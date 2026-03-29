# Wiom Payment Reconciliation Tool

## What This Project Does
Reconciles every rupee flowing from customer payments through to Wiom's bank account.
Traces money across 5 systems: **Wiom DB → Juspay → Payment Gateway → PG Settlement → Bank**.

## How It Works
You (finance team) talk to Claude Code in natural language. Claude reads this file + the `context/` docs, writes SQL/Python scripts, executes them against the DuckDB database, and gives you the answers.

---

## Project Structure

```
wiom-recon/
├── CLAUDE.md                  ← YOU ARE HERE (Claude reads this first)
├── data.duckdb                ← All data lives here (DuckDB database)
├── db_manager.py              ← Simple tool: load CSVs, list tables, run queries
│
├── context/                   ← 📚 Knowledge base (Claude reads these for every task)
│   ├── DATA_DICTIONARY.md     ←   All 20 tables, key columns, relationships
│   ├── COLUMN_STATS.csv       ←   916 columns: nulls, distinct counts, samples
│   ├── DATA_DICTIONARY_DRAFT.csv ← Full column-level definitions
│   ├── JOIN_KEYS.md           ←   Every join condition across all 4 layers
│   ├── RECON_LOGIC.md         ←   How recon works, gap categories, formulas
│   ├── GATEWAY_QUIRKS.md      ←   ⚠️ Data traps per gateway (MUST read before queries)
│   └── BASE_TABLE_SCHEMA.md   ←   Schema for monthly recon tables
│
├── scripts/                   ← Claude writes & runs analysis scripts here
│   └── archive/               ←   Old exploration scripts (Jan26 recon, deep-dives)
│
├── output/                    ← All exports: CSVs, reports, gap files
│
├── csv/                       ← Source data files (drop new month's files here)
│
└── docs/                      ← Progress tracking, architecture docs
    ├── PROGRESS.md
    ├── ARCHITECTURE.md
    └── RECON_JAN26_COMPREHENSIVE.md
```

---

## Rules for Claude Code (MUST FOLLOW)

### Before Writing ANY Query
1. **Read `context/GATEWAY_QUIRKS.md`** — contains critical data traps that will silently break queries
2. **Read `context/JOIN_KEYS.md`** — has the correct join conditions (they differ per gateway)
3. **Check the table/column name** in `context/DATA_DICTIONARY.md` — column names have spaces and quotes

### When Running Analysis
1. **Write scripts to `scripts/` folder** — never write to root or context/
2. **Save CSV/XLSX exports to `output/` folder**
3. **Use DuckDB SQL** via `python db_manager.py query "SELECT ..."` or by importing duckdb in scripts
4. **Always test queries on small samples first** before running on full dataset

### Critical Data Quirks (Quick Reference)
- **Paytm Order_ID:** Has literal single quotes → `REPLACE(Order_ID, chr(39), '')`
- **Paytm forward payments:** `transaction_type = 'ACQUIRING'` (NOT 'SALE')
- **PhonePe fees:** Stored as NEGATIVE numbers → use `ABS()` when summing
- **Razorpay join key:** Uses `juspay.order_id = razorpay.order_receipt` (NOT juspay_txn_id like others)
- **Razorpay settlement:** No separate table — embedded in `razorpay_transactions`
- **PayU ADJ_* rows:** Platform fee debits, NOT customer transactions — exclude from recon
- **Bank receipts:** Date-level matching ONLY (no UTR join possible)
- **Razorpay settled_at:** DATE type — do NOT use LIKE or VARCHAR comparison

### When Updating Project
1. If context docs need updating (new findings, corrections), update files in `context/`
2. Track progress in `docs/PROGRESS.md`
3. Keep this CLAUDE.md current with project structure changes

---

## Database: 20 Tables in data.duckdb

| Group | Tables | Purpose |
|-------|--------|---------|
| **A: Wiom Internal (8)** | wiom_booking_transactions, wiom_primary_revenue, wiom_net_income, wiom_topup_income, wiom_mobile_recharge_transactions, wiom_refunded_transactions, wiom_customer_security_deposit, wiom_ott_transactions | Source records from Wiom's app |
| **B: PG Transactions (4)** | paytm_transactions, phonepe_transactions, payu_transactions, razorpay_transactions | Payment gateway transaction records |
| **C: PG Settlements (3)** | paytm_settlements, phonepe_settlements, payu_settlements | Settlement batch records (Razorpay: embedded in B) |
| **D: PG Refunds (2)** | paytm_refunds, phonepe_refunds | Refund records (PayU: in settlements, Razorpay: in B) |
| **E: Bank & Juspay (3)** | juspay_transactions, juspay_refunds, bank_receipt_from_pg | Orchestrator + bank deposits |

**Derived table:** `recon_jan26_base` (382,253 rows) — Jan 2026 end-to-end recon base table.

## Reconciliation Layers (Summary)

```
Layer 1: Wiom DB    ↔ Juspay         (order_id prefix routing)
Layer 2: Juspay     ↔ PG Txn Tables  (juspay_txn_id → PG key; Razorpay uses order_id)
Layer 3: PG Txn     ↔ PG Settlement  (order_id within gateway)
Layer 4: PG Settle  ↔ Bank Receipt   (date-level match, 1 deposit/day/gateway)
```

**Detailed join conditions:** See `context/JOIN_KEYS.md`

## Bank Receipt Gateway Mapping
- `01 Paytm-Wallet (WIOM Gold)` — 357 rows, Rs 63.2Cr (Apr25–Mar26)
- `02 Payu-Wallet` — 263 rows, Rs 8.23Cr (Apr25–Mar26)
- `05 PhonePe Wallet-2` — 240 rows, Rs 4.03Cr (May25–Mar26)
- `06 Razorpay Wallet` — 238 rows, Rs 4.04Cr (Apr25–Mar26)

## Known Data Gaps (Jan 2026 Baseline)
- 2,617 custGen_*/cusSubs_* Juspay SUCCESS txns absent from Wiom DB export (Rs 11.87L) — needs verification
- 3,367 MISSING_WIOM rows are in `wiom_refunded_transactions` — correctly refunded, not lost
- Paytm bank residual gap ~0.6%/month — likely TDS deductions
- Full details: `docs/RECON_JAN26_COMPREHENSIVE.md`

---

## Monthly Workflow (For Finance Team)

### 1. Load New Month's Data
Drop CSVs/XLSX in `csv/` folder, then ask Claude:
```
"Load March 2026 Paytm settlements from csv/PG-settlements/paytm-mar26.csv"
"Validate the loaded data"
```

### 2. Run Reconciliation
```
"Run full reconciliation for March 2026"
"Build the recon_mar26_base table"
```

### 3. Review Results
```
"Show me the gap report for March 2026"
"What percentage of transactions are fully traced?"
"Export MISSING_WIOM transactions to CSV"
```

### 4. Investigate Specific Issues
```
"Trace order custGen_abc123 end-to-end"
"Why is the Paytm bank gap higher in March?"
"Show me all Razorpay transactions with fee > Rs 100"
```

### 5. Monthly Summary
```
"Generate monthly summary: match rates, total fees, gap comparison vs last month"
```
