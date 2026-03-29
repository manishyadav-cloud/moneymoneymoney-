# Wiom Reconciliation Tool — Architecture v2

> **Status:** PROPOSAL v2 — awaiting approval
> **Date:** 2026-03-30
> **Philosophy:** Claude Code IS the engine. We invest in documentation, not frameworks.

---

## Core Idea

```
┌──────────────────────────────────────────────────────────────────────┐
│                                                                      │
│   Finance team member talks to Claude Code:                          │
│                                                                      │
│   "Run recon for Feb 2026"                                           │
│   "Why is there a Rs 2L gap in PhonePe settlements?"                │
│   "Trace order custGen_abc123 end-to-end"                           │
│   "Export all unmatched Paytm transactions to CSV"                  │
│                                                                      │
│   Claude Code reads the docs → understands the data → writes a      │
│   script → executes it → explains the result → saves output         │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

**We don't build a Python framework. We build a knowledge base that makes Claude Code an expert reconciler.**

---

## Project Structure

```
wiom-recon/
│
├── CLAUDE.md                          # 🧠 Master brain — Claude reads this first
│                                      #    Project overview, what tables exist,
│                                      #    how to run queries, where to save output,
│                                      #    monthly workflow, known quirks
│
├── data.duckdb                        # 💾 Single database file — all raw + recon tables
│
├── db_manager.py                      # 🔧 Simple CLI: load CSV, list tables, run query
│                                      #    (Claude Code also uses this)
│
├── context/                           # 📚 THE KNOWLEDGE BASE — Claude reads these
│   ├── DATA_DICTIONARY.md             #    Every table, every column, data types, samples
│   ├── COLUMN_STATS.csv               #    Nulls, distinct counts, min/max for all 916 columns
│   ├── JOIN_KEYS.md                   #    All primary/foreign keys, join SQL, data quirks
│   ├── RECON_LOGIC.md                 #    How recon works layer-by-layer, gap categories,
│   │                                  #    formulas (bank = sett_net - refunds), MDR rates
│   ├── GATEWAY_QUIRKS.md              #    Paytm quotes, PhonePe negative fees, Razorpay
│   │                                  #    different join key, PayU ADJ rows, date formats
│   └── BASE_TABLE_SCHEMA.md           #    Schema for recon_YYYYMM_base tables
│
├── scripts/                           # 📝 All analysis/recon scripts (Claude writes here)
│   ├── _jan26_recon.py                #    (existing scripts moved here)
│   ├── _layer2_recon.py
│   ├── _bank_recon.py
│   ├── _jan26_reverse_recon.py
│   └── ...                            #    Claude creates new scripts here as needed
│
├── output/                            # 📤 All exports, CSVs, reports (Claude saves here)
│   ├── gap_juspay_no_wiom_jan26.csv
│   ├── razorpay_fee_trace_jan26.csv
│   └── ...                            #    Finance team picks up files from here
│
├── csv/                               # 📥 Source data files (finance team drops files here)
│   ├── Bank-Receipt-from-PG.xlsx
│   ├── wiom-db/
│   ├── PG-settlements/
│   └── ...
│
└── docs/                              # 📋 Progress tracking, meeting notes, etc.
    └── PROGRESS.md
```

### That's it. 4 real folders:

| Folder | Who Writes | Who Reads | Purpose |
|--------|-----------|-----------|---------|
| **context/** | Data team (us, now) | Claude Code (every session) | All knowledge about the data |
| **scripts/** | Claude Code (at runtime) | Claude Code / Finance team | Analysis scripts, recon runs |
| **output/** | Claude Code (at runtime) | Finance team | CSVs, reports, exports |
| **csv/** | Finance team (monthly) | Claude Code / db_manager.py | Raw source data files |

---

## What Goes in `context/` (The Critical Folder)

### `DATA_DICTIONARY.md`
Every table, every column, in human-readable format:

```
## paytm_settlements (927,025 rows)
Settlement records from Paytm payment gateway.

| Column | Type | Description | Sample | Notes |
|--------|------|-------------|--------|-------|
| order_id | VARCHAR | Paytm order identifier | 'custGen_abc123' | ⚠️ Has embedded single quotes |
| settled_amount | DOUBLE | Net amount after fees | 185.00 | = amount - commission - gst |
| settled_date | DATE | When Paytm settled to bank | 2026-01-15 | 1 settlement batch per day |
| transaction_type | VARCHAR | ACQUIRING / REFUND | ACQUIRING | ⚠️ Forward payments = ACQUIRING not SALE |
| utr_no | VARCHAR | Paytm UTR (not same as bank UTR) | UTIBR620260115... | Cannot join to bank RTGS UTR |
...
```

### `JOIN_KEYS.md`
Evolved from current `RECON_KEYS_AND_JOINS.md` — every join condition with tested SQL:

```
## Layer 2: Juspay → Paytm
- Juspay key: juspay_transactions.juspay_txn_id
- Paytm key: REPLACE(paytm_transactions.Order_ID, chr(39), '')
- Filter: j.payment_gateway = 'PAYTM_V2' AND j.order_status = 'CHARGED'
- Match rate (Jan26): 100% (314,833 / 314,833)
- Known issue: Paytm Order_ID has literal single quotes embedded
```

### `RECON_LOGIC.md`
The reconciliation playbook — how to think about the recon:

```
## Gap Categories
| Category | Meaning | Expected? | Action |
|----------|---------|-----------|--------|
| FULLY_TRACED | All 4 layers matched | ✅ Yes | None |
| REFUNDED | In wiom_refunded_transactions | ✅ Yes | None |
| MISSING_WIOM | In PG + Juspay, not in Wiom DB | ⚠️ No | Investigate |
...

## Layer 4 Formula
Bank deposit = Settlement net - Refund deductions (Paytm nets refunds before RTGS)
Residual gap ~0.5-0.7% = likely TDS + platform charges

## Monthly Recon Process
1. Filter settlements by settled_date in target month
2. Join to Juspay (Layer 2 keys)
3. Join to Wiom DB (Layer 1 keys, route by order_id prefix)
4. Compare settlement net to bank deposit by date (Layer 4)
5. Classify every row into gap categories
6. Build recon_YYYYMM_base table
```

### `GATEWAY_QUIRKS.md`
All the landmines, in one place:

```
## Paytm
- Order_ID has embedded single quotes: REPLACE(Order_ID, chr(39), '')
- transaction_type = 'ACQUIRING' for forward payments (NOT 'SALE')
- Refunds Settled_Date is VARCHAR: CAST(LEFT(TRIM(Settled_Date, chr(39)), 10) AS DATE)
- 1 RTGS transfer per day, UTR format: UTIBR6YYYYMMDD...

## PhonePe
- Total Fees stored as NEGATIVE: -4677 means Rs 4,677 charged
- Settlement Date is TIMESTAMP, cast to DATE for matching
- Standard UPI = 0% MDR (RBI mandate)

## Razorpay
- Uses order_id (NOT juspay_txn_id) to join Juspay: order_receipt = order_id
- Settlement embedded in razorpay_transactions (no separate settlement table)
- Charges MDR on ALL methods including UPI (0.46% UPI, ~1.9% cards)
- settled_at is DATE type (not VARCHAR)

## PayU
- ADJ_* rows in settlements = platform fee debits, NOT customer transactions
- AddedOn column is VARCHAR, cast carefully
- Refunds show in settlements with status='Refunded'
```

### `BASE_TABLE_SCHEMA.md`
Standard schema for every monthly recon table:

```sql
-- recon_YYYYMM_base — one row per settlement transaction
-- Built by reverse recon: Bank → Settlement → Juspay → Wiom DB
CREATE TABLE recon_YYYYMM_base (
    gateway               VARCHAR,     -- PAYTM_V2 / PHONEPE / PAYU / RAZORPAY
    settlement_order_id   VARCHAR,     -- Cleaned order ID
    settled_date          DATE,
    sett_gross            DOUBLE,      -- Gross transaction amount
    sett_net              DOUBLE,      -- Net after PG fees
    sett_fee              DOUBLE,      -- MDR + GST
    sett_utr              VARCHAR,
    juspay_order_id       VARCHAR,     -- = Wiom txn ID
    juspay_amount         DOUBLE,
    juspay_status         VARCHAR,
    juspay_created_date   DATE,
    wiom_txn_id           VARCHAR,
    wiom_table            VARCHAR,     -- Which Wiom table matched
    wiom_amount           DOUBLE,
    order_id_prefix       VARCHAR,     -- custGen / w / cusSubs / wiomWall / mr
    trace_status          VARCHAR,     -- FULLY_TRACED / REFUNDED / MISSING_WIOM / etc.
    source_month          VARCHAR,     -- YYYY-MM of original transaction
    bank_date             DATE,
    bank_daily_deposit    DOUBLE
);
```

---

## How a Typical Session Works

### Finance person: "Run recon for Feb 2026"

Claude Code:
1. Reads `CLAUDE.md` → knows project structure
2. Reads `context/JOIN_KEYS.md` → knows all join conditions
3. Reads `context/GATEWAY_QUIRKS.md` → knows the data traps
4. Reads `context/RECON_LOGIC.md` → knows the recon process
5. Writes `scripts/recon_feb26.py` with parameterized SQL
6. Runs the script → builds `recon_202602_base` in DuckDB
7. Prints summary to console
8. Saves gap CSVs to `output/`
9. Explains the results in plain English

### Finance person: "Why is Paytm gap higher this month?"

Claude Code:
1. Queries `recon_202602_base` directly
2. Compares to `recon_202601_base`
3. Drills into the specific dates with high gaps
4. Checks refund table for unusual activity
5. Explains: "Feb 15 had a Rs 1.2L refund batch that Paytm deducted from the settlement..."

### Finance person: "Export all MISSING_WIOM rows for Paytm"

Claude Code:
```sql
COPY (SELECT * FROM recon_202602_base
      WHERE trace_status = 'MISSING_WIOM' AND gateway = 'PAYTM_V2')
TO 'output/missing_wiom_paytm_feb26.csv';
```

---

## What We Need to Build (in order)

| Step | Task | Output |
|------|------|--------|
| 1 | Create `context/` folder with all 5 docs | Knowledge base ready |
| 2 | Move existing scripts to `scripts/`, exports to `output/` | Clean structure |
| 3 | Rewrite `CLAUDE.md` for finance team | User manual ready |
| 4 | Test: run a recon from scratch using only CLAUDE.md + context/ | Validates the approach |
| 5 | Handoff: git repo + data.duckdb + instructions | Finance team starts using |

**Step 1 is 80% of the work. The context docs ARE the product.**

---

## Why This Is Better Than a Framework

| Framework Approach | Context + Claude Code Approach |
|---|---|
| Rigid — new gateway needs code changes | Flexible — add gateway info to docs, Claude adapts |
| Breaks when CSV format changes | Claude reads the error, reads the docs, fixes the script |
| Finance team needs to learn CLI commands | Finance team talks in English |
| Maintenance burden on dev team | Self-maintaining — Claude reads updated docs |
| One-size-fits-all reports | Claude generates exactly what's asked |
| Hard to debug edge cases | "Why is this row unmatched?" → Claude investigates interactively |

---

## Open Questions

| # | Question |
|---|----------|
| 1 | **Folder naming** — `context/` or `reference/` or `knowledge/`? |
| 2 | **Data loading** — keep `db_manager.py` as-is, or enhance with schema validation? |
| 3 | **Base table naming** — `recon_202601_base` or `recon_jan26_base`? |
| 4 | **Historical scripts** — move to `scripts/archive/` or keep flat in `scripts/`? |

---

*The power isn't in the code. It's in the documentation. Claude Code turns good docs into an expert analyst.*
