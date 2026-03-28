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

## Workflow
- Update `docs/PROGRESS.md` at every stage with completed and pending items
- Keep this file updated with new context documents and references as they are created
