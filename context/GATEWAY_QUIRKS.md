# Gateway-Specific Data Quirks

> **Purpose:** Every known data trap, format oddity, and special handling rule per gateway.
> Claude Code MUST read this before writing any query involving PG data.

---

## Paytm

### 1. Order_ID has embedded single quotes
```sql
-- Stored as: 'custGen_abc123' (with literal ' characters)
-- ALWAYS strip:
REPLACE(Order_ID, chr(39), '')
-- Applies to: paytm_transactions.Order_ID, paytm_settlements.order_id, paytm_refunds.Order_ID
```

### 2. Forward payments = ACQUIRING (not SALE)
```sql
-- paytm_settlements.transaction_type values:
--   'ACQUIRING' = forward payment (customer → merchant)  ← USE THIS
--   'REFUND' = refund row
-- Common mistake: filtering by 'SALE' returns 0 rows
WHERE transaction_type = 'ACQUIRING'
```

### 3. Refunds Settled_Date is messy VARCHAR
```sql
-- paytm_refunds.Settled_Date stored as: '2025-12-02 16:24:17' (VARCHAR with quotes + timestamp)
-- To extract date:
CAST(LEFT(TRIM(Settled_Date, chr(39)), 10) AS DATE)
```

### 4. One RTGS transfer per day
- Paytm sends 1 settlement batch per day via RTGS
- UTR format: `UTIBR6YYYYMMDD...` (Paytm-issued, NOT the bank RTGS UTR)
- Bank RTGS UTR: `UTIBHYYMMDD...` (bank-assigned)
- **Cannot join by UTR — use date-level matching only**

### 5. Paytm nets refunds before bank transfer
```
Bank deposit = Settlement net − Refund deductions for that day
```
- Refunds appear in `paytm_refunds` table with `Settled_Date` = date deducted
- This explains why bank deposit < settlement net on most days

### 6. Settlement amount columns
| Column | Meaning |
|--------|---------|
| `amount` | Gross transaction amount |
| `commission` | MDR fee charged |
| `gst` | GST on MDR |
| `settled_amount` | Net = amount - commission - gst |
| `acquiring_fee` | Alternative fee column (= commission) |
| `acquiring_tax` | Alternative tax column (= gst) |

---

## PhonePe

### 1. Total Fees stored as NEGATIVE numbers
```sql
-- PhonePe sign convention: negative = fee charged to merchant
-- Total Fees = -4677.00 means Rs 4,677 MDR charged
-- ALWAYS use ABS() when summing:
ABS(SUM("Total Fees")) AS total_mdr
-- Or negate: -SUM("Total Fees")
```

### 2. Settlement Date is TIMESTAMP
```sql
-- Cast to DATE for bank matching:
CAST("Settlement Date" AS DATE) AS sett_date
```

### 3. UPI = 0% MDR (RBI mandate)
- Standard UPI transactions: 0% MDR (RBI regulation)
- Non-zero fees only on: Bank Account transfers, RuPay Credit via UPI
- 7,929 of ~16K Jan26 txns had non-zero fees

### 4. Settlement types in phonepe_settlements
```sql
-- Transaction Type values:
--   'PAYMENT' = forward payment
--   'REVERSAL' = refund/reversal
--   'REFUND' = refund
-- For forward recon: exclude REVERSAL and REFUND
WHERE "Transaction Type" NOT LIKE '%REVERSAL%'
  AND "Transaction Type" NOT LIKE '%REFUND%'
```

### 5. Settlement amount columns
| Column | Meaning |
|--------|---------|
| `"Transaction Amount"` | Gross amount |
| `"Total Fees"` | MDR (⚠️ NEGATIVE number) |
| `"Total Tax"` | Tax on MDR (also negative) |
| `"Net Amount"` | Net = gross + fees + tax (since fees are negative) |

---

## PayU

### 1. ADJ_* rows are NOT customer transactions
```sql
-- payu_settlements has rows where "Merchant Txn ID" starts with 'ADJ_'
-- These are PayU platform-level fee/MDR debit adjustments
-- They will NEVER match Juspay — this is BY DESIGN, not a gap
-- Filter them out for transaction recon:
WHERE "Merchant Txn ID" NOT LIKE 'ADJ_%'
```

### 2. AddedOn is VARCHAR
```sql
-- Settlement date column = "AddedOn" (VARCHAR type, not DATE/TIMESTAMP)
-- Cast carefully:
CAST(LEFT(CAST("AddedOn" AS VARCHAR), 10) AS DATE) AS sett_date
-- Some rows may have timestamp format: '2026-01-15 14:30:00'
```

### 3. Refunds in settlements table
```sql
-- PayU has no separate refund table
-- Refunded transactions appear in payu_settlements with:
--   Status = 'Refunded' or 'Chargebacked'
-- The Amount column shows post-refund value
```

### 4. Settlement amount columns
| Column | Meaning |
|--------|---------|
| `"Amount"` | Gross amount |
| `"Total Processing fees"` | MDR |
| `"Service Tax"` | Tax on MDR |
| `"Net Amount"` | Net after fees |
| `"Amount(Net)"` | Alternative net column |

---

## Razorpay

### 1. Uses order_id NOT juspay_txn_id to join Juspay
```sql
-- ALL other gateways: juspay.juspay_txn_id = pg.order_key
-- Razorpay ONLY:      juspay.order_id       = razorpay.order_receipt
-- This is the #1 mistake in Razorpay queries
```

### 2. NO separate settlement table
```sql
-- Settlement data lives IN razorpay_transactions:
--   settlement_id, settled_at, settlement_utr = settlement info
--   fee, tax = PG fees
--   amount - fee - tax = net settled
-- No razorpay_settlements table exists
```

### 3. Charges MDR on ALL payment methods including UPI
```
UPI:          0.4566% MDR  (unlike PhonePe which is 0%)
Visa credit:  ~1.89%
MC credit:    ~1.85%
Netbanking:   ~1.95%
Blended:      0.4944%
```

### 4. settled_at is DATE type
```sql
-- Use direct date comparison (not LIKE or VARCHAR cast):
WHERE settled_at >= '2026-01-01' AND settled_at < '2026-02-01'
-- NOT: WHERE CAST(settled_at AS VARCHAR) LIKE '2026-01%'  ← THIS ERRORS
```

### 5. Transaction type column
```sql
-- razorpay_transactions.type values:
--   'payment' = forward payment
--   'refund' = refund
--   'adjustment' = Razorpay internal adjustment
-- For forward recon: WHERE type = 'payment'
```

### 6. Amount columns (all in same table)
| Column | Meaning |
|--------|---------|
| `amount` | Gross (in paise? No — in rupees for this dataset) |
| `fee` | MDR charged |
| `tax` | GST on MDR |
| `amount - fee - tax` | Net settled |
| `credit` | Credit to merchant account |
| `debit` | Debit from merchant account (refunds) |

---

## Bank Receipts (bank_receipt_from_pg)

### 1. Four gateway filters
```sql
WHERE "Payment Gateway" = '01 Paytm-Wallet (WIOM Gold)'   -- 357 rows, Rs 63.2Cr
WHERE "Payment Gateway" = '02 Payu-Wallet'                 -- 263 rows, Rs 8.23Cr
WHERE "Payment Gateway" = '05 PhonePe Wallet-2'            -- 240 rows, Rs 4.03Cr
WHERE "Payment Gateway" = '06 Razorpay Wallet'             -- 238 rows, Rs 4.04Cr
```

### 2. Transaction column is TIMESTAMP, cast to DATE
```sql
CAST("Transaction" AS DATE) AS bank_date
```

### 3. 1 deposit per day per gateway
- Confirmed: each gateway makes exactly 1 deposit per business day
- Enables clean 1:1 date-level matching with settlement aggregates

### 4. No UTR-level join possible
- Bank RTGS UTRs and PG settlement UTRs use different numbering systems
- **Date-level matching is the only reliable join method**

### 5. Date range
- Bank data: Apr 2025 – Mar 2026 (full financial year)
- PG settlement data: Dec 2025 – Feb 2026 only
- Apr25–Nov25: bank deposits exist but no PG settlement data to match against

---

*Updated: 2026-03-30 | Source: Jan26 end-to-end reconciliation findings*
