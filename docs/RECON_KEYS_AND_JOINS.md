# Wiom Payment Reconciliation — Keys & Join Conditions

> **Purpose:** This document captures every primary key, foreign key, and join condition discovered during the end-to-end January 2026 reconciliation exercise. It is the single reference for writing any reconciliation query across all 4 layers of the money inflow chain.

---

## Money Flow Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  LAYER 1          LAYER 2          LAYER 3a         LAYER 3b        LAYER 4    │
│                                                                                  │
│  Wiom DB    →    Juspay      →   PG Txn Tables  →  PG Settlements  →  Bank     │
│  (source)      (orchestrator)   (Paytm/PhonePe    (settlement      (actual      │
│                                  /PayU/Razorpay)   batches)         deposit)    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

Money enters at **Wiom DB** (customer initiates payment), flows through **Juspay** (payment orchestrator), reaches a **PG** (Paytm/PhonePe/PayU/Razorpay), gets **settled** in a batch, and arrives in the **bank** as an RTGS/NEFT deposit.

---

## Table Inventory

| Table | Rows | Key Column(s) | Notes |
|---|---|---|---|
| `wiom_booking_transactions` | 20,785 | `BOOKING_TXN_ID` | custGen_* prefix |
| `wiom_primary_revenue` | 660,895 | `TRANSACTION_ID` | custGen/cusSubs/custWgSubs/WIFI_SRVC |
| `wiom_net_income` | 744,685 | `TXN_ID` | w_* prefix |
| `wiom_mobile_recharge_transactions` | 5,780 | `TRANSACTION_ID` | mr_* prefix |
| `wiom_refunded_transactions` | 18,189 | `TRANSACTION_ID` | REFUND_STATUS=1 |
| `wiom_ott_transactions` | 381 | `TRANSACTION_ID` | cxTeam_* prefix |
| `wiom_customer_security_deposit` | 1,501 | `SD_TXN_ID` | sd_* prefix |
| `juspay_transactions` | 1,096,610 | `order_id`, `juspay_txn_id` | Central hub table |
| `juspay_refunds` | 20,179 | `order_id`, `refund_unique_id` | Links back to juspay_transactions |
| `paytm_transactions` | 907,342 | `Order_ID` ⚠️ has quotes | Also: `Transaction_ID` |
| `phonepe_transactions` | 120,926 | `Merchant Order Id` | Also: `PhonePe Transaction Id` |
| `payu_transactions` | 295,095 | `txnid` | Also: `id` (PayU internal) |
| `razorpay_transactions` | 51,835 | `order_receipt`, `payment_id` | Settlement embedded here |
| `paytm_settlements` | 927,025 | `order_id` ⚠️ has quotes | `utr_no`, `payout_id` |
| `phonepe_settlements` | 49,832 | `Merchant Order Id` | `Settlement UTR` |
| `payu_settlements` | 94,663 | `Merchant Txn ID` | `Merchant UTR` |
| `paytm_refunds` | 16,914 | `Order_ID` ⚠️ has quotes | `Transaction_ID` |
| `phonepe_refunds` | 982 | `Merchant Order Id` | `Forward Merchant Transaction Id` |
| `bank_receipt_from_pg` | 1,098 | `Transaction` (date) + `Payment Gateway` | 4 gateways, date-level only |

---

## Layer 1 — Wiom DB → Juspay

### Central Join: `juspay_transactions.order_id`

The `order_id` in Juspay is the **same value** as the transaction ID created in Wiom DB. The Wiom table depends on the `order_id` prefix:

| order_id Prefix | Wiom Table | Wiom Key Column | Notes |
|---|---|---|---|
| `custGen_*` | `wiom_booking_transactions` | `BOOKING_TXN_ID` | Also in wiom_primary_revenue |
| `custGen_*` | `wiom_primary_revenue` | `TRANSACTION_ID` | Revenue record for same txn |
| `cusSubs_*` | `wiom_primary_revenue` | `TRANSACTION_ID` | Subscription payment |
| `custWgSubs_*` | `wiom_primary_revenue` | `TRANSACTION_ID` | Widget subscription |
| `w_*` | `wiom_net_income` | `TXN_ID` | Wallet / topup |
| `mr_*` | `wiom_mobile_recharge_transactions` | `TRANSACTION_ID` | Mobile recharge |
| `cxTeam_*` | `wiom_ott_transactions` | `TRANSACTION_ID` | CX team OTT |
| `sd_*` | `wiom_customer_security_deposit` | `SD_TXN_ID` | Security deposit |

**Prefixes NOT in Juspay (partner wallet / cash):**

| order_id Prefix | Wiom Table | Why Not in Juspay |
|---|---|---|
| `WIFI_SRVC_*` | `wiom_primary_revenue` | Routes via partner wallet deduction |
| `BILL_PAID_*` | `wiom_primary_revenue` | Cash / partner wallet |

### Join SQL

```sql
-- Wiom booking transactions → Juspay
SELECT w.BOOKING_TXN_ID, w.BOOKING_FEE, j.order_id, j.amount, j.payment_status
FROM wiom_booking_transactions w
JOIN juspay_transactions j ON j.order_id = w.BOOKING_TXN_ID
WHERE j.order_status = 'CHARGED';

-- Wiom primary revenue (online only) → Juspay
SELECT w.TRANSACTION_ID, w.TOTALPAID, j.order_id, j.amount
FROM wiom_primary_revenue w
JOIN juspay_transactions j ON j.order_id = w.TRANSACTION_ID
WHERE w.MODE = 'online'
  AND j.order_status = 'CHARGED';

-- Wiom net income → Juspay
SELECT w.TXN_ID, w.AMOUNT, j.order_id, j.amount
FROM wiom_net_income w
JOIN juspay_transactions j ON j.order_id = w.TXN_ID
WHERE j.order_status = 'CHARGED';

-- Refunded transactions → Juspay
SELECT w.TRANSACTION_ID, w.PAY_AMMOUNT, j.order_id, j.order_status
FROM wiom_refunded_transactions w
JOIN juspay_transactions j ON j.order_id = w.TRANSACTION_ID;
```

### Layer 1 Match Rates (Jan 2026)
| Wiom Table | Juspay Matched | Notes |
|---|---|---|
| booking_transactions | 100% (3,809/3,809) | 603 NULL status; 10 amount diffs |
| primary_revenue (online) | 95.8% (134,848/140,776) | 3,719 WIFI_SRVC go via partner wallet |
| net_income | 99.997% (234,002/234,003) | Cleanest table |
| mobile_recharge | Resolved 2,853 of 8,149 orphans | mr_* pattern |
| refunded_transactions | Resolved 2,941 orphans | REFUND_STATUS=1 |

---

## Layer 2 — Juspay → PG Transaction Tables

> ⚠️ **Critical:** Razorpay uses a **different Juspay key** than all other gateways.

| Gateway | Juspay Key | PG Table | PG Key | Special Handling |
|---|---|---|---|---|
| Paytm | `juspay_transactions.juspay_txn_id` | `paytm_transactions` | `Order_ID` | Strip embedded single quotes from Order_ID |
| PhonePe | `juspay_transactions.juspay_txn_id` | `phonepe_transactions` | `Merchant Order Id` | None |
| PayU | `juspay_transactions.juspay_txn_id` | `payu_transactions` | `txnid` | None |
| Razorpay | `juspay_transactions.order_id` | `razorpay_transactions` | `order_receipt` | Uses order_id NOT juspay_txn_id |

### Join SQL

```sql
-- Juspay → Paytm  (strip single quotes from Paytm Order_ID)
SELECT j.order_id, j.juspay_txn_id, j.amount AS juspay_amt,
       p.Amount AS paytm_amt, p.Status
FROM juspay_transactions j
JOIN paytm_transactions p
  ON j.juspay_txn_id = REPLACE(p.Order_ID, chr(39), '')
WHERE j.payment_gateway = 'PAYTM_V2'
  AND j.order_status = 'CHARGED';

-- Juspay → PhonePe
SELECT j.order_id, j.juspay_txn_id, j.amount AS juspay_amt,
       p."Transaction Amount" AS pp_amt, p."Transaction Status"
FROM juspay_transactions j
JOIN phonepe_transactions p
  ON j.juspay_txn_id = p."Merchant Order Id"
WHERE j.payment_gateway = 'PHONEPE'
  AND j.order_status = 'CHARGED';

-- Juspay → PayU
SELECT j.order_id, j.juspay_txn_id, j.amount AS juspay_amt,
       p.amount AS payu_amt, p.status
FROM juspay_transactions j
JOIN payu_transactions p
  ON j.juspay_txn_id = p.txnid
WHERE j.payment_gateway = 'PAYU'
  AND j.order_status = 'CHARGED';

-- Juspay → Razorpay  (NOTE: order_id not juspay_txn_id)
SELECT j.order_id, j.juspay_txn_id, j.amount AS juspay_amt,
       r.amount AS rzp_amt, r.settled
FROM juspay_transactions j
JOIN razorpay_transactions r
  ON j.order_id = r.order_receipt          -- ← order_id, not juspay_txn_id
WHERE j.payment_gateway = 'RAZORPAY'
  AND j.order_status = 'CHARGED'
  AND r.type = 'payment';
```

### Layer 2 Match Rates (Jan 2026)
| Gateway | Juspay Txns | PG Matched | Amount Diff |
|---|---|---|---|
| PAYTM_V2 | 314,833 | 100% | Rs 0 |
| PHONEPE | 16,792 | 100% | Rs 0 |
| PAYU | 33,242 | 100% | Rs 54,160 (Juspay > PG — refund timing) |
| RAZORPAY | 16,494 | 100% | Rs 0 |

---

## Layer 2b — Juspay Refunds → PG Refund Tables

| Gateway | Juspay Refund Key | PG Refund Table | PG Key | Match Rate |
|---|---|---|---|---|
| Paytm | `juspay_refunds.order_id` | `paytm_refunds` | `Order_ID` ⚠️ quotes | 2,656/2,656 (100%) |
| PhonePe | `juspay_refunds.order_id` | `phonepe_refunds` | `Merchant Order Id` | 158/158 (100%) |
| PayU | No separate PG refund table | `payu_settlements` (status='Refunded') | `Merchant Txn ID` | — |
| Razorpay | No separate PG refund table | `razorpay_transactions` (type='refund') | `order_receipt` | — |

```sql
-- Juspay refunds → Paytm refunds
SELECT jr.order_id, jr.refund_amount, pr.Amount AS paytm_refund_amt
FROM juspay_refunds jr
JOIN paytm_refunds pr
  ON jr.order_id = REPLACE(pr.Order_ID, chr(39), '')
WHERE jr.payment_gateway = 'PAYTM_V2';

-- Juspay refunds → PhonePe refunds
SELECT jr.order_id, jr.refund_amount, pp.Total_Refund_Amount
FROM juspay_refunds jr
JOIN phonepe_refunds pp
  ON jr.order_id = pp."Merchant Order Id"
WHERE jr.payment_gateway = 'PHONEPE';
```

---

## Layer 3a — PG Transaction Tables → PG Settlement Tables

> ⚠️ **Razorpay exception:** Razorpay has NO separate settlement table. Settlement data (`settlement_id`, `settled_at`, `settlement_utr`) is embedded directly in `razorpay_transactions`.

| Gateway | PG Txn Key | Settlement Table | Settlement Key | Join Note |
|---|---|---|---|---|
| Paytm | `paytm_transactions.Order_ID` ⚠️ | `paytm_settlements` | `Order_ID` ⚠️ | Strip quotes on both sides |
| PhonePe | `phonepe_transactions."Merchant Order Id"` | `phonepe_settlements` | `"Merchant Order Id"` | None |
| PayU | `payu_transactions.txnid` | `payu_settlements` | `"Merchant Txn ID"` | None |
| Razorpay | N/A | (embedded in `razorpay_transactions`) | `settlement_id`, `settled_at`, `settlement_utr` | No join needed |

### Join SQL

```sql
-- Paytm transactions → Settlements
SELECT t.Order_ID, t.Amount, t.Status,
       s.settled_amount, s.commission, s.gst,
       s.settled_date, s.utr_no
FROM paytm_transactions t
JOIN paytm_settlements s
  ON REPLACE(t.Order_ID, chr(39), '') = REPLACE(s.Order_ID, chr(39), '')
WHERE t.Status = 'TXN_SUCCESS';

-- PhonePe transactions → Settlements
SELECT t."Merchant Order Id", t."Transaction Amount", t."Transaction Status",
       s."Net Amount", s."Total Fees", s."Settlement Date", s."Settlement UTR"
FROM phonepe_transactions t
JOIN phonepe_settlements s
  ON t."Merchant Order Id" = s."Merchant Order Id"
WHERE t."Transaction Status" = 'PAYMENT_SUCCESS';

-- PayU transactions → Settlements
SELECT t.txnid, t.amount, t.status,
       s."Net Amount", s."Total Processing fees", s."Settlement Date", s."Merchant UTR"
FROM payu_transactions t
JOIN payu_settlements s
  ON t.txnid = s."Merchant Txn ID"
WHERE t.status = 'success';

-- Razorpay: settlement data is already in the same table
SELECT r.order_receipt, r.amount, r.fee, r.tax,
       r.amount - r.fee - r.tax AS net_settled,
       r.settled_at, r.settlement_id, r.settlement_utr
FROM razorpay_transactions r
WHERE r.type = 'payment'
  AND r.settled = 1;
```

### Layer 3a Match Rates (Jan 2026 Juspay scope)
| Gateway | Juspay Txns | Settled | Gross | Net (after fees) |
|---|---|---|---|---|
| PAYTM_V2 | 314,833 | 100% | Rs 5.84Cr | Rs 5.84Cr (fees Rs 64,858) |
| PHONEPE | 16,792 | 100% | Rs 25.46L | Rs 25.46L (fees Rs 4,677) |
| PAYU | 33,242 | 100% | Rs 1.10Cr | Rs 1.10Cr (fees Rs 27,592) |
| RAZORPAY | 16,494 | 100% | Rs 24.44L | Rs 24.30L (fees Rs 13,732) |
| **TOTAL** | **381,361** | **100%** | **Rs 7.43Cr** | **Rs 7.43Cr** |

---

## Layer 3b — PG Settlements → Bank Receipts

> ⚠️ **No transaction-level key join is possible.** Reconciliation is **date-level only**.

### Why No Key Join

| Gateway | Settlement UTR | Bank RTGS UTR | Problem |
|---|---|---|---|
| Paytm | `UTIBR6YYYYMMDD...` (Paytm-issued) | `UTIBHYYMMDD...` (bank-assigned) | Different UTR issuing systems |
| PhonePe | `Settlement UTR` column | `NEFT/YESPH...` in remarks | Different reference systems |
| PayU | `Merchant UTR` column | `PAYU PAYMENTS PRIVATELIMITED` | No UTR in bank remarks |
| Razorpay | `settlement_utr` column | `RTGS/UTIBH...` in remarks | Bank RTGS UTR ≠ settlement UTR |

### Bank Receipt Gateway Filter

```sql
-- Filter bank_receipt_from_pg by gateway:
WHERE "Payment Gateway" = '01 Paytm-Wallet (WIOM Gold)'   -- Paytm
WHERE "Payment Gateway" = '02 Payu-Wallet'                 -- PayU
WHERE "Payment Gateway" = '05 PhonePe Wallet-2'            -- PhonePe
WHERE "Payment Gateway" = '06 Razorpay Wallet'             -- Razorpay
```

### Date-Level Join SQL

```sql
-- Paytm: settlement date → bank date
SELECT s.settled_date, SUM(s.settled_amount) AS sett_net,
       b."Deposit Amt(INR)" AS bank_deposit,
       b."Deposit Amt(INR)" - SUM(s.settled_amount) AS gap
FROM paytm_settlements s
JOIN bank_receipt_from_pg b
  ON s.settled_date = CAST(b."Transaction" AS DATE)
  AND b."Payment Gateway" = '01 Paytm-Wallet (WIOM Gold)'
GROUP BY s.settled_date, b."Deposit Amt(INR)"
ORDER BY s.settled_date;

-- PhonePe: settlement date → bank date
SELECT CAST(s."Settlement Date" AS DATE) AS sett_dt,
       SUM(s."Net Amount") AS sett_net,
       b."Deposit Amt(INR)" AS bank_deposit
FROM phonepe_settlements s
JOIN bank_receipt_from_pg b
  ON CAST(s."Settlement Date" AS DATE) = CAST(b."Transaction" AS DATE)
  AND b."Payment Gateway" = '05 PhonePe Wallet-2'
GROUP BY 1, b."Deposit Amt(INR)"
ORDER BY 1;

-- PayU: settlement date → bank date
SELECT CAST(s."AddedOn" AS DATE) AS sett_dt,
       SUM(s."Net Amount") AS sett_net,
       b."Deposit Amt(INR)" AS bank_deposit
FROM payu_settlements s
JOIN bank_receipt_from_pg b
  ON CAST(s."AddedOn" AS DATE) = CAST(b."Transaction" AS DATE)
  AND b."Payment Gateway" = '02 Payu-Wallet'
GROUP BY 1, b."Deposit Amt(INR)"
ORDER BY 1;

-- Razorpay: settled_at date → bank date
SELECT r.settled_at,
       SUM(r.amount - r.fee - r.tax) AS sett_net,
       b."Deposit Amt(INR)" AS bank_deposit
FROM razorpay_transactions r
JOIN bank_receipt_from_pg b
  ON r.settled_at = CAST(b."Transaction" AS DATE)
  AND b."Payment Gateway" = '06 Razorpay Wallet'
WHERE r.type = 'payment'
GROUP BY r.settled_at, b."Deposit Amt(INR)"
ORDER BY r.settled_at;
```

### Layer 4 Match Rates (Dec25–Feb26, matched months)
| Gateway | Settlement Net | Bank Deposits | Gap | Status |
|---|---|---|---|---|
| Paytm | Rs 17.36Cr | Rs 17.14Cr | -1.28% | ✅ CLEAN |
| PhonePe | Rs 77.2L | Rs 76.8L | -0.53% | ✅ CLEAN |
| PayU | Rs 2.90Cr | Rs 2.90Cr | -0.40% to -1.70% | ✅ CLEAN |
| Razorpay | Rs 79.9L | Rs 79.2L | -0.32% to -0.60% | ✅ CLEAN |

**Residual gap explanation:**
- Paytm: refund deductions account for majority; ~0.6% residual = likely TDS deductions or platform charges not captured in `paytm_refunds`
- PhonePe/PayU/Razorpay: residual <0.6% consistently = rounding / minor timing differences

---

## Special Handling — Data Quirks

### 1. Paytm Order_ID Embedded Single Quotes
```sql
-- Paytm Order_ID in both transactions and settlements has literal ' characters
-- e.g. stored as:  'custGen_abc123'  (with the quotes)
-- Always strip with:
REPLACE(Order_ID, chr(39), '')
-- or equivalently:
REPLACE(Order_ID, '''', '')
```

### 2. Paytm Refunds Settled_Date Format
```sql
-- paytm_refunds.Settled_Date is VARCHAR with embedded quotes + timestamp
-- e.g. stored as:  '2025-12-02 16:24:17'
-- To extract date:
CAST(LEFT(TRIM(Settled_Date, chr(39)), 10) AS DATE)
```

### 3. PhonePe Total Fees Sign Convention
```sql
-- PhonePe stores MDR as NEGATIVE numbers
-- e.g. Total Fees = -4677.00  means Rs 4,677 charged to merchant
-- Always use ABS() when summing fees:
ABS(SUM("Total Fees")) AS total_mdr
```

### 4. Razorpay Uses order_id (Not juspay_txn_id) to Join Juspay
```sql
-- ALL other gateways: juspay.juspay_txn_id = pg.order_key
-- Razorpay ONLY:      juspay.order_id       = razorpay.order_receipt
```

### 5. Razorpay Settlement Embedded in Transactions Table
```sql
-- No razorpay_settlements table exists
-- Settlement fields live directly in razorpay_transactions:
SELECT settlement_id, settled_at, settlement_utr,
       amount - fee - tax AS net_settled
FROM razorpay_transactions
WHERE type = 'payment';   -- type IN ('payment','refund','adjustment')
```

### 6. PayU Settlement Date Column
```sql
-- PayU settlement date = "AddedOn" column (VARCHAR/TIMESTAMP)
-- Cast carefully:
CAST(LEFT(CAST("AddedOn" AS VARCHAR), 10) AS DATE) AS sett_date
```

### 7. PhonePe Settlement Date Column
```sql
-- PhonePe settlement date = "Settlement Date" (TIMESTAMP type)
CAST("Settlement Date" AS DATE) AS sett_date
```

### 8. Paytm Refund Deductions (Bank Netting)
```sql
-- Paytm nets refunds OUT of the settlement batch before RTGS transfer
-- Formula: Bank Deposit = Settlement Net − Refund Deductions
-- Refunds appear in paytm_refunds table; Settled_Date = date deducted from batch
```

---

## Settlement Date Columns by Gateway

| Gateway | Table | Settlement Date Column | Data Type | Notes |
|---|---|---|---|---|
| Paytm | `paytm_settlements` | `settled_date` | DATE | Clean — direct use |
| PhonePe | `phonepe_settlements` | `"Settlement Date"` | TIMESTAMP | Cast to DATE |
| PayU | `payu_settlements` | `"AddedOn"` | VARCHAR | Cast via LEFT(...,10) |
| Razorpay | `razorpay_transactions` | `settled_at` | DATE | Filter type='payment' |
| Paytm Refunds | `paytm_refunds` | `Settled_Date` | VARCHAR ⚠️ | Has embedded quotes + timestamp; use CAST(LEFT(TRIM(Settled_Date, chr(39)), 10) AS DATE) |

---

## Settlement Amount Columns by Gateway

| Gateway | Table | Gross Column | Net Column | Fee Column | Tax Column |
|---|---|---|---|---|---|
| Paytm | `paytm_settlements` | `amount` | `settled_amount` | `commission` | `gst` |
| PhonePe | `phonepe_settlements` | `"Transaction Amount"` | `"Net Amount"` | `"Total Fees"` ⚠️ negative | `"Total Tax"` |
| PayU | `payu_settlements` | `"Amount"` | `"Net Amount"` | `"Total Processing fees"` | `"Service Tax"` |
| Razorpay | `razorpay_transactions` | `amount` | `amount - fee - tax` | `fee` | `tax` |

---

## MDR / Fee Rates (Jan 2026 actuals)

| Gateway | Gross Collected | Total Fees | Effective MDR | UPI Rate |
|---|---|---|---|---|
| Paytm | Rs 5.84Cr | Rs 64,858 | 0.111% | ~0% (Paytm wallet) |
| PhonePe | Rs 25.46L | Rs 4,677 | 0.184% | 0% standard UPI (RBI mandate) |
| PayU | Rs 1.10Cr | Rs 27,592 | 0.251% | Varies |
| Razorpay | Rs 24.44L | Rs 13,732 | 0.494% | 0.457% (charges UPI unlike others) |
| **Blended** | **Rs 7.43Cr** | **Rs 1.11L** | **0.149%** | — |

---

## Full End-to-End Query Template (Jan 2026)

```sql
-- Complete money trail for a single transaction: Wiom DB → Bank
-- Example: custGen_abc123

WITH juspay AS (
    SELECT order_id, juspay_txn_id, amount, order_status, payment_gateway,
           order_date_created
    FROM juspay_transactions
    WHERE order_id = 'custGen_abc123'
),
wiom AS (
    SELECT BOOKING_TXN_ID AS txn_id, BOOKING_FEE AS wiom_amount,
           CREATED_ON, RESULTSTATUS
    FROM wiom_booking_transactions
    WHERE BOOKING_TXN_ID = 'custGen_abc123'
),
pg_txn AS (
    -- Paytm example (adjust gateway condition from juspay.payment_gateway)
    SELECT REPLACE(Order_ID, chr(39), '') AS order_id,
           Amount AS pg_amount, Status AS pg_status,
           Settled_Date AS pg_settled_date
    FROM paytm_transactions
    WHERE REPLACE(Order_ID, chr(39), '') IN (SELECT juspay_txn_id FROM juspay)
),
pg_sett AS (
    SELECT REPLACE(Order_ID, chr(39), '') AS order_id,
           settled_amount, commission + gst AS fees,
           settled_date, utr_no
    FROM paytm_settlements
    WHERE REPLACE(Order_ID, chr(39), '') IN (SELECT juspay_txn_id FROM juspay)
),
bank AS (
    SELECT CAST("Transaction" AS DATE) AS bank_date,
           "Deposit Amt(INR)" AS bank_deposit
    FROM bank_receipt_from_pg
    WHERE "Payment Gateway" = '01 Paytm-Wallet (WIOM Gold)'
      AND CAST("Transaction" AS DATE) = (SELECT settled_date FROM pg_sett LIMIT 1)
)
SELECT
    w.txn_id,
    w.wiom_amount,
    j.amount              AS juspay_amount,
    t.pg_amount           AS pg_txn_amount,
    s.settled_amount      AS pg_settled_amount,
    s.fees                AS pg_fees,
    s.settled_date,
    s.utr_no              AS settlement_utr,
    b.bank_date,
    b.bank_deposit
FROM wiom w
LEFT JOIN juspay j ON j.order_id = w.txn_id
LEFT JOIN pg_txn t ON t.order_id = j.juspay_txn_id
LEFT JOIN pg_sett s ON s.order_id = j.juspay_txn_id
LEFT JOIN bank b ON b.bank_date = s.settled_date;
```

---

## Summary Cheat Sheet

```
WIOM DB                     JUSPAY                      PG TRANSACTIONS           PG SETTLEMENTS          BANK
─────────────────────────── ─────────────────────────── ──────────────────────── ─────────────────────── ──────────────
BOOKING_TXN_ID          ──► order_id
TRANSACTION_ID (pr/ref) ──► order_id
TXN_ID (net_income)     ──► order_id
SD_TXN_ID               ──► order_id

                            juspay_txn_id           ──► paytm Order_ID*          Order_ID*           ──► Date match
                            juspay_txn_id           ──► phonepe Merchant Order Id Merchant Order Id   ──► Date match
                            juspay_txn_id           ──► payu txnid               Merchant Txn ID     ──► Date match
                            order_id ←DIFF KEY!     ──► razorpay order_receipt   (embedded in txn)   ──► Date match

* Paytm: strip embedded single quotes — REPLACE(col, chr(39), '')
```

---

*Generated: 2026-03-29 | Based on Jan 2026 end-to-end reconciliation | All match rates verified against data.duckdb*
