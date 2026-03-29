# Data Dictionary

> **Purpose:** Every table in `data.duckdb` — what it is, key columns, and row counts.
> **Full column-level detail:** See `context/COLUMN_STATS.csv` and `context/DATA_DICTIONARY_DRAFT.csv` for all 916 columns with nulls, distinct counts, and sample values.

---

## Database Overview

**20 tables** organized in 5 groups across the payment chain:

| Group | Tables | Role in Money Flow |
|-------|--------|--------------------|
| **A: Wiom Internal** | 8 tables | Source — where the order originates |
| **B: PG Transactions** | 4 tables | Processing — PG acknowledges the payment |
| **C: PG Settlements** | 3 tables | Batching — PG batches and sends to bank |
| **D: PG Refunds** | 2 tables | Returns — money returned to customer |
| **E: Bank & Juspay** | 3 tables | Orchestrator + final destination |

---

## Group A — Wiom Internal DB Exports (8 tables)

### wiom_booking_transactions (20,785 rows, 22 cols)
Customer WiFi session bookings. One row per booking.

| Column | Type | Key? | Description |
|--------|------|------|-------------|
| `BOOKING_TXN_ID` | VARCHAR | **PK** → joins to `juspay.order_id` | Transaction ID (e.g., `custGen_abc123`) |
| `BOOKING_FEE` | BIGINT | | Amount paid (in Rs) |
| `CREATED_ON` | VARCHAR | | Booking timestamp |
| `RESULTSTATUS` | VARCHAR | | Payment result: TXN_SUCCESS / TXN_FAILURE / NULL |
| `PAYMENT_GATEWAY_NAME` | VARCHAR | | Gateway used: PAYTM / PHONEPE / PAYU / RAZORPAY |
| `ACCOUNT_ID` | BIGINT | | Customer account ID |
| `PARTNER_ACCOUNT_ID` | BIGINT | | Partner (LCO) who owns the hotspot |
| `CITY` / `ZONE` / `PINCODE` | VARCHAR | | Location metadata |

### wiom_primary_revenue (660,895 rows, 44 cols)
Revenue records for WiFi services. Includes online + offline (partner wallet) payments.

| Column | Type | Key? | Description |
|--------|------|------|-------------|
| `TRANSACTION_ID` | VARCHAR | **PK** → joins to `juspay.order_id` | Order ID (custGen/cusSubs/custWgSubs/WIFI_SRVC) |
| `TOTALPAID` | BIGINT | | Amount collected |
| `MODE` | VARCHAR | | `online` (Juspay path) or `offline` (partner wallet) |
| `RECHARGE_DT` | VARCHAR | | Recharge date |
| `PLAN_TYPE` | VARCHAR | | Plan type taken |
| `PARTNER_NAME` / `PARTNER_ID` | VARCHAR/BIGINT | | Partner info |
| `TOTAL_COMMISSION` | DOUBLE | | Commission paid to partner |

**Note:** `WIFI_SRVC_*` and `BILL_PAID_*` prefixes in TRANSACTION_ID = partner wallet path → NOT in Juspay.

### wiom_net_income (744,685 rows, 18 cols)
Partner wallet / net income topup transactions.

| Column | Type | Key? | Description |
|--------|------|------|-------------|
| `TXN_ID` | VARCHAR | **PK** → joins to `juspay.order_id` | Transaction ID (`w_*` prefix) |
| `AMOUNT` | BIGINT | | Topup amount |
| `TXN_DT` / `TXN_TIME` | VARCHAR | | Date and time |
| `MODE` | VARCHAR | | Payment mode |
| `PARTNER_ID` / `PARTNER` | BIGINT/VARCHAR | | Partner info |

### wiom_topup_income (625,184 rows, 10 cols)
Wallet top-up transactions (partner wallet funding).

| Column | Type | Key? | Description |
|--------|------|------|-------------|
| `TRANSACTION_ID` | VARCHAR | **PK** → joins to `juspay.order_id` | Transaction ID (`wiomWall_*` prefix for PG path) |
| `AMOUNT` | DOUBLE | | Top-up amount |
| `ACTION` | VARCHAR | | Action type |
| `REMARK` | VARCHAR | | Description |
| `PARTNER_NAME` | VARCHAR | | Partner name |

### wiom_mobile_recharge_transactions (5,780 rows, 34 cols)
Mobile recharge orders.

| Column | Type | Key? | Description |
|--------|------|------|-------------|
| `TRANSACTION_ID` | VARCHAR | **PK** → joins to `juspay.order_id` | Order ID (`mr_*` prefix) |
| `PAY_AMMOUNT` | DOUBLE | | Amount paid |
| `TOTAL_PRICE` | DOUBLE | | Total price |
| `PAYMENT_MODE` | VARCHAR | | Payment mode |

### wiom_refunded_transactions (18,189 rows, 34 cols)
Transactions that were refunded. **Universal fallback table** — any order_id prefix may appear here.

| Column | Type | Key? | Description |
|--------|------|------|-------------|
| `TRANSACTION_ID` | VARCHAR | **PK** → joins to `juspay.order_id` | Order ID (any prefix) |
| `PAY_AMMOUNT` | BIGINT | | Amount that was refunded |
| `REFUND_STATUS` | BIGINT | | 1 = refunded |
| `DT` | DATE | | Transaction date |

### wiom_booking_transactions, wiom_customer_security_deposit (1,501 rows), wiom_ott_transactions (381 rows)
Smaller tables — security deposits (`sd_*` prefix) and OTT orders (`cxTeam_*` prefix).

---

## Group B — PG Transaction Tables (4 tables)

### paytm_transactions (907,342 rows, 124 cols)
Raw transaction records from Paytm.

| Column | Type | Key? | Description |
|--------|------|------|-------------|
| `Order_ID` | VARCHAR | **PK** ⚠️ has embedded `'` quotes | Paytm order ID = Juspay's juspay_txn_id |
| `Amount` | DOUBLE | | Transaction amount |
| `Status` | VARCHAR | | TXN_SUCCESS / TXN_FAILURE |
| `Transaction_Date` | VARCHAR | | When transaction happened |
| `Payment_Mode` | VARCHAR | | UPI / Debit Card / Credit Card / Net Banking |
| `Settled_Amount` | DOUBLE | | Amount settled after fees |
| `Commission` | DOUBLE | | MDR fee |
| `GST` | DOUBLE | | GST on MDR |

### phonepe_transactions (120,926 rows, 54 cols)
Raw transaction records from PhonePe.

| Column | Type | Key? | Description |
|--------|------|------|-------------|
| `"Merchant Order Id"` | VARCHAR | **PK** → = Juspay's juspay_txn_id | PhonePe merchant order ID |
| `"Transaction Amount"` | BIGINT | | Amount in rupees |
| `"Transaction Status"` | VARCHAR | | PAYMENT_SUCCESS / PAYMENT_ERROR |
| `"Transaction Date"` | TIMESTAMP | | Transaction timestamp |
| `Instrument` | VARCHAR | | UPI / CARD etc. |

### payu_transactions (295,095 rows, 85 cols)
Raw transaction records from PayU.

| Column | Type | Key? | Description |
|--------|------|------|-------------|
| `txnid` | VARCHAR | **PK** → = Juspay's juspay_txn_id | PayU transaction ID |
| `amount` | BIGINT | | Transaction amount |
| `status` | VARCHAR | | success / failure |
| `mode` | VARCHAR | | UPI / CC / DC / NB |
| `addedon` | VARCHAR | | Transaction timestamp |

### razorpay_transactions (51,835 rows, 27 cols)
Transaction + settlement records from Razorpay (combined in one table).

| Column | Type | Key? | Description |
|--------|------|------|-------------|
| `order_receipt` | VARCHAR | **PK** → = Juspay's **order_id** (NOT juspay_txn_id!) | Razorpay order receipt |
| `amount` | DOUBLE | | Gross amount |
| `fee` | DOUBLE | | MDR fee |
| `tax` | DOUBLE | | GST on MDR |
| `type` | VARCHAR | | `payment` / `refund` / `adjustment` |
| `settled_at` | DATE | | Settlement date |
| `settlement_id` | VARCHAR | | Settlement batch ID |
| `settlement_utr` | VARCHAR | | Settlement UTR |
| `method` | VARCHAR | | upi / card / netbanking |
| `payment_id` | VARCHAR | | Razorpay payment ID |

---

## Group C — PG Settlement Tables (3 tables)

### paytm_settlements (927,025 rows, 101 cols)
Settlement records from Paytm. One row per settled transaction.

| Column | Type | Key? | Description |
|--------|------|------|-------------|
| `order_id` | VARCHAR | **PK** ⚠️ has embedded quotes | = paytm_transactions.Order_ID |
| `amount` | DOUBLE | | Gross transaction amount |
| `settled_amount` | DOUBLE | | Net after fees |
| `commission` | DOUBLE | | MDR fee |
| `gst` | DOUBLE | | GST on MDR |
| `settled_date` | DATE | | Settlement date |
| `utr_no` | VARCHAR | | Settlement UTR |
| `transaction_type` | VARCHAR | | `ACQUIRING` (forward) / `REFUND` |
| `payout_date` | DATE | | Bank payout date |

### phonepe_settlements (49,832 rows, 70 cols)
Settlement records from PhonePe.

| Column | Type | Key? | Description |
|--------|------|------|-------------|
| `"Merchant Order Id"` | VARCHAR | **PK** | = phonepe_transactions."Merchant Order Id" |
| `"Transaction Amount"` | DOUBLE | | Gross amount |
| `"Net Amount"` | DOUBLE | | Net after fees |
| `"Total Fees"` | DOUBLE | ⚠️ NEGATIVE | MDR (negative = fee charged) |
| `"Total Tax"` | DOUBLE | | Tax on MDR |
| `"Settlement Date"` | TIMESTAMP | | Settlement date |
| `"Settlement UTR"` | VARCHAR | | Settlement UTR |
| `"Transaction Type"` | VARCHAR | | PAYMENT / REVERSAL / REFUND |

### payu_settlements (94,663 rows, 92 cols)
Settlement records from PayU.

| Column | Type | Key? | Description |
|--------|------|------|-------------|
| `"Merchant Txn ID"` | VARCHAR | **PK** | = payu_transactions.txnid |
| `"Amount"` | DOUBLE | | Gross amount |
| `"Net Amount"` | DOUBLE | | Net after fees |
| `"Total Processing fees"` | DOUBLE | | MDR fee |
| `"Service Tax"` | DOUBLE | | Tax on MDR |
| `"AddedOn"` | VARCHAR | | Settlement date (VARCHAR — cast to DATE) |
| `"Merchant UTR"` | VARCHAR | | Settlement UTR |
| `Status` | VARCHAR | | Captured / Refunded / Chargebacked |

**Note:** Razorpay has NO separate settlement table — settlement data is embedded in `razorpay_transactions`.

---

## Group D — PG Refund Tables (2 tables)

### paytm_refunds (16,914 rows, 120 cols)
Refund records from Paytm.

| Column | Type | Key? | Description |
|--------|------|------|-------------|
| `Order_ID` | VARCHAR | **FK** ⚠️ has quotes | = paytm_transactions.Order_ID |
| `Amount` | DOUBLE | | Refund amount |
| `Settled_Date` | VARCHAR | ⚠️ messy format | Settlement date (has embedded quotes + timestamp) |
| `Status` | VARCHAR | | TXN_SUCCESS etc. |
| `Transaction_Type` | VARCHAR | | REFUND |

### phonepe_refunds (982 rows, 24 cols)
Refund records from PhonePe.

| Column | Type | Key? | Description |
|--------|------|------|-------------|
| `"Merchant Order Id"` | VARCHAR | **FK** | = phonepe_transactions."Merchant Order Id" |
| `"Total Refund Amount"` | DOUBLE | | Amount refunded |
| `"Transaction Date"` | TIMESTAMP | | Refund date |

**Note:** PayU refunds are in `payu_settlements` (status=Refunded). Razorpay refunds are in `razorpay_transactions` (type=refund).

---

## Group E — Bank Receipts & Juspay (3 tables)

### juspay_transactions (1,096,610 rows, 39 cols)
Central orchestrator table. Every online payment flows through Juspay.

| Column | Type | Key? | Description |
|--------|------|------|-------------|
| `order_id` | VARCHAR | **PK** → = Wiom txn ID | Customer order ID (custGen/w/mr/etc.) |
| `juspay_txn_id` | VARCHAR | **FK** → PG tables | Juspay's internal txn ID (used to join Paytm/PhonePe/PayU) |
| `amount` | DOUBLE | | Payment amount |
| `order_status` | VARCHAR | | CHARGED / NEW / AUTHENTICATION_FAILED / etc. |
| `payment_gateway` | VARCHAR | | PAYTM_V2 / PHONEPE / PAYU / RAZORPAY |
| `order_date_created` | VARCHAR | | Order creation timestamp |
| `payment_status` | VARCHAR | | Payment-level status |
| `source_month` | VARCHAR | | YYYY-MM derived month |

### juspay_refunds (20,179 rows, 30 cols)
Refund records from Juspay.

| Column | Type | Key? | Description |
|--------|------|------|-------------|
| `order_id` | VARCHAR | **FK** → juspay_transactions.order_id | Original order that was refunded |
| `refund_amount` | BIGINT | | Refund amount |
| `refund_status` | VARCHAR | | SUCCESS / FAILURE |
| `payment_gateway` | VARCHAR | | Which gateway processed the refund |
| `refund_unique_id` | VARCHAR | | Unique refund identifier |

### bank_receipt_from_pg (1,098 rows, 6 cols)
Bank deposits received from all 4 payment gateways. One row per deposit per day.

| Column | Type | Key? | Description |
|--------|------|------|-------------|
| `Transaction` | TIMESTAMP | **PK** (with gateway) | Deposit date |
| `"Payment Gateway"` | VARCHAR | **PK** (with date) | Gateway: `01 Paytm-Wallet (WIOM Gold)` / `02 Payu-Wallet` / `05 PhonePe Wallet-2` / `06 Razorpay Wallet` |
| `"Deposit Amt(INR)"` | DOUBLE | | Amount deposited in bank |
| `"Transaction Remarks"` | VARCHAR | | Bank narration (RTGS ref + sender name) |
| `Month` | VARCHAR | | Month label (Apr-25, Jan-26, etc.) |

**Coverage:**
- Paytm: 357 rows, Rs 63.2Cr (Apr 2025 – Mar 2026)
- PayU: 263 rows, Rs 8.23Cr (Apr 2025 – Mar 2026)
- PhonePe: 240 rows, Rs 4.03Cr (May 2025 – Mar 2026)
- Razorpay: 238 rows, Rs 4.04Cr (Apr 2025 – Mar 2026)

---

## Derived Tables (in DuckDB)

### recon_jan26_base (382,253 rows)
Monthly reconciliation base table — one row per settlement transaction for Jan 2026.
See `context/BASE_TABLE_SCHEMA.md` for full schema.

---

## Table Relationships Diagram

```
wiom_booking_transactions ──┐
wiom_primary_revenue ───────┤
wiom_net_income ────────────┤
wiom_topup_income ──────────┼──► juspay_transactions ──┬──► paytm_transactions ──► paytm_settlements ──┐
wiom_mobile_recharge ───────┤     (order_id)           ├──► phonepe_transactions ► phonepe_settlements ├──► bank_receipt_from_pg
wiom_customer_security_dep ─┤                          ├──► payu_transactions ───► payu_settlements ───┤    (date-level match)
wiom_ott_transactions ──────┤                          └──► razorpay_transactions (settlement embedded)┘
wiom_refunded_transactions ─┘                                    │
                                                     juspay_refunds ──► paytm_refunds
                                                                   ──► phonepe_refunds
```

---

*Updated: 2026-03-30 | 20 tables, 916 columns | Full column stats in COLUMN_STATS.csv*
