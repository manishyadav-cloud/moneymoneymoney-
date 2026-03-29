# Recon Base Table Schema

> **Purpose:** Standard schema for the monthly reconciliation base table.
> One table per month: `recon_jan26_base`, `recon_feb26_base`, etc.
> Each row = one settlement transaction, traced from bank back to Wiom DB.

---

## Naming Convention

```
recon_{mon}{yy}_base
Examples: recon_jan26_base, recon_feb26_base, recon_dec25_base
```

---

## Schema

```sql
CREATE TABLE recon_{mon}{yy}_base (

    -- === IDENTITY ===
    gateway               VARCHAR NOT NULL,     -- PAYTM_V2 / PHONEPE / PAYU / RAZORPAY
    settlement_order_id   VARCHAR NOT NULL,     -- Order ID in settlement table (Paytm: quotes stripped)

    -- === LAYER 4: BANK RECEIPT ===
    bank_date             DATE,                 -- Matched bank deposit date (= settled_date)
    bank_daily_deposit    DOUBLE,               -- Total deposit on this date for this gateway
                                                -- NULL if no bank record for that date

    -- === LAYER 3: PG SETTLEMENT ===
    settled_date          DATE,                 -- When PG settled to bank
    sett_gross            DOUBLE,               -- Gross transaction amount
    sett_net              DOUBLE,               -- Net after PG fees (MDR + GST)
    sett_fee              DOUBLE,               -- Total fees (MDR + tax)
    sett_utr              VARCHAR,              -- Settlement batch UTR
    sett_txn_type         VARCHAR,              -- ACQUIRING (Paytm) / PAYMENT (PhonePe) / etc.

    -- === LAYER 2: JUSPAY ===
    juspay_order_id       VARCHAR,              -- juspay.order_id = Wiom txn ID
                                                -- NULL if settlement has no Juspay match
    juspay_txn_id         VARCHAR,              -- juspay.juspay_txn_id (used for PG joins except Razorpay)
    juspay_amount         DOUBLE,               -- Amount in Juspay
    juspay_status         VARCHAR,              -- order_status: CHARGED / SUCCESS / etc.
    juspay_gateway        VARCHAR,              -- payment_gateway in Juspay
    juspay_created_date   DATE,                 -- When customer initiated the payment
    l2_match              VARCHAR,              -- 'MATCHED' = found in Juspay
                                                -- 'SETT_ONLY' = not in Juspay

    -- === LAYER 1: WIOM DB ===
    wiom_txn_id           VARCHAR,              -- Transaction ID in the matched Wiom table
                                                -- NULL if not found in any Wiom table
    wiom_table            VARCHAR,              -- Which table matched:
                                                --   booking_transactions / primary_revenue /
                                                --   net_income / topup_income /
                                                --   mobile_recharge / refunded_transactions /
                                                --   customer_security_deposit / ott_transactions
    wiom_amount           DOUBLE,               -- Amount in Wiom DB
    l1_match              VARCHAR,              -- 'MATCHED' = found in a Wiom table
                                                -- 'JUSPAY_ONLY' = in Juspay but not Wiom
                                                -- 'NO_JUSPAY' = not in Juspay either
    order_id_prefix       VARCHAR,              -- Extracted prefix: custGen / w / cusSubs /
                                                --   wiomWall / mr / sd / cxTeam / custWgSubs

    -- === CLASSIFICATION ===
    trace_status          VARCHAR NOT NULL,      -- Final classification:
                                                --   FULLY_TRACED     = matched all layers
                                                --   REFUNDED         = in wiom_refunded_transactions
                                                --   WALLET_TOPUP     = wiomWall_ in wiom_topup_income
                                                --   MISSING_WIOM     = in Juspay, not in Wiom DB
                                                --   MISSING_JUSPAY   = in settlement, not in Juspay
                                                --   PG_ADJUSTMENT    = PayU ADJ_* or Paytm w-* rows

    -- === DERIVED / DIAGNOSTICS ===
    diff_juspay_vs_sett   DOUBLE,               -- juspay_amount - sett_gross (should be ~0)
    diff_wiom_vs_juspay   DOUBLE,               -- wiom_amount - juspay_amount (should be ~0)
    source_month          VARCHAR                -- YYYY-MM of juspay_created_date
                                                -- May differ from settled_date month (Dec25 → Jan26)
);
```

---

## Useful Queries on Base Table

### Overall summary
```sql
SELECT trace_status, COUNT(*) AS rows,
       SUM(sett_gross) AS gross, SUM(sett_net) AS net
FROM recon_jan26_base
GROUP BY 1 ORDER BY rows DESC;
```

### Gateway breakdown
```sql
SELECT gateway, trace_status, COUNT(*) AS rows, SUM(sett_gross) AS gross
FROM recon_jan26_base
GROUP BY 1, 2 ORDER BY 1, 2;
```

### Amount waterfall
```sql
SELECT gateway,
       SUM(sett_gross) AS sett_gross,
       SUM(sett_net) AS sett_net,
       SUM(sett_fee) AS total_fees,
       SUM(juspay_amount) AS juspay_total,
       SUM(wiom_amount) AS wiom_total
FROM recon_jan26_base
WHERE trace_status = 'FULLY_TRACED'
GROUP BY 1;
```

### Gap drill-down
```sql
-- What's missing from Wiom DB?
SELECT order_id_prefix, gateway, COUNT(*) AS rows, SUM(sett_gross) AS amount
FROM recon_jan26_base
WHERE trace_status = 'MISSING_WIOM'
GROUP BY 1, 2 ORDER BY amount DESC;
```

### Trace a single transaction
```sql
SELECT * FROM recon_jan26_base
WHERE settlement_order_id = 'custGen_abc123'
   OR juspay_order_id = 'custGen_abc123';
```

### Compare two months
```sql
SELECT 'Jan26' AS month, trace_status, COUNT(*) AS rows, SUM(sett_gross) AS gross
FROM recon_jan26_base GROUP BY 1, 2
UNION ALL
SELECT 'Feb26', trace_status, COUNT(*), SUM(sett_gross)
FROM recon_feb26_base GROUP BY 1, 2
ORDER BY 1, 2;
```

### Daily bank gap
```sql
SELECT settled_date, gateway,
       SUM(sett_net) AS sett_net,
       MAX(bank_daily_deposit) AS bank_deposit,
       MAX(bank_daily_deposit) - SUM(sett_net) AS gap
FROM recon_jan26_base
WHERE bank_date IS NOT NULL
GROUP BY 1, 2
HAVING ABS(gap) > 1000
ORDER BY 1, 2;
```

---

## How to Build the Base Table

The script `scripts/archive/_jan26_reverse_recon.py` contains the reference implementation.

**High-level steps:**
1. Query each gateway's settlement table for the target month
2. LEFT JOIN to `juspay_transactions` (per-gateway join key)
3. Extract `order_id_prefix` from `juspay_order_id`
4. LEFT JOIN to the appropriate Wiom table based on prefix
5. LEFT JOIN to `wiom_refunded_transactions` as fallback
6. LEFT JOIN to `bank_receipt_from_pg` by date + gateway filter
7. Classify `trace_status` using CASE logic
8. INSERT INTO `recon_{mon}{yy}_base`

---

*Updated: 2026-03-30*
