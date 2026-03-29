# -*- coding: utf-8 -*-
"""Explore settlement tables before writing Layer 3 recon."""
import duckdb
con = duckdb.connect('data.duckdb', read_only=True)

# ── PHONEPE ──────────────────────────────────────────────────
print('=== PHONEPE SETTLEMENTS ===')
r = con.execute("""
    SELECT "Merchant Order Id", "Transaction Amount", "Transaction Date",
           "Transaction Status", "Settlement Date", "Settlement UTR"
    FROM phonepe_settlements LIMIT 3
""").fetchall()
for x in r: print(' ', x)

match = con.execute("""
    SELECT COUNT(*) FROM phonepe_settlements s
    INNER JOIN phonepe_transactions t ON s."Merchant Order Id" = t."Merchant Order Id"
""").fetchone()[0]
print(f'  Settlements matching transactions: {match:,}')

date_range = con.execute("""
    SELECT MIN("Transaction Date"), MAX("Transaction Date") FROM phonepe_settlements
""").fetchone()
print(f'  Date range: {date_range}')

# ── PAYU ─────────────────────────────────────────────────────
print()
print('=== PAYU SETTLEMENTS ===')
cols = [c[0] for c in con.execute(
    "SELECT column_name FROM information_schema.columns WHERE table_name='payu_settlements' ORDER BY ordinal_position"
).fetchall()]
txn_cols = [c for c in cols if any(k in c.lower() for k in ['txn', 'merchant', 'id', 'utr', 'amount', 'date', 'status', 'settle', 'ref'])]
print('  Key cols:', txn_cols[:20])
r = con.execute('SELECT * FROM payu_settlements LIMIT 2').fetchdf()
print(r[txn_cols[:15]].to_string())

# Find PayU settlement join key — try "Merchant Txn ID"
try:
    match = con.execute("""
        SELECT COUNT(*) FROM payu_settlements s
        INNER JOIN payu_transactions t ON s."Merchant Txn ID" = t.txnid
    """).fetchone()[0]
    print(f'  Settlements matching transactions (via Merchant Txn ID): {match:,}')
except Exception as e:
    print(f'  Merchant Txn ID join failed: {e}')

# ── PAYTM ────────────────────────────────────────────────────
print()
print('=== PAYTM SETTLEMENTS ===')
r = con.execute("""
    SELECT order_id, amount, transaction_date, status, utr_no, payout_date, settled_date
    FROM paytm_settlements LIMIT 3
""").fetchall()
for x in r: print(' ', x)

match = con.execute("""
    SELECT COUNT(*) FROM paytm_settlements s
    INNER JOIN paytm_transactions t ON TRIM(s.order_id, chr(39)) = TRIM(t.Order_ID, chr(39))
""").fetchone()[0]
print(f'  Settlements matching transactions: {match:,}')

date_range = con.execute("SELECT MIN(transaction_date), MAX(transaction_date) FROM paytm_settlements").fetchone()
print(f'  Date range: {date_range}')

# ── RAZORPAY (embedded) ───────────────────────────────────────
print()
print('=== RAZORPAY SETTLEMENTS (embedded in razorpay_transactions) ===')
r = con.execute("""
    SELECT settlement_id, settled_at, settlement_utr, amount, settled
    FROM razorpay_transactions WHERE type='payment' LIMIT 3
""").fetchall()
for x in r: print(' ', x)
settled = con.execute("""
    SELECT settled, COUNT(*), SUM(amount)
    FROM razorpay_transactions WHERE type='payment'
    GROUP BY settled
""").fetchall()
print('  settled breakdown:', settled)

con.close()
