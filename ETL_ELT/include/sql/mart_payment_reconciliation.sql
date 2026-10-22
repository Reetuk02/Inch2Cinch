CREATE OR REPLACE TABLE mart_payment_reconciliation AS
SELECT o.order_id, o.order_amount,
       p.payment_id, p.amount_paid, p.payment_status,
       CASE WHEN p.payment_id IS NULL THEN 'NO_PAYMENT'
            WHEN UPPER(p.payment_status) = 'SUCCESS'
                 AND ABS(o.order_amount - p.amount_paid) > 0.01
                 THEN 'AMOUNT_MISMATCH'
            ELSE 'MATCH' END AS reconciliation_status
FROM stg_orders o
LEFT JOIN stg_payments p ON o.order_id = p.order_id
WHERE o.dq_status = 'PASS'
  AND (p.payment_id IS NULL
       OR (UPPER(p.payment_status) = 'SUCCESS'
           AND ABS(o.order_amount - p.amount_paid) > 0.01));
