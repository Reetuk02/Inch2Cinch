CREATE OR REPLACE TABLE mart_top_customers AS
SELECT o.customer_id, c.full_name, c.loyalty_tier,
       SUM(o.order_amount) AS total_spend,
       COUNT(*) AS delivered_orders
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.customer_id
WHERE o.dq_status = 'PASS'
  AND c.dq_status = 'PASS'
  AND o.order_status = 'DELIVERED'
GROUP BY 1,2,3
ORDER BY total_spend DESC
LIMIT 10;
