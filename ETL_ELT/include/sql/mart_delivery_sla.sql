CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT s.store_id, s.store_name, s.zone,
       AVG(o.delivery_minutes) AS avg_delivery_minutes,
       100.0 * COUNT(*) FILTER (WHERE o.delivery_minutes <= 15)
             / NULLIF(COUNT(*), 0) AS pct_within_15_min,
       COUNT(*) AS delivered_orders
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASS' AND o.order_status = 'DELIVERED'
GROUP BY 1,2,3;
