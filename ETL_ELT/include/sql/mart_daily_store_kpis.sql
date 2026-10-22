CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT CAST(o.order_ts AS DATE) AS order_date,
       s.store_id, s.store_name, s.zone,
       COUNT(*) FILTER (WHERE o.order_status = 'DELIVERED') AS delivered_orders,
       SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END) AS net_revenue,
       AVG(o.delivery_minutes) FILTER (WHERE o.order_status = 'DELIVERED') AS avg_delivery_minutes
FROM stg_orders o
LEFT JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASS'
GROUP BY 1,2,3,4;
