CREATE OR REPLACE TABLE stg_orders AS
WITH typed AS (
    SELECT
        TRIM(order_id) AS order_id,
        TRY_CAST(NULLIF(TRIM(order_ts), '') AS TIMESTAMP) AS order_ts,
        NULLIF(TRIM(customer_id), '') AS customer_id,
        NULLIF(TRIM(store_id), '') AS store_id,
        NULLIF(TRIM(product_id), '') AS product_id,
        TRY_CAST(quantity AS INTEGER) AS quantity,
        TRY_CAST(unit_price AS DOUBLE) AS unit_price,
        TRY_CAST(discount_pct AS DOUBLE) AS discount_pct,
        TRY_CAST(order_amount AS DOUBLE) AS order_amount,
        UPPER(TRIM(payment_mode)) AS payment_mode,
        UPPER(TRIM(order_status)) AS order_status,
        TRIM(channel) AS channel,
        TRY_CAST(delivery_minutes AS INTEGER) AS delivery_minutes,
        _source_file,
        _loaded_at,
        _run_id
    FROM raw_orders
),

checked AS (
    SELECT *,
        CASE
            WHEN order_id IS NULL OR order_id = '' THEN 'FAIL'
            WHEN order_ts IS NULL THEN 'FAIL'
            WHEN _source_file = 'orders.csv'
                 AND CAST(order_ts AS DATE) NOT BETWEEN
                     DATE '2026-09-01' AND DATE '2026-09-30'
                THEN 'FAIL'
            WHEN customer_id IS NULL THEN 'FAIL'
            WHEN product_id IS NULL OR store_id IS NULL THEN 'FAIL'
            WHEN quantity IS NULL OR quantity <= 0 THEN 'FAIL'
            WHEN discount_pct IS NULL
                 OR discount_pct NOT BETWEEN 0 AND 100 THEN 'FAIL'
            WHEN order_status NOT IN ('DELIVERED','CANCELLED','RETURNED')
                THEN 'FAIL'
            WHEN payment_mode NOT IN
                 ('UPI','CARD','CASH ON DELIVERY','WALLET')
                THEN 'FAIL'
            WHEN ABS(
                order_amount -
                ROUND(quantity * unit_price * (1 - discount_pct / 100), 2)
            ) > 0.01 THEN 'FAIL'
            WHEN order_status = 'DELIVERED'
                 AND delivery_minutes IS NULL THEN 'FAIL'
            ELSE 'PASS'
        END AS dq_status,

        CASE
            WHEN order_id IS NULL OR order_id = ''
                THEN 'missing_order_id'
            WHEN order_ts IS NULL
                THEN 'invalid_order_ts'
            WHEN _source_file = 'orders.csv'
                 AND CAST(order_ts AS DATE) NOT BETWEEN
                     DATE '2026-09-01' AND DATE '2026-09-30'
                THEN 'order_date_outside_expected_period'
            WHEN customer_id IS NULL
                THEN 'missing_customer_id'
            WHEN product_id IS NULL OR store_id IS NULL
                THEN 'missing_product_or_store_id'
            WHEN quantity IS NULL OR quantity <= 0
                THEN 'invalid_quantity'
            WHEN discount_pct IS NULL
                 OR discount_pct NOT BETWEEN 0 AND 100
                THEN 'invalid_discount'
            WHEN order_status NOT IN ('DELIVERED','CANCELLED','RETURNED')
                THEN 'invalid_order_status'
            WHEN payment_mode NOT IN
                 ('UPI','CARD','CASH ON DELIVERY','WALLET')
                THEN 'invalid_payment_mode'
            WHEN ABS(
                order_amount -
                ROUND(quantity * unit_price * (1 - discount_pct / 100), 2)
            ) > 0.01 THEN 'incorrect_order_amount'
            WHEN order_status = 'DELIVERED'
                 AND delivery_minutes IS NULL
                THEN 'missing_delivery_minutes'
            ELSE NULL
        END AS failure_reason
    FROM typed
)

SELECT * FROM checked
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id ORDER BY _loaded_at DESC
) = 1;
