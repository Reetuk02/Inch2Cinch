CREATE OR REPLACE TABLE stg_payments AS
WITH typed AS (
    SELECT
        TRIM(payment_id) AS payment_id,
        TRIM(order_id) AS order_id,
        TRY_CAST(payment_ts AS TIMESTAMP) AS payment_ts,
        UPPER(TRIM(payment_mode)) AS payment_mode,
        TRY_CAST(amount_paid AS DOUBLE) AS amount_paid,
        UPPER(TRIM(payment_status)) AS payment_status,
        NULLIF(TRIM(gateway_ref), '') AS gateway_ref,
        _source_file, _loaded_at, _run_id
    FROM raw_payments
),
checked AS (
    SELECT *,
        CASE WHEN payment_id IS NULL OR payment_id = ''
                   OR order_id IS NULL OR order_id = ''
                   OR payment_ts IS NULL OR amount_paid IS NULL
             THEN 'FAIL' ELSE 'PASS' END AS dq_status,
        CASE WHEN payment_id IS NULL OR payment_id = '' THEN 'missing_payment_id'
             WHEN order_id IS NULL OR order_id = '' THEN 'missing_order_id'
             WHEN payment_ts IS NULL THEN 'invalid_payment_ts'
             WHEN amount_paid IS NULL THEN 'invalid_amount_paid'
             ELSE NULL END AS failure_reason
    FROM typed
)
SELECT * FROM checked
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY payment_id ORDER BY _loaded_at DESC
) = 1;
