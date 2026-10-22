CREATE OR REPLACE TABLE stg_stores AS
WITH typed AS (
    SELECT
        TRIM(store_id) AS store_id,
        TRIM(store_name) AS store_name,
        TRIM(zone) AS zone,
        TRIM(area) AS area,
        TRIM(pincode) AS pincode,
        TRY_CAST(open_date AS DATE) AS open_date,
        TRIM(manager_name) AS manager_name,
        TRY_CAST(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
        _source_file, _loaded_at, _run_id
    FROM raw_stores
),
checked AS (
    SELECT *,
        CASE WHEN store_id IS NULL OR store_id = ''
                   OR zone IS NULL OR zone = ''
                   OR open_date IS NULL
             THEN 'FAIL' ELSE 'PASS' END AS dq_status,
        CASE WHEN store_id IS NULL OR store_id = ''
             THEN 'missing_store_id'
             WHEN zone IS NULL OR zone = ''
             THEN 'missing_zone'
             WHEN open_date IS NULL THEN 'invalid_open_date'
             ELSE NULL END AS failure_reason
    FROM typed
)
SELECT * FROM checked
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY store_id ORDER BY _loaded_at DESC
) = 1;
