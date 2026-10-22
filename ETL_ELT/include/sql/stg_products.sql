CREATE OR REPLACE TABLE stg_products AS
WITH typed AS (
    SELECT
        TRIM(product_id) AS product_id,
        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        TRIM(sub_category) AS sub_category,
        TRIM(brand) AS brand,
        TRY_CAST(mrp AS DOUBLE) AS mrp,
        TRY_CAST(cost_price AS DOUBLE) AS cost_price,
        TRIM(unit) AS unit,
        LOWER(TRIM(is_perishable)) AS is_perishable,
        TRY_CAST(launch_date AS DATE) AS launch_date,
        _source_file, _loaded_at, _run_id
    FROM raw_products
),
checked AS (
    SELECT *,
        CASE WHEN product_id IS NULL OR product_id = ''
                   OR category IS NULL OR category = ''
                   OR mrp IS NULL OR mrp <= 0
                   OR cost_price IS NULL OR cost_price < 0
             THEN 'FAIL' ELSE 'PASS' END AS dq_status,
        CASE WHEN product_id IS NULL OR product_id = ''
             THEN 'missing_product_id'
             WHEN category IS NULL OR category = ''
             THEN 'missing_category'
             WHEN mrp IS NULL OR mrp <= 0
             THEN 'invalid_mrp'
             WHEN cost_price IS NULL OR cost_price < 0
             THEN 'invalid_cost_price'
             ELSE NULL END AS failure_reason
    FROM typed
)
SELECT * FROM checked
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_id ORDER BY _loaded_at DESC
) = 1;
