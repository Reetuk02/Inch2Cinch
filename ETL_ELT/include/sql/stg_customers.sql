CREATE OR REPLACE TABLE stg_customers AS
WITH typed AS (
    SELECT
        TRIM(customer_id) AS customer_id,
        TRIM(full_name) AS full_name,
        CASE WHEN STRPOS(email, '@') > 0
             THEN SUBSTR(TRIM(email), 1, 2) || '***@' ||
                  SUBSTR(TRIM(email), STRPOS(email, '@') + 1)
             ELSE NULL END AS email,
        CASE WHEN LENGTH(TRIM(phone)) >= 4
             THEN '******' || RIGHT(TRIM(phone), 4)
             ELSE NULL END AS phone,
        TRIM(city) AS city, TRIM(area) AS area,
        TRIM(pincode) AS pincode,
        TRY_CAST(signup_date AS DATE) AS signup_date,
        UPPER(TRIM(loyalty_tier)) AS loyalty_tier,
        LOWER(TRIM(is_active)) AS is_active,
        _source_file, _loaded_at, _run_id
    FROM raw_customers
),
checked AS (
    SELECT *,
        CASE WHEN customer_id IS NULL OR customer_id = ''
                   OR signup_date IS NULL
             THEN 'FAIL' ELSE 'PASS' END AS dq_status,
        CASE WHEN customer_id IS NULL OR customer_id = ''
             THEN 'missing_customer_id'
             WHEN signup_date IS NULL THEN 'invalid_signup_date'
             ELSE NULL END AS failure_reason
    FROM typed
)
SELECT * FROM checked
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id ORDER BY _loaded_at DESC
) = 1;
