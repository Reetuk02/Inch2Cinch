
from datetime import timedelta
from pathlib import Path

import pendulum
import duckdb

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context


DAG_ID = "nammamart_elt_reetu"
DATA_DIR = Path("/usr/local/airflow/include/data")
SQL_DIR = Path("/usr/local/airflow/include/sql")
DB_PATH = DATA_DIR / "warehouse_reetu.duckdb"

SOURCE_FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}
INCREMENT_FILE = "orders_2026-10-01_increment.csv"

default_args = {
    "owner": "reetu",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}

# SQL is deliberately kept in separate files.
SQL_FILES = {
    "stg_orders.sql": """
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
""",
    "stg_payments.sql": """
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
""",
    "stg_customers.sql": """
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
""",
    "stg_products.sql": """
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
""",
    "stg_stores.sql": """
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
""",
    "mart_daily_store_kpis.sql": """
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
""",
    "mart_category_margin.sql": """
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT p.category,
       SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END) AS revenue,
       SUM(CASE WHEN o.order_status = 'DELIVERED'
                THEN o.order_amount - o.quantity * p.cost_price ELSE 0 END) AS gross_margin
FROM stg_orders o
JOIN stg_products p ON o.product_id = p.product_id
WHERE o.dq_status = 'PASS' AND p.dq_status = 'PASS'
GROUP BY p.category;
""",
    "mart_delivery_sla.sql": """
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
""",
    "mart_payment_reconciliation.sql": """
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
""",
    "mart_top_customers.sql": """
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
""",
}


@task
def prepare_sql_files():
    SQL_DIR.mkdir(parents=True, exist_ok=True)
    for filename, sql in SQL_FILES.items():
        (SQL_DIR / filename).write_text(sql.strip() + "\n", encoding="utf-8")
    return sorted(SQL_FILES.keys())


@task
def load_raw_tables():
    """Keep all source rows as text and retain run/file metadata."""
    context = get_current_context()
    run_id = context["run_id"]

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw_ingestion_log (
                source_file VARCHAR PRIMARY KEY,
                loaded_at TIMESTAMP,
                row_count BIGINT
            )
        """)

        for dataset, filename in SOURCE_FILES.items():
            path = DATA_DIR / filename
            if not path.is_file():
                raise FileNotFoundError(f"Missing source file: {path}")

            table = f"raw_{dataset}"
            already_loaded = con.execute(
                "SELECT COUNT(*) FROM raw_ingestion_log WHERE source_file = ?",
                [filename],
            ).fetchone()[0]

            if already_loaded:
                print(f"Base file already loaded: {filename}")
                continue

            df = duckdb.read_csv(
                str(path),
                all_varchar=True,
                header=True,
                null_padding=True,
            ).df()

            df["_loaded_at"] = pendulum.now("Asia/Kolkata").to_iso8601_string()
            df["_source_file"] = filename
            df["_run_id"] = run_id

            con.register("incoming_df", df)
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM incoming_df")
            con.unregister("incoming_df")

            con.execute(
                "INSERT INTO raw_ingestion_log VALUES (?, CURRENT_TIMESTAMP, ?)",
                [filename, len(df)],
            )
            print(f"{table}: loaded {len(df)} row      s")

        return {"run_id": run_id, "database": str(DB_PATH)}
    finally:
        con.close()


@task
def load_increment():
    """Append the increment exactly once to raw_orders."""
    context = get_current_context()
    run_id = context["run_id"]
    filename = INCREMENT_FILE
    path = DATA_DIR / filename

    if not path.is_file():
        raise FileNotFoundError(f"Missing increment file: {path}")

    con = duckdb.connect(str(DB_PATH))
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw_ingestion_log (
                source_file VARCHAR PRIMARY KEY,
                loaded_at TIMESTAMP,
                row_count BIGINT
            )
        """)

        already_loaded = con.execute(
            "SELECT COUNT(*) FROM raw_ingestion_log WHERE source_file = ?",
            [filename],
        ).fetchone()[0]
        if already_loaded:
            print(f"Increment already loaded: {filename}")
            return {"status": "already_loaded"}

        df = duckdb.read_csv(
            str(path), all_varchar=True, header=True, null_padding=True
        ).df()
        df["_loaded_at"] = pendulum.now("Asia/Kolkata").to_iso8601_string()
        df["_source_file"] = filename
        df["_run_id"] = run_id

        con.register("increment_df", df)
        con.execute("INSERT INTO raw_orders SELECT * FROM increment_df")
        con.unregister("increment_df")

        con.execute(
            "INSERT INTO raw_ingestion_log VALUES (?, CURRENT_TIMESTAMP, ?)",
            [filename, len(df)],
        )
        print(f"Appended {len(df)} increment rows to raw_orders")
        return {"status": "loaded", "rows": len(df)}
    finally:
        con.close()


@task
def run_staging_sql(sql_files):
    con = duckdb.connect(str(DB_PATH))
    try:
        for filename in [
            "stg_orders.sql",
            "stg_payments.sql",
            "stg_customers.sql",
            "stg_products.sql",
            "stg_stores.sql",
        ]:
            sql = (SQL_DIR / filename).read_text(encoding="utf-8")
            con.execute(sql)
            print(f"Executed {filename}")

        # Ensure corrected records from the increment are preferred.
        # The staging query deduplicates by order_id using latest _loaded_at.
        return {"status": "staging_complete"}
    finally:
        con.close()


@task
def merge_incremental_orders():
    """Upsert staged order records by order_id into a persistent staging table."""
    con = duckdb.connect(str(DB_PATH))
    try:
        # Preserve current staged orders before applying the latest feed.
        con.execute("""
            CREATE TABLE IF NOT EXISTS stg_orders_current AS
            SELECT * FROM stg_orders WHERE 1 = 0
        """)

        con.execute("""
            UPDATE stg_orders AS o
            SET
                dq_status = 'FAIL',
                failure_reason = CASE
                    WHEN NOT EXISTS (
                        SELECT 1 FROM stg_customers c
                        WHERE c.customer_id = o.customer_id
                    ) THEN 'missing_customer_reference'

                    WHEN NOT EXISTS (
                        SELECT 1 FROM stg_products p
                        WHERE p.product_id = o.product_id
                    ) THEN 'missing_product_reference'

                    WHEN NOT EXISTS (
                        SELECT 1 FROM stg_stores s
                        WHERE s.store_id = o.store_id
                    ) THEN 'missing_store_reference'
                END
            WHERE o.dq_status = 'PASS'
            AND (
                NOT EXISTS (
                    SELECT 1 FROM stg_customers c
                    WHERE c.customer_id = o.customer_id
                )
                OR NOT EXISTS (
                    SELECT 1 FROM stg_products p
                    WHERE p.product_id = o.product_id
                )
                OR NOT EXISTS (
                    SELECT 1 FROM stg_stores s
                    WHERE s.store_id = o.store_id
                )
            )
        """)

        con.execute("""
            MERGE INTO stg_orders_current AS target
            USING stg_orders AS source
            ON target.order_id = source.order_id
            WHEN MATCHED THEN UPDATE SET
                    order_ts = source.order_ts,
                    customer_id = source.customer_id,
                    store_id = source.store_id,
                    product_id = source.product_id,
                    quantity = source.quantity,
                    unit_price = source.unit_price,
                    discount_pct = source.discount_pct,
                    order_amount = source.order_amount,
                    payment_mode = source.payment_mode,
                    order_status = source.order_status,
                    channel = source.channel,
                    delivery_minutes = source.delivery_minutes,
                    dq_status = source.dq_status,
                    failure_reason = source.failure_reason,
                    _source_file = source._source_file,
                    _loaded_at = source._loaded_at,
                    _run_id = source._run_id
            WHEN NOT MATCHED THEN INSERT BY NAME
        """)

        # Add the base orders to the persistent target on first run.
        con.execute("""
            INSERT INTO stg_orders_current
            SELECT s.* FROM stg_orders s
            WHERE NOT EXISTS (
                SELECT 1 FROM stg_orders_current t
                WHERE t.order_id = s.order_id
            )
        """)

        con.execute("CREATE OR REPLACE TABLE stg_orders AS SELECT * FROM stg_orders_current")
        count = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
        print(f"Current staged orders after upsert: {count}")
        return {"staged_orders": count}
    finally:
        con.close()

@task
def quarantine_failed_orders():
    """Quarantine critical failed orders; leave date warnings out."""
    context = get_current_context()
    run_id = context["run_id"]

    con = duckdb.connect(str(DB_PATH))
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS quarantine_orders AS
            SELECT
                o.*,
                CAST(NULL AS VARCHAR) AS quarantine_run_id,
                CAST(NULL AS VARCHAR) AS quarantine_reason,
                CAST(NULL AS VARCHAR) AS severity,
                CAST(NULL AS TIMESTAMP) AS quarantined_at
            FROM stg_orders o
            WHERE 1 = 0
        """)

        # Make retries of this task safe for the current run.
        con.execute(
            "DELETE FROM quarantine_orders WHERE quarantine_run_id = ?",
            [run_id],
        )

        # Date-period issues are warnings, not critical quarantine records.
        con.execute("""
            INSERT INTO quarantine_orders
            SELECT
                o.*,
                ? AS quarantine_run_id,
                COALESCE(
                    o.failure_reason,
                    'unspecified_data_quality_failure'
                ) AS quarantine_reason,
                'Critical' AS severity,
                CURRENT_TIMESTAMP AS quarantined_at
            FROM stg_orders o
            WHERE o.dq_status = 'FAIL'
              AND COALESCE(o.failure_reason, '')
                  <> 'order_date_outside_expected_period'
        """, [run_id])

        count = con.execute("""
            SELECT COUNT(*)
            FROM quarantine_orders
            WHERE quarantine_run_id = ?
        """, [run_id]).fetchone()[0]

        print(f"Critical orders quarantined for this run: {count}")
        return {"rows_quarantined": count}

    finally:
        con.close()


@task
def build_marts():
    con = duckdb.connect(str(DB_PATH))
    try:
        for filename in [
            "mart_daily_store_kpis.sql",
            "mart_category_margin.sql",
            "mart_delivery_sla.sql",
            "mart_payment_reconciliation.sql",
            "mart_top_customers.sql",
        ]:
            con.execute((SQL_DIR / filename).read_text(encoding="utf-8"))
            print(f"Executed {filename}")

        tables = [
            "mart_daily_store_kpis",
            "mart_category_margin",
            "mart_delivery_sla",
            "mart_payment_reconciliation",
            "mart_top_customers",
        ]
        return {
            table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in tables
        }
    finally:
        con.close()


@task(trigger_rule="all_done")
def record_run_audit():
    context = get_current_context()
    con = duckdb.connect(str(DB_PATH))
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR,
                dag_id VARCHAR,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                rows_extracted BIGINT,
                rows_loaded BIGINT,
                rows_quarantined BIGINT,
                status VARCHAR
            )
        """)

        run = context["dag_run"]
        start = run.start_date or pendulum.now("UTC")
        end = pendulum.now("UTC")
        ti = context["ti"]
        required_outputs = [
            ti.xcom_pull(task_ids="prepare_sql_files"),
            ti.xcom_pull(task_ids="load_raw_tables"),
            ti.xcom_pull(task_ids="load_increment"),
            ti.xcom_pull(task_ids="run_staging_sql"),
            ti.xcom_pull(task_ids="merge_incremental_orders"),
            ti.xcom_pull(task_ids="quarantine_failed_orders"),
            ti.xcom_pull(task_ids="build_marts"),
        ]
        dq_count = con.execute(
            "SELECT COUNT(*) FROM dq_results WHERE run_id = ? AND dag_id = ?",
            [context["run_id"], DAG_ID],
        ).fetchone()[0] if con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            ["dq_results"],
        ).fetchone()[0] else 0
        state = (
            "SUCCESS"
            if all(output is not None for output in required_outputs)
            and dq_count == 6
            else "FAILED"
        )
        counts = {}
        for table in [
            "raw_orders", "raw_payments", "raw_customers",
            "raw_products", "raw_stores",
        ]:
            exists = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                [table],
            ).fetchone()[0]
            counts[table] = (
                con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                if exists else 0
            )

        total = sum(counts.values())

        quarantined = con.execute("""
            SELECT COUNT(*)
            FROM quarantine_orders
            WHERE quarantine_run_id = ?
        """, [context["run_id"]]).fetchone()[0]

        con.execute(
            "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                context["run_id"], DAG_ID, start, end,
                total, total, quarantined, state,
            ],
        )
    finally:
        con.close()


@task
def record_dq_results():
    """Record six data-quality dimensions for the current ELT run."""
    context = get_current_context()
    run_id = context["run_id"]

    con = duckdb.connect(str(DB_PATH))
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS dq_results (
                run_id VARCHAR,
                dag_id VARCHAR,
                check_name VARCHAR,
                dimension VARCHAR,
                severity VARCHAR,
                rows_checked BIGINT,
                rows_failed BIGINT,
                status VARCHAR,
                checked_at TIMESTAMP
            )
        """)

        # Make retries of this task idempotent for the current run.
        con.execute(
            "DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?",
            [run_id, DAG_ID],
        )

        checks = [
            (
                "order_required_fields",
                "completeness",
                "Critical",
                "SELECT COUNT(*) FROM stg_orders",
                """
                SELECT COUNT(*) FROM stg_orders
                WHERE order_id IS NULL OR order_id = ''
                   OR order_ts IS NULL
                   OR customer_id IS NULL OR customer_id = ''
                   OR store_id IS NULL OR store_id = ''
                   OR product_id IS NULL OR product_id = ''
                """,
            ),
            (
                "raw_order_id_uniqueness",
                "uniqueness",
                "Critical",
                "SELECT COUNT(*) FROM raw_orders",
                """
                SELECT COALESCE(SUM(n - 1), 0)
                FROM (
                    SELECT COUNT(*) AS n
                    FROM raw_orders
                    WHERE order_id IS NOT NULL AND TRIM(order_id) <> ''
                    GROUP BY TRIM(order_id)
                    HAVING COUNT(*) > 1
                ) duplicates
                """,
            ),
            (
                "order_business_rules",
                "validity",
                "Critical",
                "SELECT COUNT(*) FROM stg_orders",
                """
                SELECT COUNT(*) FROM stg_orders
                WHERE quantity IS NULL OR quantity <= 0
                   OR discount_pct IS NULL
                   OR discount_pct NOT BETWEEN 0 AND 100
                   OR order_status NOT IN
                      ('DELIVERED', 'CANCELLED', 'RETURNED')
                   OR payment_mode NOT IN
                      ('UPI', 'CARD', 'CASH ON DELIVERY', 'WALLET')
                """,
            ),
            (
                "order_amount_accuracy",
                "accuracy",
                "Critical",
                "SELECT COUNT(*) FROM stg_orders",
                """
                SELECT COUNT(*) FROM stg_orders
                WHERE quantity IS NOT NULL
                  AND unit_price IS NOT NULL
                  AND discount_pct BETWEEN 0 AND 100
                  AND order_amount IS NOT NULL
                  AND ABS(
                      order_amount -
                      ROUND(
                          quantity * unit_price *
                          (1 - discount_pct / 100), 2
                      )
                  ) > 0.01
                """,
            ),
            (
                "order_reference_consistency",
                "consistency",
                "Critical",
                "SELECT COUNT(*) FROM stg_orders",
                """
                SELECT COUNT(*)
                FROM stg_orders o
                LEFT JOIN stg_customers c
                    ON o.customer_id = c.customer_id
                LEFT JOIN stg_products p
                    ON o.product_id = p.product_id
                LEFT JOIN stg_stores s
                    ON o.store_id = s.store_id
                WHERE c.customer_id IS NULL
                   OR p.product_id IS NULL
                   OR s.store_id IS NULL
                """,
            ),
            (
                "order_date_timeliness",
                "timeliness",
                "Warning",
                """
                SELECT COUNT(*) FROM raw_orders
                WHERE _source_file = 'orders.csv'
                """,
                """
                SELECT COUNT(*) FROM stg_orders
                WHERE failure_reason =
                    'order_date_outside_expected_period'
                """,
            ),
        ]

        for name, dimension, severity, checked_sql, failed_sql in checks:
            rows_checked = con.execute(checked_sql).fetchone()[0]
            rows_failed = con.execute(failed_sql).fetchone()[0]
            status = "PASS" if rows_failed == 0 else "FAIL"

            con.execute("""
                INSERT INTO dq_results
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [
                run_id, DAG_ID, name, dimension, severity,
                rows_checked, rows_failed, status,
            ])

            print(
                f"{dimension}: {name} | "
                f"checked={rows_checked}, failed={rows_failed}, "
                f"status={status}"
            )

    finally:
        con.close()

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="NammaMart ELT: raw, SQL staging, incremental upsert and marts",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    schedule="0 9 * * 1-6",
    catchup=False,
    max_active_runs=1,
    tags=["nammamart", "elt"],
) as dag:
    sql_files = prepare_sql_files()
    raw = load_raw_tables()
    increment = load_increment()
    staging = run_staging_sql(sql_files)
    merged = merge_incremental_orders()
    quarantine = quarantine_failed_orders()
    marts = build_marts()
    audit = record_run_audit()
    dq_audit = record_dq_results()
    sql_files >> raw >> increment >> staging >> merged
    merged >> quarantine >> dq_audit >> marts >> audit
