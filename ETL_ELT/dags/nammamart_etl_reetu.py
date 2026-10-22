from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from pathlib import Path
from airflow.decorators import task
import os
import duckdb

DAG_ID = "nammamart_etl_reetu"
DATA_DIR = Path("/usr/local/airflow/include/data")
STAGING_DIR = DATA_DIR / "staging" / "nammamart"
DB_PATH = DATA_DIR / "nammamart.duckdb"

SOURCE_FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}

default_args = {
    "owner": "reetu",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}
DATASET_RULES = {
    "orders": {
        "required": ["order_id", "order_ts", "customer_id",
                     "product_id", "store_id"],
        "unique_key": "order_id",
    },
    "payments": {
        "required": ["payment_id", "order_id", "payment_ts"],
        "unique_key": "payment_id",
    },
    "customers": {
        "required": ["customer_id"],
        "unique_key": "customer_id",
    },
    "products": {
        "required": ["product_id", "category"],
        "unique_key": "product_id",
    },
    "stores": {
        "required": ["store_id"],
        "unique_key": "store_id",
    },
}


def extract_csv(dataset_name):
    """Read one source CSV and write an extracted Parquet file."""

    file_path = DATA_DIR / SOURCE_FILES[dataset_name]

    if not file_path.exists():
        raise FileNotFoundError(
            f"Source file not found: {file_path}"
        )

    df = pd.read_csv(file_path)

    if df.empty:
        raise ValueError(
            f"Source file contains no data rows: {file_path}"
        )

    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    # Define staged_path BEFORE using it.
    staged_path = STAGING_DIR / f"{dataset_name}_extracted.parquet"

    # Write the DataFrame to Parquet.
    df.to_parquet(staged_path, index=False)

    # Log extraction results.
    print(f"Dataset: {dataset_name}")
    print(f"Source file: {file_path}")
    print(f"Staged path: {staged_path}")
    print(f"File exists: {staged_path.is_file()}")
    print(f"File size: {staged_path.stat().st_size}")
    print(f"Rows extracted: {len(df)}")

    # Return the Parquet path to the validation task.
    return {
        "dataset": dataset_name,
        "file_path": str(staged_path),
        "row_count": len(df),
        "columns": df.columns.tolist(),
    }

@task
def extract_orders():
    return extract_csv("orders")
@task
def extract_payments():
    return extract_csv("payments")
@task
def extract_products():
    return extract_csv("products")
@task
def extract_customers():
    return extract_csv("customers")
@task
def extract_stores():
    return extract_csv("stores")
@task
def validate_orders(extraction_info: dict):
    return validate_dataset("orders", extraction_info)


@task
def validate_payments(extraction_info: dict):
    return validate_dataset("payments", extraction_info)


@task
def validate_customers(extraction_info: dict):
    return validate_dataset("customers", extraction_info)


@task
def validate_products(extraction_info: dict):
    return validate_dataset("products", extraction_info)


@task
def validate_stores(extraction_info: dict):
    return validate_dataset("stores", extraction_info)

@task
def transform_data(
    orders_info: dict,
    customers_info: dict,
    products_info: dict,
    stores_info: dict,
):
    orders = pd.read_parquet(orders_info["passed_path"])
    customers = pd.read_parquet(customers_info["passed_path"])
    products = pd.read_parquet(products_info["passed_path"])
    stores = pd.read_parquet(stores_info["passed_path"])


    # Standardize text values.
    orders["order_status"] = orders["order_status"].astype("string").str.strip().str.upper()
    customers["loyalty_tier"] = customers["loyalty_tier"].astype("string").str.strip().str.upper()
    products["category"] = products["category"].astype("string").str.strip()

    # Mask personally identifiable information.
    if "email" in customers.columns:
        customers["email"] = customers["email"].apply(
            lambda x: (
                str(x)[:2] + "***@***"
                if pd.notna(x) and "@" in str(x)
                else None
            )
        )

    if "phone" in customers.columns:
        customers["phone"] = customers["phone"].apply(
            lambda x: ("******" + str(x)[-4:])
            if pd.notna(x) else None
        )
    # Join orders with their dimensions.

    result = orders.merge(
        customers, on="customer_id", how="left",
        suffixes=("", "_customer")
    )
    result = result.merge(
        products, on="product_id", how="left",
        suffixes=("", "_product")
    )
    result = result.merge(
        stores, on="store_id", how="left",
        suffixes=("", "_store")
    )

    # Derive business metrics.
    result["order_ts"] = pd.to_datetime(
        result["order_ts"], errors="coerce"
    )
    result["order_date"] = result["order_ts"].dt.date

    result["net_revenue"] = result["order_amount"].where(
        result["order_status"].eq("DELIVERED"), 0
    )

    result["gross_margin"] = (
        result["net_revenue"]
        - result["cost_price"] * result["quantity"]
    )

    result["within_15_min"] = (
        result["delivery_minutes"].le(15)
        & result["order_status"].eq("DELIVERED")
    )

    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    output_path = STAGING_DIR / "transformed_orders.parquet"
    result.to_parquet(output_path, index=False)

    print(f"Transformed rows: {len(result)}")
    print(f"Output: {output_path}")

    return {
        "file_path": str(output_path),
        "row_count": len(result),
    }

@task
def load_to_duckdb(transformed_info: dict):
    """Load clean data and dimensions into DuckDB."""
    con = duckdb.connect(str(DB_PATH))
    try:
        orders = pd.read_parquet(transformed_info["file_path"])
        customers = pd.read_parquet(
            STAGING_DIR / "customers_passed.parquet"
        )
        products = pd.read_parquet(
            STAGING_DIR / "products_passed.parquet"
        )
        stores = pd.read_parquet(
            STAGING_DIR / "stores_passed.parquet"
        )
        con.register("orders_df", orders)
        con.register("customers_df", customers)
        con.register("products_df", products)
        con.register("stores_df", stores)

        con.execute("""
            CREATE OR REPLACE TABLE etl_fact_orders AS
            SELECT * FROM orders_df
        """)
        con.execute("""
            CREATE OR REPLACE TABLE etl_dim_customer AS
            SELECT * FROM customers_df
        """)
        con.execute("""
            CREATE OR REPLACE TABLE etl_dim_product AS
            SELECT * FROM products_df
        """)
        con.execute("""
            CREATE OR REPLACE TABLE etl_dim_store AS
            SELECT * FROM stores_df
        """)

        count = con.execute(
            "SELECT COUNT(*) FROM etl_fact_orders"
        ).fetchone()[0]

        print(f"Rows loaded into etl_fact_orders: {count}")
        
        # 1. Daily revenue by zone
        con.execute("""
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT
                CAST(order_ts AS DATE) AS order_date,
                zone,
                COUNT(*) FILTER (
                    WHERE order_status = 'DELIVERED'
                ) AS delivered_orders,
                ROUND(SUM(
                    CASE WHEN order_status = 'DELIVERED'
                         THEN order_amount ELSE 0 END
                ), 2) AS net_revenue
            FROM etl_fact_orders
            GROUP BY 1, 2
        """)

        # 2. Revenue and gross margin by category
        con.execute("""
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT
                category,
                ROUND(SUM(net_revenue), 2) AS revenue,
                ROUND(SUM(gross_margin), 2) AS gross_margin
            FROM etl_fact_orders
            GROUP BY category
        """)

        # 3. Payment reconciliation
        # The validated payment file is used as the payment source.
        payments = pd.read_parquet(
            STAGING_DIR / "payments_passed.parquet"
        )
        con.register("payments_df", payments)

        con.execute("""
            CREATE OR REPLACE TABLE etl_payment_reconciliation AS
            WITH unique_payments AS (
                SELECT DISTINCT
                    TRIM(payment_id) AS payment_id,
                    TRIM(order_id) AS order_id,
                    UPPER(TRIM(payment_status)) AS payment_status,
                    TRY_CAST(amount_paid AS DOUBLE) AS amount_paid
                FROM payments_df
            ),
            payment_totals AS (
                SELECT
                    order_id,
                    SUM(amount_paid) FILTER (
                        WHERE payment_status = 'SUCCESS'
                    ) AS amount_paid,
                    COUNT(*) FILTER (
                        WHERE payment_status = 'SUCCESS'
                    ) AS successful_records,
                    COUNT(*) FILTER (
                        WHERE payment_status = 'FAILED'
                    ) AS failed_records,
                    COUNT(*) FILTER (
                        WHERE payment_status = 'REFUNDED'
                    ) AS refunded_records
                FROM unique_payments
                GROUP BY order_id
            )
            SELECT
                o.order_id,
                o.order_amount,
                p.amount_paid,
                CASE
                    WHEN p.order_id IS NULL THEN 'NO_PAYMENT'
                    WHEN p.successful_records > 0
                         AND ABS(o.order_amount - p.amount_paid) <= 0.01
                        THEN 'MATCH'
                    WHEN p.successful_records > 0 THEN 'AMOUNT_MISMATCH'
                    WHEN p.refunded_records > 0 THEN 'REFUNDED'
                    WHEN p.failed_records > 0 THEN 'FAILED_PAYMENT'
                    ELSE 'OTHER_PAYMENT_STATUS'
                END AS reconciliation_status
            FROM etl_fact_orders o
            LEFT JOIN payment_totals p
                ON TRIM(o.order_id) = p.order_id
        """)

        con.unregister("payments_df")

        
    finally:
        con.close()
    return {"loaded_rows": count, "database": str(DB_PATH)}

def validate_dataset(dataset_name, extraction_info: dict):
    staged_path = Path(extraction_info["file_path"])
    if not staged_path.is_file():
        raise FileNotFoundError(
            f"Staged Parquet file not found: {staged_path}"
        )
    # Read the staged Parquet file, not the original CSV.
    df = pd.read_parquet(staged_path)
    rules = DATASET_RULES[dataset_name]
    # Normalize known text variants before validation.
    if dataset_name == "orders":
        if "order_status" in df.columns:
            df["order_status"] = (
                df["order_status"].astype("string").str.strip().str.upper()
            )

        if "payment_mode" in df.columns:
            payment_mode_map = {
                "UPI": "UPI",
                "CARD": "Card",
                "CASH ON DELIVERY": "Cash on Delivery",
                "WALLET": "Wallet",
                "COD": "Cash on Delivery",
            }

            df["payment_mode"] = (
                df["payment_mode"]
                .astype("string")
                .str.strip()
                .str.upper()
                .map(payment_mode_map)
                .fillna(df["payment_mode"])
            )

    # Check whether all required columns exist.
    missing_columns = [
        column for column in rules["required"]
        if column not in df.columns
    ]
    if missing_columns:
        raise ValueError(
            f"Orders file is missing required columns: {missing_columns}"
        )
    checks = pd.DataFrame(index=df.index)
    for column in rules["required"]:
        values = df[column]
        missing = values.isna()
        if (
            pd.api.types.is_object_dtype(values)
            or pd.api.types.is_string_dtype(values)
        ):
            missing = (
                    missing | values.astype("string")
                        .str.strip()
                        .eq("")
                        .fillna(True)
            )
    
        checks[f"missing_{column}"] = missing
    key = rules["unique_key"]
    checks[f"duplicate_{key}"] = (
        df[key].notna()
        & df[key].duplicated(keep=False)
    )

       # Additional business checks for orders.
    if dataset_name == "orders":
                # Validate order dates against the expected reporting period.
        order_dates = pd.to_datetime(df["order_ts"], errors="coerce")

        checks["invalid_order_ts"] = order_dates.isna()

        checks["order_date_outside_expected_period"] = (
            order_dates.notna()
            & (
                (order_dates < pd.Timestamp("2026-09-01"))
                |(order_dates >= pd.Timestamp("2026-10-02"))
            )
        )
        checks["invalid_status"] = ~df["order_status"].isin(
            ["DELIVERED", "CANCELLED", "RETURNED"]
        )
        checks["invalid_discount"] = (
            df["discount_pct"].isna()
            | ~df["discount_pct"].between(0, 100)
        )
        checks["invalid_quantity"] = (
            df["quantity"].isna()
            | (df["quantity"] <= 0)
        )
        expected_amount = (
            df["quantity"]
            * df["unit_price"]
            * (1 - df["discount_pct"] / 100)
        ).round(2)

        checks["incorrect_order_amount"] = (
            df["order_amount"].isna()
            | ((df["order_amount"] - expected_amount).abs() > 0.01)
        )

        checks["missing_delivery_minutes"] = (
            df["order_status"].eq("DELIVERED")
            & df["delivery_minutes"].isna()
        )
    # VALIDITY: allowed payment modes
        checks["invalid_payment_mode"] = ~df["payment_mode"].isin(
            ["UPI", "Card", "Cash on Delivery", "Wallet"]
        )

# True means at least one validation rule failed for that order.
    failed_mask = checks.any(axis=1)

    # For each row, collect the names of checks that failed.
    failure_reasons = checks.apply(
        lambda row: "; ".join(row.index[row].tolist()),
        axis=1,
    )

    passed = df.loc[~failed_mask].copy()
    failed = df.loc[failed_mask].copy()

    # Save passed records for the transformation stage.
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    passed_path = STAGING_DIR / f"{dataset_name}_passed.parquet"
    failed_path = STAGING_DIR / f"{dataset_name}_failed_pending.parquet"

    passed.to_parquet(passed_path, index=False)

    # Save failed rows temporarily for the separate quarantine task.
    failed["failure_reason"] = failure_reasons.loc[failed_mask]
    failed.to_parquet(failed_path, index=False)

    return {
        "dataset": dataset_name,
        "input_rows": len(df),
        "passed_rows": len(passed),
        "failed_rows": len(failed),
        "passed_path": str(passed_path),
        "failed_path": str(failed_path),
    }



@task
def quarantine_all_datasets(
    orders_info: dict,
    payments_info: dict,
    customers_info: dict,
    products_info: dict,
    stores_info: dict,
):
    """Combine failed records from all five datasets."""

    validation_results = [
        orders_info,
        payments_info,
        customers_info,
        products_info,
        stores_info,
    ]

    quarantine_frames = []

    for info in validation_results:
        failed_path = Path(info["failed_path"])

        if not failed_path.is_file():
            raise FileNotFoundError(
                f"Failed records file not found: {failed_path}"
            )

        failed = pd.read_parquet(failed_path)
        # Write a separate, auditable CSV for each dataset and DAG run.
        from airflow.operators.python import get_current_context
        context = get_current_context()
        run_id_safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in context["run_id"])
        quarantine_dir = DATA_DIR / "quarantine" / "reetu"
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        failed_csv = quarantine_dir / f"{info['dataset']}_failed_{run_id_safe}.csv"
        failed.to_csv(failed_csv, index=False)
        print(f"Per-dataset quarantine CSV: {failed_csv}")

        # Identify which dataset each failed row came from.
        failed.insert(0, "source_dataset", info["dataset"])
        # Mask customer PII before writing quarantine records.
        if info["dataset"] == "customers":
            if "email" in failed.columns:
                failed["email"] = failed["email"].apply(
                    lambda x: (
                        str(x)[:2] + "***@***"
                        if pd.notna(x) and "@" in str(x)
                        else None
                    )
                )

            if "phone" in failed.columns:
                failed["phone"] = failed["phone"].apply(
                    lambda x: (
                        "******" + str(int(float(x)))[-4:]
                        if pd.notna(x)
                        else None
                    )
                )
        quarantine_frames.append(failed)

    # Combine all failed records into one DataFrame.
    if quarantine_frames:
        all_failed = pd.concat(
            quarantine_frames,
            ignore_index=True,
            sort=False,
        )
    else:
        all_failed = pd.DataFrame()

    quarantine_path = STAGING_DIR / "all_datasets_quarantine.parquet"
    all_failed.to_parquet(quarantine_path, index=False)

    print(f"Total quarantined records: {len(all_failed)}")
    print(f"Quarantine file: {quarantine_path}")

    return {
        "quarantined_rows": len(all_failed),
        "quarantine_path": str(quarantine_path),
    }


@task
def record_etl_dq_results(loaded_info: dict):
    """Persist six data-quality dimensions for the loaded ETL data."""
    from airflow.operators.python import get_current_context

    context = get_current_context()
    run_id = context["run_id"]
    con = duckdb.connect(str(DB_PATH))

    checks = [
        (
            "required_order_fields", "Completeness", "Critical",
            """SELECT COUNT(*) FROM etl_fact_orders
               WHERE order_id IS NULL OR customer_id IS NULL
                  OR product_id IS NULL OR store_id IS NULL
                  OR order_ts IS NULL"""
        ),
        (
            "duplicate_order_id", "Uniqueness", "Critical",
            """SELECT COUNT(*) FROM (
                 SELECT order_id FROM etl_fact_orders
                 GROUP BY order_id HAVING COUNT(*) > 1
               )"""
        ),
        (
            "valid_order_amount_and_status", "Validity", "Critical",
            """SELECT COUNT(*) FROM etl_fact_orders
               WHERE TRY_CAST(order_amount AS DOUBLE) IS NULL
                  OR order_amount < 0
                  OR order_status NOT IN
                     ('DELIVERED', 'CANCELLED', 'RETURNED')"""
        ),
        (
            "payment_amount_mismatch", "Accuracy", "Warning",
            """SELECT COUNT(*) FROM etl_payment_reconciliation
               WHERE reconciliation_status = 'AMOUNT_MISMATCH'"""
        ),
        (
            "dimension_reference_integrity", "Consistency", "Critical",
            """SELECT COUNT(*) FROM etl_fact_orders f
               LEFT JOIN etl_dim_customer c
                 ON f.customer_id = c.customer_id
               LEFT JOIN etl_dim_product p
                 ON f.product_id = p.product_id
               LEFT JOIN etl_dim_store s
                 ON f.store_id = s.store_id
               WHERE c.customer_id IS NULL
                  OR p.product_id IS NULL
                  OR s.store_id IS NULL"""
        ),
        (
            "future_order_timestamp", "Timeliness", "Warning",
            """SELECT COUNT(*) FROM etl_fact_orders
               WHERE TRY_CAST(order_ts AS TIMESTAMP) > CURRENT_TIMESTAMP"""
        ),
    ]

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

        # Make retries for the same run idempotent.
        con.execute(
            "DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?",
            [run_id, DAG_ID],
        )

        rows_checked = con.execute(
            "SELECT COUNT(*) FROM etl_fact_orders"
        ).fetchone()[0]

        for name, dimension, severity, sql in checks:
            rows_failed = con.execute(sql).fetchone()[0]
            con.execute("""
                INSERT INTO dq_results
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [
                run_id, DAG_ID, name, dimension, severity,
                rows_checked, rows_failed,
                "PASS" if rows_failed == 0 else "FAIL",
            ])

        return {"dq_checks": len(checks)}
    finally:
        con.close()


@task(trigger_rule="all_done")
def record_etl_pipeline_run():
    """Write one audit record after upstream tasks finish."""
    from airflow.operators.python import get_current_context

    context = get_current_context()
    ti = context["ti"]
    dag_run = context["dag_run"]
    run_id = context["run_id"]

    extracted = 0
    for task_id in (
        "extract_orders", "extract_payments", "extract_customers",
        "extract_products", "extract_stores",
    ):
        info = ti.xcom_pull(task_ids=task_id)
        if isinstance(info, dict):
            extracted += int(info.get("row_count", 0))

    loaded_info = ti.xcom_pull(task_ids="load_to_duckdb") or {}
    quarantine_info = ti.xcom_pull(
        task_ids="quarantine_all_datasets"
    ) or {}

    dq_info = ti.xcom_pull(task_ids="record_etl_dq_results") or {}
    extraction_task_ids = (
        "extract_orders", "extract_payments", "extract_customers",
        "extract_products", "extract_stores",
    )
    extraction_results = [
        ti.xcom_pull(task_ids=task_id)
        for task_id in extraction_task_ids
    ]
    status = (
        "SUCCESS"
        if all(isinstance(info, dict) for info in extraction_results)
        and "loaded_rows" in loaded_info
        and "quarantined_rows" in quarantine_info
        and dq_info.get("dq_checks") == 6
        else "FAILED"
    )

    start_time = dag_run.start_date or pendulum.now("UTC")
    end_time = pendulum.now("UTC")

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
        con.execute(
            "DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?",
            [run_id, DAG_ID],
        )
        con.execute("""
            INSERT INTO pipeline_runs
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            run_id, DAG_ID, start_time, end_time,
            extracted,
            int(loaded_info.get("loaded_rows", 0)),
            int(quarantine_info.get("quarantined_rows", 0)),
            status,
        ])
    finally:
        con.close()


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="NammaMart ETL pipeline",
    start_date=pendulum.datetime(
        2026, 10, 1, tz="Asia/Kolkata"
    ),
    schedule="0 9 * * 1-6",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["nammamart", "etl"],
) as dag:
    orders = extract_orders()
    payments = extract_payments()
    customers = extract_customers()
    products = extract_products()
    stores = extract_stores()
   # Step 2: Validate the extracted datasets
    orders_validation = validate_orders(orders)
    payments_validation = validate_payments(payments)
    customers_validation = validate_customers(customers)
    products_validation = validate_products(products)
    stores_validation = validate_stores(stores)
    quarantine_result = quarantine_all_datasets(
        orders_validation,
        payments_validation,
        customers_validation,
        products_validation,
        stores_validation,
    )
    transformed = transform_data(
        orders_validation,
        customers_validation,
        products_validation,
        stores_validation,
    )
    loaded = load_to_duckdb(transformed)
    dq_results = record_etl_dq_results(loaded)
    audit = record_etl_pipeline_run()
    loaded >> audit
    quarantine_result >> audit
    dq_results >> audit

    loaded = load_to_duckdb(transformed)