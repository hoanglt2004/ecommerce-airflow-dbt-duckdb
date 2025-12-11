from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

import duckdb
import os

# ==== 1. CẤU HÌNH ĐƯỜNG DẪN (SỬA CHO PHÙ HỢP MÁY/BÊN TRONG CONTAINER) ====

# Nếu bạn đã chỉnh path phù hợp cho DAG cũ và nó chạy OK,
# thì dùng lại y chang path đó ở đây.
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DUCKDB_PATH = PROJECT_ROOT / "duckdb" / "ecommerce.duckdb"
CSV_PATH = PROJECT_ROOT / "data" / "raw" / "ecommerce.csv"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"
EXPORT_DIR = PROJECT_ROOT / "data" / "exports"   # nơi export CSV cho BI
# ==== 2. HÀM INGEST RAW CSV → DUCKDB (RAW LAYER) ====

def ingest_raw_to_duckdb(**context):
    """
    Load (hoặc reload) file ecommerce.csv vào DuckDB schema raw.ecommerce.
    Safe để chạy nhiều lần: CREATE OR REPLACE TABLE.
    """
    print(f"[INGEST] Using DuckDB file: {DUCKDB_PATH}")
    print(f"[INGEST] Using CSV file   : {CSV_PATH}")

    con = duckdb.connect(str(DUCKDB_PATH))

    # Tạo schema raw nếu chưa có
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    # Load lại bảng raw.ecommerce từ CSV
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.ecommerce AS
        SELECT *
        FROM read_csv_auto('{CSV_PATH}', HEADER=TRUE);
    """)

    # Log số dòng để dễ debug
    rows = con.execute("SELECT COUNT(*) FROM raw.ecommerce;").fetchone()[0]
    print(f"[INGEST] Loaded {rows} rows into raw.ecommerce")

    con.close()


# ==== 3. HÀM TẠO LỆNH DBT (GIÚP CODE NGẮN GỌN HƠN) ====

def build_dbt_command(subcommand: str, selectors: str) -> str:
    """
    Tạo bash command để chạy dbt trong đúng thư mục project.
    Ví dụ:
      build_dbt_command("run", "staging") → "cd ... && dbt run --select staging"
    """
    return f"""
    cd {DBT_PROJECT_DIR} && \
    dbt {subcommand} --project-dir . --profiles-dir . --select {selectors}
    """.strip()


# ==== 4. EXPORT MARTS → CSV CHO BI (METABASE, POWER BI, ETC.) ====

def export_marts_to_csv(**context):
    """
    Export các bảng mart chính ra CSV để phục vụ BI tools (Metabase, Power BI, Visivo, ...).
    Chỉ chạy sau khi dbt test đã pass, nên data đã được validate.
    """
    print(f"[EXPORT] Using DuckDB file: {DUCKDB_PATH}")
    print(f"[EXPORT] Export directory : {EXPORT_DIR}")

    # Tạo folder export nếu chưa tồn tại
    os.makedirs(EXPORT_DIR, exist_ok=True)

    con = duckdb.connect(str(DUCKDB_PATH))

    exports = [
        ("mart_daily_sales", "mart_daily_sales.csv"),
        ("mart_product_performance", "mart_product_performance.csv"),
        ("mart_customer_ltv", "mart_customer_ltv.csv"),
    ]

    exports = [
    ("mart_daily_sales", "mart_daily_sales.csv"),
    ("mart_product_performance", "mart_product_performance.csv"),
    ("mart_customer_ltv", "mart_customer_ltv.csv"),
]

    for table_name, file_name in exports:
        export_path = EXPORT_DIR / file_name
        export_path_str = export_path.as_posix()

        if table_name == "mart_customer_ltv":
            # ÉP CUSTOMER THÀNH TEXT CÓ PREFIX 'C' → Metabase chắc chắn coi là TEXT
            query = f"""
                COPY (
                    SELECT
                        'C' || CAST(customer_id AS VARCHAR) AS customer_id,
                        country,
                        lifetime_value,
                        total_orders,
                        first_order_date,
                        last_order_date
                    FROM main_mart.{table_name}
                )
                TO '{export_path_str}'
                WITH (FORMAT 'csv', HEADER true);
            """
        else:
            query = f"""
                COPY main_mart.{table_name}
                TO '{export_path_str}'
                WITH (FORMAT 'csv', HEADER true);
            """

        print(f"[EXPORT] Running query for {table_name} -> {export_path_str}")
        con.execute(query)


        # Log số dòng trong file (optional: check trực tiếp từ table)
        row_count = con.execute(f"SELECT COUNT(*) FROM main_mart.{table_name};").fetchone()[0]
        print(f"[EXPORT] {table_name}: {row_count} rows exported.")

    con.close()
    print("[EXPORT] All marts exported successfully.")

# ==== 4. ĐỊNH NGHĨA DAG ====

default_args = {
    "owner": "ecommerce_de",
    "retries": 1,                         # retry mặc định cho các task (có thể override từng task)
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="ecommerce_elt_advanced",
    default_args=default_args,
    description="Production-style ELT: CSV → DuckDB → dbt (staging/core/mart) → tests",
    schedule_interval="0 6 * * *",       # chạy mỗi ngày lúc 06:00 (có thể đổi thành None nếu chỉ muốn trigger tay)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "elt", "duckdb", "dbt", "production"],
) as dag:

    # ---- TASK 1: INGEST RAW ----
    ingest_task = PythonOperator(
        task_id="ingest_raw_csv_to_duckdb",
        python_callable= ingest_raw_to_duckdb,
        provide_context=True,
        retries=2,                        # ingest có thể retry 2 lần
        retry_delay=timedelta(minutes=2),
    )

    # ---- TASK GROUP: DBT RUN (STAGING → CORE → MART) ----
    with TaskGroup(group_id="dbt_run_group", tooltip="Run dbt models by layer") as dbt_run_group:

        dbt_run_staging = BashOperator(
            task_id="dbt_run_staging",
            bash_command=build_dbt_command("run", "staging"),
        )

        dbt_run_core = BashOperator(
            task_id="dbt_run_core",
            bash_command=build_dbt_command("run", "core"),
        )

        dbt_run_mart = BashOperator(
            task_id="dbt_run_mart",
            bash_command=build_dbt_command("run", "mart"),
        )

        # thứ tự: staging → core → mart
        dbt_run_staging >> dbt_run_core >> dbt_run_mart

    # ---- TASK GROUP: DBT TEST (STAGING / CORE / MART) ----
    with TaskGroup(group_id="dbt_test_group", tooltip="Test dbt models by layer") as dbt_test_group:

        dbt_test_staging = BashOperator(
            task_id="dbt_test_staging",
            bash_command=build_dbt_command("test", "staging"),
            retries=0,       # test fail thì fail luôn, không retry (thường là lỗi data, không phải hạ tầng)
        )

        dbt_test_core = BashOperator(
            task_id="dbt_test_core",
            bash_command=build_dbt_command("test", "core"),
            retries=0,
        )

        dbt_test_mart = BashOperator(
            task_id="dbt_test_mart",
            bash_command=build_dbt_command("test", "mart"),
            retries=0,
        )

        # Có thể cho test chạy song song sau khi run xong mart,
        # hoặc test từng layer sau khi run từng layer.
        # Ở đây mình cho test theo thứ tự cho dễ đọc log: staging → core → mart
        dbt_test_staging >> dbt_test_core >> dbt_test_mart

     # ---- TASK: EXPORT MARTS → CSV (CHO BI) ----
    export_marts_task = PythonOperator(
        task_id="export_marts_to_csv",
        python_callable=export_marts_to_csv,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )


    # ---- DEFINE OVERALL DEPENDENCY ----
    # ingest → dbt_run_group → dbt_test_group → export_marts_to_csv
    ingest_task >> dbt_run_group >> dbt_test_group >> export_marts_task