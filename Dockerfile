FROM apache/airflow:2.7.3

USER airflow

# Cài duckdb + pandas
RUN pip install --no-cache-dir --only-binary=:all: "duckdb<1.3" pandas

# Cài dbt-core + dbt-duckdb
RUN pip install --no-cache-dir "dbt-core==1.6.0" "dbt-duckdb==1.6.0"
