from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import duckdb
import logging


DB_PATH = "/opt/airflow/duckdb/ecommerce.duckdb"
CSV_PATH = "/opt/airflow/data/ecommerce_raw.csv"

