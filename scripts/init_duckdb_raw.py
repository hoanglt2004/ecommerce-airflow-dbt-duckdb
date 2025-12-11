import duckdb
from pathlib import Path

# Đường dẫn file DuckDB và CSV
PROJECT_ROOT = Path(__file__).resolve().parents[1]
duckdb_path = PROJECT_ROOT / "duckdb" / "ecommerce.duckdb"
csv_path = PROJECT_ROOT / "data" / "raw" / "ecommerce.csv"

print(f"Using DuckDB file: {duckdb_path}")
print(f"Using CSV file: {csv_path}")

con = duckdb.connect(str(duckdb_path))

# Tạo schema raw nếu chưa có
con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

# Tạo hoặc replace bảng raw.ecommerce
con.execute(f"""
    CREATE OR REPLACE TABLE raw.ecommerce AS
    SELECT *
    FROM read_csv_auto('{csv_path}', HEADER=TRUE);
""")

# Kiểm tra quick: đếm số dòng
rows = con.execute("SELECT COUNT(*) FROM raw.ecommerce;").fetchone()[0]
print(f"Loaded {rows} rows into raw.ecommerce")

con.close()