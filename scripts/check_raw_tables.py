import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
duckdb_path = PROJECT_ROOT /"duckdb"/ "ecommerce.duckdb"

con = duckdb.connect(str(duckdb_path))

print(con.execute("SELECT * FROM raw.ecommerce LIMIT 5;").fetchdf())

con.close()
