# Architecture Overview

## 1. Business Use Case

We model an e-commerce analytics warehouse with the following entities:

- Customers
- Products
- Orders
- Order items

Main business questions:
- Daily / monthly revenue
- Top products by revenue and quantity
- Customer lifetime value (LTV)
- Order status distribution

## 2. Tech Stack

- Orchestration: Apache Airflow
- Transformations & Modeling: dbt
- Warehouse: DuckDB
- Version Control: Git + GitHub

## 3. High-level ELT Flow

1. **Ingest (Load):**
   - Load CSV files from `data/raw/` into DuckDB schema `raw`.

2. **Staging:**
   - Create cleaned and standardized `stg_` models in schema `staging` using dbt.

3. **Core (Dimensional Models):**
   - Build Kimball-style facts and dimensions in schema `core`:
     - `dim_customer`, `dim_product`, `dim_date`
     - `fact_orders`

4. **Data Marts:**
   - Build analytics-ready tables in schema `mart`:
     - `mart_daily_sales`
     - `mart_product_performance`
     - `mart_customer_ltv`

## 4. DuckDB Layout

- File: `ecommerce.duckdb` (located in the project root)
- Schemas:
  - `raw`
  - `staging`
  - `core`
  - `mart`

## 5. Source Dataset

We use a single transactional CSV file with the following columns:

- InvoiceNo (order identifier)
- StockCode (product identifier)
- Description (product description)
- Quantity (number of units purchased in this line)
- InvoiceDate (order timestamp)
- UnitPrice (price per unit)
- CustomerID (customer identifier)
- Country (customer country)

Each row represents a single order line item.

From this raw table, we will derive logical entities:

- Orders: distinct InvoiceNo
- Order items: line-level data per InvoiceNo + StockCode
- Customers: distinct CustomerID + Country
- Products: distinct StockCode + Description
