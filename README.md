# Modern ELT Data Warehouse (Airflow + dbt + DuckDB)

This project implements a modern ELT data warehouse for an e-commerce use case using:

- **Airflow** for orchestration
- **dbt** for transformations & data modeling
- **DuckDB** as the analytical warehouse
- (Optionally) a BI tool for dashboards

## Business Context

An e-commerce platform with data about:

- Customers
- Products
- Orders
- Order items

The goal is to build an ELT pipeline:

1. Ingest raw data into DuckDB
2. Build staging models
3. Build dimensional models (facts & dimensions)
4. Build data marts for analytics (revenue, product performance, customer LTV)
5. Orchestrate end-to-end pipeline using Apache Airflow

## Dataset

The project uses a single transactional CSV file (Online Retail-style) where each row represents an order line item with columns:

InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country.
