{{
    config(
        materialized = 'view',
        schema = 'staging'
    )
}}

WITH src AS (
    SELECT *
    FROM {{ source('raw','ecommerce')}}
)

SELECT 
    InvoiceNo AS order_id,
    StockCode AS product_id,
    Description AS product_name,
    Quantity AS quantity,
    UnitPrice AS unit_price,
    Quantity * UnitPrice AS line_revenue
FROM src
WHERE InvoiceNo IS NOT NULL AND StockCode IS NOT NUll