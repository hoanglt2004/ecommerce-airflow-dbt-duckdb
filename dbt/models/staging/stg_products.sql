{{
    config(
        materialized='view',
        schema='staging'
    )
}}

WITH src AS (
    SELECT *
    FROM {{ source('raw','ecommerce')}}
    WHERE StockCode IS NOT NULL
),

dedup AS (
    SELECT  
        StockCode AS product_id,
        Description AS product_name,
        ROW_NUMBER() OVER(
            PARTITION BY StockCode
            ORDER BY InvoiceDate DESC
        ) AS rn
    FROM src
)

SELECT 
    product_id,
    product_name
FROM dedup
WHERE rn=1