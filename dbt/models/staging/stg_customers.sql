{{
    config(
        materialized='view',
        schema='staging'
    )
}}

WITH src AS (
    SELECT *
    FROM {{ source('raw','ecommerce')}}
    WHERE CustomerID IS NOT NULL
),

dedup AS (
    SELECT
        CustomerID AS customer_id,
        Country AS country,
        ROW_NUMBER() OVER(
            PARTITION BY CustomerID
            ORDER BY InvoiceDate DESC
        ) AS rn
    FROM src
)

SELECT 
    customer_id,
    country
FROM dedup
WHERE rn = 1