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
    CustomerID AS customer_id,
    strptime(InvoiceDate, '%m/%d/%Y %H:%M') AS order_datetime,
    CAST(strptime(InvoiceDate, '%m/%d/%Y %H:%M') AS DATE) AS order_date,
    Country AS country
FROM src
WHERE InvoiceNo IS NOT NULL
AND CustomerID IS NOT NULL