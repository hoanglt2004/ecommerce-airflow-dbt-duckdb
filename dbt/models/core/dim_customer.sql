{{
    config(
        schema='core',
        materialized='table'
    )
}}

SELECT
    customer_id,
    country 
FROM {{ ref('stg_customers') }}