{{
    config(
        schema='core',
        materialized='table'
    )
}}

SELECT  
    product_id,
    product_name
FROM {{ ref('stg_products') }}