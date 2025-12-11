{{
    config(
        schema='mart',
        materialized='table'
    )
}}

SELECT 
    c.customer_id,
    c.country,
    SUM(f.line_revenue) AS lifetime_value,
    COUNT(DISTINCT f.order_id) AS total_orders,
    MIN(f.order_date_key) AS first_order_date,
    MAX(f.order_date_key) AS last_order_date
FROM {{ ref('fact_order_items') }} f
LEFT JOIN {{ ref('dim_customer') }} c
    ON f.customer_id = c.customer_id
GROUP BY c.customer_id, c.country
ORDER BY lifetime_value DESC