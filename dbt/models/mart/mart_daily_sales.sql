{{
    config(
        schema='mart',
        materialized='table'
    )
}}

SELECT
    f.order_date_key AS date,
    SUM(f.line_revenue) AS daily_revenue,
    SUM(f.quantity) AS total_quantity,
    COUNT(DISTINCT f.order_id) AS total_orders,
FROM {{ ref('fact_order_items') }} f
GROUP BY f.order_date_key
ORDER BY date