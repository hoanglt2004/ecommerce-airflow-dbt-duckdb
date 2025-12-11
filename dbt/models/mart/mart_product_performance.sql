{{
    config(
        schema='mart',
        materialized='table'
    )
}}

SELECT
    p.product_id,
    p.product_name,
    SUM(f.quantity) AS total_quantity_sold,
    SUM(f.line_revenue) AS total_revenue,
    COUNT(DISTINCT f.order_id) AS orders_count
FROM {{ ref('fact_order_items') }} f
LEFT JOIN {{ ref('dim_product') }} p
ON f.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_revenue DESC