{{ config(
    schema='core',
    materialized='table'
) }}

WITH orders AS (
    SELECT
        order_id,
        customer_id,
        order_date
    FROM {{ ref('stg_orders') }}
),

items AS (
    SELECT
        order_id,
        product_id,
        quantity,
        unit_price,
        line_revenue
    FROM {{ ref('stg_order_items') }}
),

joined AS (
    SELECT
        i.order_id,
        o.customer_id,
        o.order_date AS order_date_key,
        i.product_id,
        i.quantity,
        i.unit_price,
        i.line_revenue
    FROM items i
    INNER JOIN orders o          -- ĐỔI THÀNH INNER JOIN
        ON i.order_id = o.order_id
    WHERE
        o.order_date IS NOT NULL
        AND o.customer_id IS NOT NULL
)

SELECT *
FROM joined