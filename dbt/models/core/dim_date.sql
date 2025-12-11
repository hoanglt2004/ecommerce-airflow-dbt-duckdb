{{
    config(
        schema='core',
        materialized='table'
    )
}}

WITH date_bounds AS (
    SELECT
        MIN(order_date) as min_date,
        MAX(order_date) AS max_date
    FROM {{ ref('stg_orders') }}
),

date_spine AS (
    SELECT
        range AS date_day
    FROM date_bounds,
    LATERAL generate_series(
        min_date,
        max_date,
        INTERVAL 1 DAY
    ) AS t(range)
)

SELECT
    date_day AS date_key,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    STRFTIME(date_day, '%Y-%m-%d') AS date_ymd,
    STRFTIME(date_day, '%m-%Y') AS month_year,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
    EXTRACT(QUARTER FROM date_day) AS quarter,
FROM date_spine
ORDER BY date_day