{{
    config(
        materialized='incremental',
        unique_key='order_item_pk',
        incremental_strategy='merge',
    )
}}

WITH order_items AS (
    SELECT *
    FROM {{ ref('stg_order_items') }}
),
orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
),
customers AS (
    SELECT *
    FROM {{ ref('dim_customers') }}
),
products AS (
    SELECT *
    FROM {{ ref('dim_products') }}
),
sellers AS (
    SELECT *
    FROM {{ ref('dim_sellers') }}
),
dates AS (
    SELECT *
    FROM {{ ref('dim_dates') }}
),
order_items_joined AS (
    SELECT
        CONCAT(oi.order_id, '-', oi.order_item_id) AS order_item_pk,

        -- keys
        c.customer_pk AS customer_fk,
        p.product_pk AS product_fk,
        s.seller_pk AS seller_fk,

        -- order items
        oi.order_id,
        oi.order_item_id,
        oi.price,
        oi.freight_value,
        CAST(oi.shipping_limit_date AS DATE) AS shipping_limit_date,

        -- orders
        LOWER(o.order_status) AS order_status,
        CAST(o.order_purchase_timestamp AS DATE) AS order_purchase_date,
        CAST(o.order_approved_at AS DATE) AS order_approved_date,
        CAST(o.order_delivered_carrier_date AS DATE) AS delivered_carrier_date,
        CAST(o.order_delivered_customer_date AS DATE) AS delivered_customer_date,
        CAST(o.order_estimated_delivery_date AS DATE) AS estimated_delivery_date,

        -- date
        d.calendar_week,
        d.calendar_month,
        d.calendar_quarter,
        d.calendar_week_start_date,
        d.calendar_week_end_date,
        d.calendar_month_start_date,
        d.calendar_month_end_date,
        d.calendar_quarter_start_date,
        d.calendar_quarter_end_date

    FROM order_items oi
    LEFT JOIN orders o ON oi.order_id = o.order_id
    LEFT JOIN customers c ON o.customer_id = c.customer_id AND c.dbt_valid_to IS NULL
    LEFT JOIN products p ON oi.product_id = p.product_id AND p.dbt_valid_to IS NULL
    LEFT JOIN sellers s ON oi.seller_id = s.seller_id AND s.dbt_valid_to IS NULL
    LEFT JOIN dates d ON CAST(o.order_purchase_timestamp AS DATE) = d.date
)

SELECT *
FROM order_items_joined

{% if is_incremental() %}
WHERE order_purchase_date > (SELECT MAX(order_purchase_date) FROM {{ this }})
{% endif %}
