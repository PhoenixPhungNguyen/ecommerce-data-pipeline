{{
    config(
        materialized='incremental',
        unique_key='payment_pk',
        incremental_strategy='merge',
    )
}}

WITH order_payments AS (
    SELECT *
    FROM {{ ref('stg_order_payments') }}
),
orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
),
customers AS (
    SELECT *
    FROM {{ ref('dim_customers') }}
),
dates AS (
    SELECT *
    FROM {{ ref('dim_dates') }}
),
payments_joined AS (
    SELECT
        op.order_id || '-' || op.payment_sequential AS payment_pk,

        -- Foreign keys
        c.customer_pk AS customer_fk,

        -- order payments
        op.payment_sequential,
        LOWER(TRIM(op.payment_type)) AS payment_type,
        op.payment_installments,
        op.payment_value,

        -- orders
        LOWER(o.order_status) AS order_status,
        CAST(o.order_purchase_timestamp AS DATE) AS order_purchase_date,

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

    FROM order_payments op
    LEFT JOIN orders o ON op.order_id = o.order_id
    LEFT JOIN customers c ON o.customer_id = c.customer_id AND c.dbt_valid_to IS NULL
    LEFT JOIN dates d ON CAST(o.order_purchase_timestamp AS DATE) = d.date
)

SELECT *
FROM payments_joined
