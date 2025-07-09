{{
    config(
        materialized='incremental',
        unique_key='review_pk',
        incremental_strategy='merge',
    )
}}

WITH order_reviews AS (
    SELECT *
    FROM {{ ref('stg_order_reviews') }}
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
reviews_joined AS (
    SELECT
        --key
        r.review_id AS review_pk,

        -- customer
        c.customer_pk AS customer_fk,

        -- review
        r.review_score,
        r.review_comment_title,
        r.review_comment_message,
        CAST(r.review_creation_date AS DATE) AS review_creation_date,
        CAST(r.review_answer_timestamp AS DATE) AS review_answer_date,

        -- orders
        o.order_id,
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

    FROM order_reviews r
    LEFT JOIN orders o ON r.order_id = o.order_id
    LEFT JOIN customers c ON o.customer_id = c.customer_id AND c.dbt_valid_to IS NULL
    LEFT JOIN dates d ON CAST(r.review_creation_date AS DATE) = d.date
)

SELECT *
FROM reviews_joined

{% if is_incremental() %}
WHERE review_creation_date > (SELECT MAX(review_creation_date) FROM {{ this }})
{% endif %}
