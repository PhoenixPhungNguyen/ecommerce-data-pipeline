WITH source AS (
    SELECT *
    FROM {{ source('landing', 'orders') }}
),
orders_transformed AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    FROM source
)
SELECT *
FROM orders_transformed