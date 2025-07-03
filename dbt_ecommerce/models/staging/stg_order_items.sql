WITH source AS (
    SELECT *
    FROM {{ source('landing', 'order_items') }}
),
order_items_transformed AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value
    FROM source
)
SELECT *
FROM order_items_transformed