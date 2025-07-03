WITH source AS (
    SELECT *
    FROM {{ source('landing', 'order_payments') }}
),
order_payments_transformed AS (
    SELECT
       order_id,
       payment_sequential,
       payment_type,
       payment_installments,
       payment_value 
    FROM source
)
SELECT *
FROM order_payments_transformed