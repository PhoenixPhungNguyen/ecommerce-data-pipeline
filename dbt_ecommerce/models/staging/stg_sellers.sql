WITH source AS (
    SELECT *
    FROM {{ source('landing', 'sellers') }}
),
sellers_transformed AS (
    SELECT
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    FROM source
)
SELECT *
FROM sellers_transformed