WITH source AS (
    SELECT *
    FROM {{ source('landing', 'product_category_name_translation') }}
),
product_category_name_translation_transformed AS (
    SELECT
        product_category_name,
        product_category_name_english
    FROM source
)
SELECT *
FROM product_category_name_translation_transformed