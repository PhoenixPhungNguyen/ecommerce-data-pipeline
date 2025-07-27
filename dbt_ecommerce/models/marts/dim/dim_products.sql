{{
    config(
        materialized='incremental',
        unique_key='product_pk'
    )
}}
WITH products AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['product_id','dbt_valid_from']) }} AS product_pk,
        product_id,
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        dbt_valid_from,
        dbt_valid_to
    FROM {{ ref('scd_products') }}
    WHERE product_id IS NOT NULL
)
SELECT *
FROM products
{% if is_incremental() %}
WHERE dbt_valid_from > (SELECT MAX(dbt_valid_from) FROM {{ this }})
{% endif %}