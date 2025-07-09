{{
    config(
        materialized='table'
    )
}}

SELECT DISTINCT
    TRIM(product_category_name) AS product_category_name,
    TRIM(product_category_name_english) AS product_category_name_english
FROM {{ ref('stg_product_category_name_translation') }}