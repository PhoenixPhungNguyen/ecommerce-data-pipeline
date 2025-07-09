{{
    config(
        materialized='incremental',
        unique_key='seller_pk',
    )
}}
WITH sellers AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['seller_id','dbt_valid_from']) }} AS seller_pk,
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        dbt_valid_from,
        dbt_valid_to
    FROM {{ ref('scd_sellers') }}
)
SELECT *
FROM sellers