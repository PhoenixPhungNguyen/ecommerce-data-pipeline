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
    WHERE seller_id IS NOT NULL
)
SELECT *
FROM sellers
{% if is_incremental() %}
WHERE dbt_valid_from > (SELECT MAX(dbt_valid_from) FROM {{ this }})
{% endif %}