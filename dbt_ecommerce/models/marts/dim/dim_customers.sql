{{
    config(
        materialized='incremental',
        unique_key='customer_pk',
    )
}}
WITH customers AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_id','dbt_valid_from']) }} AS customer_pk,
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        dbt_valid_from,
        dbt_valid_to
    FROM {{ ref('scd_customers') }}
    WHERE customer_id IS NOT NULL
)
SELECT *
FROM customers
{% if is_incremental() %}
WHERE dbt_valid_from > (SELECT MAX(dbt_valid_from) FROM {{ this }})
{% endif %}