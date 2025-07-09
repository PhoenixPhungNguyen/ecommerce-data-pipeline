{{
    config(
        materialized='incremental',
        unique_key='order_item_pk',
        incremental_strategy='merge',
    )
}}
WITH int_order_item_joined AS (
    SELECT *
    FROM {{ ref('int_order_item_joined') }}
)
SELECT *
FROM int_order_item_joined