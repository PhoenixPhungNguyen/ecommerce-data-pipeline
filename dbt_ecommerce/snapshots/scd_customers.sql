{% snapshot scd_customers %}
{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'customer_id',
            'customer_unique_id',
            'customer_zip_code_prefix',
            'customer_city',
            'customer_state'
        ],
        hard_deletes='invalidate'
    )
}}
SELECT *
FROM {{ ref('stg_customers') }}
{% endsnapshot %}