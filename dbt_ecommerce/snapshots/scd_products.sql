{% snapshot scd_products %}
{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='product_id',
        strategy='check',
        check_cols=[
            'product_id',
            'product_category_name',
            'product_name_length',
            'product_description_length',
            'product_photos_qty',
            'product_weight_g',
            'product_length_cm',
            'product_height_cm',
            'product_width_cm'
        ],
        hard_deletes='invalidate'
    )
}}
SELECT *
FROM {{ ref('stg_products') }}
{% endsnapshot %}