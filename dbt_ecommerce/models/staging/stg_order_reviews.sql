WITH source AS (
    SELECT *
    FROM {{ source('landing', 'order_reviews') }}
),
order_reviews_transformed AS (
    SELECT
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp
    FROM source
)
SELECT *
FROM order_reviews_transformed