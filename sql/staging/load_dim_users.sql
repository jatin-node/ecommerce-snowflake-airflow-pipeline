-- ============================================================================
-- LOAD DIM USERS (FULL REFRESH)
-- ============================================================================

TRUNCATE TABLE ecommerce_dw.staging.stg_dim_users;

INSERT INTO ecommerce_dw.staging.stg_dim_users (
    user_id,
    name,
    email,
    city,
    signup_date,
    updated_at,
    ingestion_date,
    load_ts
)
SELECT
    $1:user_id::STRING,
    $1:name::STRING,
    $1:email::STRING,
    $1:city::STRING,
    $1:signup_date::DATE,
    TO_TIMESTAMP(
        NULLIF($1:updated_at::STRING, ''),
        'YYYY-MM-DD HH24:MI:SS'
    ),
    TO_DATE($1:ingestion_date::STRING, 'YYYY-MM-DD'),
    CURRENT_TIMESTAMP
FROM @ecommerce_dw.staging.stg_dim_users;
