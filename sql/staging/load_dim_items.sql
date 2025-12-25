-- ============================================================================
-- LOAD DIM ITEMS (FULL REFRESH)
-- ============================================================================

TRUNCATE TABLE ecommerce_dw.staging.stg_dim_items;

INSERT INTO ecommerce_dw.staging.stg_dim_items (
    item_id,
    item_name,
    category,
    price,
    active,
    load_ts
)
SELECT
    $1:item_id::STRING,
    $1:item_name::STRING,
    $1:category::STRING,
    $1:price::NUMBER(10,2),
    $1:active::BOOLEAN,
    CURRENT_TIMESTAMP
FROM @ecommerce_dw.staging.stg_dim_items;
