-- ============================================================================
-- LOAD FACT ORDER ITEMS (INCREMENTAL)
-- ============================================================================

TRUNCATE TABLE ecommerce_dw.staging.stg_fact_order_items;

INSERT INTO ecommerce_dw.staging.stg_fact_order_items (
    order_item_id,
    order_id,
    item_id,
    quantity,
    price,
    revenue,
    ingestion_date,
    load_ts
)
SELECT
    $1:order_item_id::STRING,
    $1:order_id::STRING,
    $1:item_id::STRING,
    $1:quantity::INTEGER,
    $1:price::NUMBER(10,2),
    $1:revenue::NUMBER(12,2),
    TO_DATE($1:ingestion_date::STRING, 'YYYY-MM-DD'),
    CURRENT_TIMESTAMP
FROM @ecommerce_dw.staging.stg_fact_order_items
WHERE TO_DATE($1:ingestion_date::STRING, 'YYYY-MM-DD') >
(
    SELECT last_ingestion_date
    FROM ecommerce_dw.analytics.load_control
    WHERE table_name = 'fact_order_items'
);
