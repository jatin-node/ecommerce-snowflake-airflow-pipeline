-- ============================================================================
-- LOAD FACT ORDERS (INCREMENTAL)
-- ============================================================================

TRUNCATE TABLE ecommerce_dw.staging.stg_fact_orders;

INSERT INTO ecommerce_dw.staging.stg_fact_orders (
    order_id,
    user_id,
    order_date,
    status,
    payment_status,
    total_amount,
    ingestion_date,
    load_ts
)
SELECT
    $1:order_id::STRING,
    $1:user_id::STRING,
    $1:order_date::DATE,
    $1:status::STRING,
    $1:payment_status::STRING,
    $1:total_amount::NUMBER(12,2),
    TO_DATE($1:ingestion_date::STRING, 'YYYY-MM-DD'),
    CURRENT_TIMESTAMP
FROM @ecommerce_dw.staging.stg_fact_orders
WHERE TO_DATE($1:ingestion_date::STRING, 'YYYY-MM-DD') >
(
    SELECT last_ingestion_date
    FROM ecommerce_dw.analytics.load_control
    WHERE table_name = 'fact_orders'
);
