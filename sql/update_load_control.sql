/* =============================================================================
   Purpose: Update load_control after successful analytics load
============================================================================= */

-- ============================================================================
-- Update fact_orders
-- ============================================================================
UPDATE ecommerce_dw.analytics.load_control
SET
    last_ingestion_date = (
        SELECT MAX(ingestion_date)
        FROM ecommerce_dw.staging.stg_fact_orders
    ),
    last_run_ts = CURRENT_TIMESTAMP,
    status = 'SUCCESS'
WHERE table_name = 'fact_orders';

-- ============================================================================
-- Update fact_order_items
-- ============================================================================
UPDATE ecommerce_dw.analytics.load_control
SET
    last_ingestion_date = (
        SELECT MAX(ingestion_date)
        FROM ecommerce_dw.staging.stg_fact_order_items
    ),
    last_run_ts = CURRENT_TIMESTAMP,
    status = 'SUCCESS'
WHERE table_name = 'fact_order_items';

-- Debug / visibility
SELECT * FROM ecommerce_dw.analytics.load_control;
