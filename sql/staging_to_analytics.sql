/* =============================================================================
   Purpose: Load data from STAGING to ANALYTICS (final warehouse tables)
============================================================================= */

-- ============================================================================
-- DIMENSIONS (FULL REFRESH)
-- ============================================================================

-- =================================================
-- DIM USERS
-- =================================================
TRUNCATE TABLE ecommerce_dw.analytics.dim_users;

INSERT INTO ecommerce_dw.analytics.dim_users (
    user_id,
    name,
    email,
    city,
    signup_date,
    updated_at,
    ingestion_date
)
SELECT
    user_id,
    name,
    email,
    city,
    signup_date,
    updated_at,
    ingestion_date
FROM ecommerce_dw.staging.stg_dim_users;

-- =================================================
-- DIM ITEMS
-- =================================================
TRUNCATE TABLE ecommerce_dw.analytics.dim_items;

INSERT INTO ecommerce_dw.analytics.dim_items (
    item_id,
    item_name,
    category,
    price,
    active
)
SELECT
    item_id,
    item_name,
    category,
    price,
    active
FROM ecommerce_dw.staging.stg_dim_items;

-- ============================================================================
-- FACT TABLES (APPEND-ONLY)
-- ============================================================================

-- =================================================
-- FACT ORDERS
-- =================================================
INSERT INTO ecommerce_dw.analytics.fact_orders (
    order_id,
    user_id,
    order_date,
    status,
    payment_status,
    total_amount,
    ingestion_date
)
SELECT
    order_id,
    user_id,
    order_date,
    status,
    payment_status,
    total_amount,
    ingestion_date
FROM ecommerce_dw.staging.stg_fact_orders;

-- =================================================
-- FACT ORDER ITEMS
-- =================================================
INSERT INTO ecommerce_dw.analytics.fact_order_items (
    order_item_id,
    order_id,
    item_id,
    quantity,
    price,
    revenue,
    ingestion_date
)
SELECT
    order_item_id,
    order_id,
    item_id,
    quantity,
    price,
    revenue,
    ingestion_date
FROM ecommerce_dw.staging.stg_fact_order_items;
