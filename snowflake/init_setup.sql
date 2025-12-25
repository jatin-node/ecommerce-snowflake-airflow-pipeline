-- ============================================================================
-- DATABASE AND SCHEMA
-- ============================================================================
CREATE DATABASE IF NOT EXISTS ecommerce_dw;
USE DATABASE ecommerce_dw;

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ============================================================================
-- WAREHOUSE
-- ============================================================================
CREATE OR REPLACE WAREHOUSE ecommerce_wh
WITH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Activate warehouse
USE WAREHOUSE ecommerce_wh;

-- ============================================================================
-- FILE FORMAT (Parquet)
-- ============================================================================
CREATE OR REPLACE FILE FORMAT staging.parquet_format
    TYPE = 'PARQUET';

-- ============================================================================
-- EXTERNAL STAGES
-- ============================================================================
CREATE OR REPLACE STAGE staging.stg_dim_users
    STORAGE_INTEGRATION = s3_ecommerce_gold_init
    URL = 's3://ecommerce-dev-data-lake/gold/dim_users/'
    FILE_FORMAT = staging.parquet_format;

CREATE OR REPLACE STAGE staging.stg_dim_items
    STORAGE_INTEGRATION = s3_ecommerce_gold_init
    URL = 's3://ecommerce-dev-data-lake/gold/dim_items/'
    FILE_FORMAT = staging.parquet_format;

CREATE OR REPLACE STAGE staging.stg_fact_orders
    STORAGE_INTEGRATION = s3_ecommerce_gold_init
    URL = 's3://ecommerce-dev-data-lake/gold/fact_orders/'
    FILE_FORMAT = staging.parquet_format;

CREATE OR REPLACE STAGE staging.stg_fact_order_items
    STORAGE_INTEGRATION = s3_ecommerce_gold_init
    URL = 's3://ecommerce-dev-data-lake/gold/fact_order_items/'
    FILE_FORMAT = staging.parquet_format;

-- ============================================================================
-- LOAD CONTROL
-- ============================================================================
CREATE OR REPLACE TABLE analytics.load_control (
    table_name STRING,
    last_ingestion_date DATE,
    last_run_ts TIMESTAMP,
    status STRING
);

INSERT INTO analytics.load_control
SELECT 'fact_orders', '1900-01-01', CURRENT_TIMESTAMP, 'INIT'
WHERE NOT EXISTS (
    SELECT 1 FROM analytics.load_control WHERE table_name = 'fact_orders'
);

INSERT INTO analytics.load_control
SELECT 'fact_order_items', '1900-01-01', CURRENT_TIMESTAMP, 'INIT'
WHERE NOT EXISTS (
    SELECT 1 FROM analytics.load_control WHERE table_name = 'fact_order_items'
);

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================
CREATE OR REPLACE TABLE analytics.dim_users (
    user_id         STRING      NOT NULL,
    name            STRING,
    email           STRING,
    city            STRING,
    signup_date     DATE,
    updated_at      TIMESTAMP,
    ingestion_date  DATE,

    created_ts      TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_dim_users PRIMARY KEY (user_id)
);

CREATE OR REPLACE TABLE analytics.dim_items (
    item_id     STRING      NOT NULL,
    item_name   STRING,
    category    STRING,
    price       NUMBER(10,2),
    active      BOOLEAN,

    created_ts  TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_dim_items PRIMARY KEY (item_id)
);

-- ============================================================================
-- FACT TABLES
-- ============================================================================
CREATE OR REPLACE TABLE analytics.fact_orders (
    order_id        STRING      NOT NULL,
    user_id         STRING      NOT NULL,
    order_date      DATE,
    status          STRING,
    payment_status  STRING,
    total_amount    NUMBER(12,2),
    ingestion_date  DATE,

    created_ts      TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_fact_orders PRIMARY KEY (order_id),
    CONSTRAINT fk_orders_user FOREIGN KEY (user_id)
        REFERENCES analytics.dim_users (user_id)
);


CREATE OR REPLACE TABLE analytics.fact_order_items (
    order_item_id   STRING      NOT NULL,
    order_id        STRING      NOT NULL,
    item_id         STRING      NOT NULL,
    quantity        INTEGER,
    price           NUMBER(10,2),
    revenue         NUMBER(12,2),
    ingestion_date  DATE,

    created_ts      TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_fact_order_items PRIMARY KEY (order_item_id),
    CONSTRAINT fk_oi_order FOREIGN KEY (order_id)
        REFERENCES analytics.fact_orders (order_id),
    CONSTRAINT fk_oi_item FOREIGN KEY (item_id)
        REFERENCES analytics.dim_items (item_id)
);
