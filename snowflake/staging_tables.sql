USE DATABASE ecommerce_dw;
USE SCHEMA staging;
USE WAREHOUSE ecommerce_wh;

CREATE OR REPLACE TRANSIENT TABLE stg_dim_users (
    user_id STRING,
    name STRING,
    email STRING,
    city STRING,
    signup_date DATE,
    updated_at TIMESTAMP,
    ingestion_date DATE,
    load_ts TIMESTAMP
);

CREATE OR REPLACE TRANSIENT TABLE stg_dim_items (
    item_id STRING,
    item_name STRING,
    category STRING,
    price NUMBER(10,2),
    active BOOLEAN,
    load_ts TIMESTAMP
);

CREATE OR REPLACE TRANSIENT TABLE stg_fact_orders (
    order_id STRING,
    user_id STRING,
    order_date DATE,
    status STRING,
    payment_status STRING,
    total_amount NUMBER(12,2),
    ingestion_date DATE,
    load_ts TIMESTAMP
);

CREATE OR REPLACE TRANSIENT TABLE stg_fact_order_items (
    order_item_id STRING,
    order_id STRING,
    item_id STRING,
    quantity INTEGER,
    price NUMBER(10,2),
    revenue NUMBER(12,2),
    ingestion_date DATE,
    load_ts TIMESTAMP
);
