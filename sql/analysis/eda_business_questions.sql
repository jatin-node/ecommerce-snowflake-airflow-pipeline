/* =============================================================================
   Exploratory Analysis: Business Questions
   Purpose:
     Demonstrate how analytics tables can answer common e-commerce questions
    
    - Read-only
============================================================================= */

USE DATABASE ecommerce_dw;
USE SCHEMA analytics;
USE WAREHOUSE ecommerce_wh;

-- ---------------------------------------------------------------------------
-- 1) Overall business snapshot
-- ---------------------------------------------------------------------------
SELECT
    COUNT(order_id)       AS total_orders,
    SUM(total_amount)     AS total_revenue,
    AVG(total_amount)     AS avg_order_value
FROM analytics.fact_orders;

-- ---------------------------------------------------------------------------
-- 2) Daily revenue trend
-- ---------------------------------------------------------------------------
SELECT
    order_date,
    SUM(total_amount) AS daily_revenue,
    COUNT(order_id)   AS orders_count
FROM analytics.fact_orders
GROUP BY order_date
ORDER BY order_date;

-- ---------------------------------------------------------------------------
-- 3) Top customers by total spend
-- ---------------------------------------------------------------------------
SELECT
    u.user_id,
    u.name,
    SUM(o.total_amount) AS total_spent
FROM analytics.fact_orders o
JOIN analytics.dim_users u
  ON o.user_id = u.user_id
GROUP BY u.user_id, u.name
ORDER BY total_spent DESC
LIMIT 5;

-- ---------------------------------------------------------------------------
-- 4) Top selling products by revenue
-- ---------------------------------------------------------------------------
SELECT
    i.item_id,
    i.item_name,
    SUM(oi.quantity) AS units_sold,
    SUM(oi.revenue)  AS total_revenue
FROM analytics.fact_order_items oi
JOIN analytics.dim_items i
  ON oi.item_id = i.item_id
GROUP BY i.item_id, i.item_name
ORDER BY total_revenue DESC
LIMIT 5;

-- ---------------------------------------------------------------------------
-- 5) Order status distribution
-- ---------------------------------------------------------------------------
SELECT
    status,
    COUNT(*) AS order_count
FROM analytics.fact_orders
GROUP BY status
ORDER BY order_count DESC;

-- ---------------------------------------------------------------------------
-- 6) Customers with no orders (data insight)
-- ---------------------------------------------------------------------------
SELECT
    u.user_id,
    u.name
FROM analytics.dim_users u
LEFT JOIN analytics.fact_orders o
  ON u.user_id = o.user_id
WHERE o.order_id IS NULL;

-- ---------------------------------------------------------------------------
-- 7) Revenue contribution by category
-- ---------------------------------------------------------------------------
SELECT
    i.category,
    SUM(oi.revenue) AS category_revenue
FROM analytics.fact_order_items oi
JOIN analytics.dim_items i
  ON oi.item_id = i.item_id
GROUP BY i.category
ORDER BY category_revenue DESC;
