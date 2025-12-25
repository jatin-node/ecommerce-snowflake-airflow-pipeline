-- Fail if any staging table is empty

SELECT 'stg_dim_users empty'
WHERE NOT EXISTS (
    SELECT 1 FROM ecommerce_dw.staging.stg_dim_users
)
UNION ALL
SELECT 'stg_dim_items empty'
WHERE NOT EXISTS (
    SELECT 1 FROM ecommerce_dw.staging.stg_dim_items
)