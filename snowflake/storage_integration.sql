/* =============================================================================
   Purpose:     Create Snowflake Storage Integration for S3 Gold Layer
   Scope:       Account-level (run once per Snowflake account)
   Role:        ACCOUNTADMIN

   Description:
   - Creates a Snowflake Storage Integration backed by an AWS IAM Role
   - Enables secure, keyless access from Snowflake to Amazon S3
   - Restricts access strictly to the Gold layer path
============================================================================= */


-- ============================================================================
-- STORAGE INTEGRATION
-- ============================================================================

CREATE OR REPLACE STORAGE INTEGRATION s3_ecommerce_gold_init
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::682033469082:role/analytics_snowflake_role'
STORAGE_ALLOWED_LOCATIONS = (
  's3://ecommerce-dev-data-lake/gold/'
);


DESC INTEGRATION s3_ecommerce_gold_init;
