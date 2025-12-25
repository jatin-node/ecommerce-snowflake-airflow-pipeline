from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_snowflake_batch_etl",
    description="End-to-end ETL: S3 -> Snowflake Staging -> Analytics",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["snowflake", "etl", "ecommerce"],
) as dag:

    # -----------------------------
    # STAGING LOADS (FROM S3 STAGES)
    # -----------------------------
    load_dim_users = SQLExecuteQueryOperator(
        task_id="load_dim_users",
        conn_id="snowflake_conn",
        sql="sql/staging/load_dim_users.sql",
    )

    load_dim_items = SQLExecuteQueryOperator(
        task_id="load_dim_items",
        conn_id="snowflake_conn",
        sql="sql/staging/load_dim_items.sql",
    )

    load_fact_orders = SQLExecuteQueryOperator(
        task_id="load_fact_orders",
        conn_id="snowflake_conn",
        sql="sql/staging/load_fact_orders.sql",
    )

    load_fact_order_items = SQLExecuteQueryOperator(
        task_id="load_fact_order_items",
        conn_id="snowflake_conn",
        sql="sql/staging/load_fact_order_items.sql",
    )

    # -----------------------------
    # VALIDATION
    # -----------------------------
    validate_staging = SQLExecuteQueryOperator(
        task_id="validate_staging",
        conn_id="snowflake_conn",
        sql="sql/validate/validate_staging.sql",
    )

    # -----------------------------
    # ANALYTICS LOADS
    # -----------------------------
    load_analytics = SQLExecuteQueryOperator(
        task_id="load_analytics",
        conn_id="snowflake_conn",
        sql="sql/analytics/staging_to_analytics.sql",
    )

    # -----------------------------
    # LOAD CONTROL UPDATE
    # -----------------------------
    update_load_control = SQLExecuteQueryOperator(
        task_id="update_load_control",
        conn_id="snowflake_conn",
        sql="sql/analytics/update_load_control.sql",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # -----------------------------
    # DEPENDENCIES
    # -----------------------------
    (
        load_dim_users
        >> load_dim_items
        >> load_fact_orders
        >> load_fact_order_items
        >> validate_staging
        >> load_analytics
        >> update_load_control
    )
