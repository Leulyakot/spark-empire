# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Control Tables Setup
# MAGIC
# MAGIC Creates all four pipeline control tables under `{CATALOG}.pipeline_control`.
# MAGIC Safe to re-run — all statements use `CREATE TABLE IF NOT EXISTS`.
# MAGIC
# MAGIC | Table | Purpose |
# MAGIC |---|---|
# MAGIC | `_watermark` | Incremental load state — one row per Bronze table |
# MAGIC | `_run_log` | One row per pipeline execution per table |
# MAGIC | `_error_log` | Append-only exception log |
# MAGIC | `_schema_registry` | Bronze → Silver column mapping for all tables |

# COMMAND ----------
# MAGIC %run ./_config

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{LOGGING_SCHEMA}")
print(f"Schema: {CATALOG}.{LOGGING_SCHEMA}")

# COMMAND ----------
# MAGIC %md ### `_watermark`

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {log('_watermark')} (
    source_table       STRING    NOT NULL  COMMENT 'Fully-qualified Bronze table name',
    watermark_col      STRING    NOT NULL  COMMENT 'Column used as the high-water mark',
    last_watermark_ts  TIMESTAMP NOT NULL  COMMENT 'Highest watermark value successfully written to Silver',
    environment        STRING    NOT NULL  COMMENT 'dev | uat | prod',
    updated_at         TIMESTAMP NOT NULL  COMMENT 'When this row was last updated',
    updated_by_run_id  STRING             COMMENT 'FK → _run_log.run_id'
)
USING DELTA
COMMENT 'Incremental load state. Only advanced after a confirmed successful Silver MERGE.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'pipeline.owner' = 'data_engineering')
""")
print(f"OK: {log('_watermark')}")

# COMMAND ----------
# MAGIC %md ### `_run_log`

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {log('_run_log')} (
    run_id            STRING    NOT NULL  COMMENT 'UUID generated at pipeline start',
    object_name       STRING    NOT NULL  COMMENT 'Fully-qualified Bronze table being processed',
    status            STRING    NOT NULL  COMMENT 'RUNNING | SUCCEEDED | FAILED | SKIPPED',
    rows_written      LONG               COMMENT 'Net rows inserted or updated in Silver',
    started_at        TIMESTAMP NOT NULL  COMMENT 'Run start (UTC)',
    completed_at      TIMESTAMP          COMMENT 'Run end (UTC)',
    duration_seconds  DOUBLE             COMMENT 'Wall-clock seconds',
    environment       STRING    NOT NULL  COMMENT 'dev | uat | prod'
)
USING DELTA
COMMENT 'One row per pipeline execution per table. Status updated in-place via MERGE.'
TBLPROPERTIES ('pipeline.owner' = 'data_engineering')
""")
print(f"OK: {log('_run_log')}")

# COMMAND ----------
# MAGIC %md ### `_error_log`

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {log('_error_log')} (
    run_id         STRING    NOT NULL  COMMENT 'FK → _run_log.run_id',
    object_name    STRING    NOT NULL  COMMENT 'Fully-qualified Bronze table that failed',
    error_type     STRING             COMMENT 'Exception class name',
    error_message  STRING             COMMENT 'str(exception)',
    stack_trace    STRING             COMMENT 'Full traceback',
    environment    STRING    NOT NULL  COMMENT 'dev | uat | prod',
    occurred_at    TIMESTAMP NOT NULL  COMMENT 'UTC timestamp of the exception'
)
USING DELTA
COMMENT 'Append-only error log. Never deleted; used for alerting and replay decisions.'
TBLPROPERTIES ('pipeline.owner' = 'data_engineering')
""")
print(f"OK: {log('_error_log')}")

# COMMAND ----------
# MAGIC %md ### `_schema_registry`

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {log('_schema_registry')} (
    table_key          STRING   NOT NULL  COMMENT 'Registry key, e.g. account',
    bronze_table       STRING   NOT NULL  COMMENT 'Unqualified Bronze table name',
    silver_table       STRING   NOT NULL  COMMENT 'Unqualified Silver table name',
    pk                 STRING   NOT NULL  COMMENT 'Primary key column in Bronze',
    watermark_col      STRING   NOT NULL  COMMENT 'Column driving incremental load',
    surrogate_key_col  STRING   NOT NULL  COMMENT 'IDENTITY column name for this Silver table, e.g. account_sk',
    col_ordinal        INT      NOT NULL  COMMENT 'Column order for DDL and SELECT',
    bronze_col         STRING   NOT NULL  COMMENT 'Column name as it exists in Bronze',
    silver_col         STRING   NOT NULL  COMMENT 'Column name in Silver',
    data_type          STRING   NOT NULL  COMMENT 'Spark SQL type: STRING | BOOLEAN | BIGINT | DECIMAL(18,4) | DATE | TIMESTAMP',
    nullable           BOOLEAN  NOT NULL  COMMENT 'Whether the Silver column allows NULLs',
    col_comment        STRING             COMMENT 'Business description',
    is_active          BOOLEAN  NOT NULL  COMMENT 'FALSE = exclude column without deleting the row',
    updated_at         TIMESTAMP          COMMENT 'Row last-modified timestamp'
)
USING DELTA
COMMENT 'Single source of truth for all Bronze → Silver column mappings.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true', 'pipeline.owner' = 'data_engineering')
""")
print(f"OK: {log('_schema_registry')}")

# COMMAND ----------
# MAGIC %md ### Verify

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name, table_type, created
    FROM   {CATALOG}.information_schema.tables
    WHERE  table_schema = '{LOGGING_SCHEMA}'
    ORDER  BY table_name
"""))
