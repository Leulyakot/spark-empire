# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Diagnostics & Operations
# MAGIC
# MAGIC Ad-hoc queries for monitoring, debugging, and manual operations.
# MAGIC All queries are parameterised via widgets so you can run them without editing cells.
# MAGIC
# MAGIC Run `%run ./_config` first (or set the `ENV` widget) to target the right environment.

# COMMAND ----------
# MAGIC %run ./_config

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Widgets

# COMMAND ----------

dbutils.widgets.dropdown("ENV",        "dev",         ["dev", "uat", "prod"],  "Environment")
dbutils.widgets.text("table_filter",   "",            "Table key filter (blank = all)")
dbutils.widgets.text("lookback_hours", "24",          "Lookback hours for run/error history")

TABLE_FILTER   = dbutils.widgets.get("table_filter").strip()
LOOKBACK_HOURS = int(dbutils.widgets.get("lookback_hours") or 24)

_tbl_where = f"AND source_table LIKE '%{TABLE_FILTER}%'" if TABLE_FILTER else ""

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Watermark Status
# MAGIC Current high-water mark per table — how fresh is each Silver table?

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        source_table,
        last_watermark_ts,
        updated_at,
        updated_by_run_id,
        timestampdiff(HOUR, last_watermark_ts, current_timestamp()) AS hours_behind
    FROM  {log('_watermark_log')}
    WHERE 1=1 {_tbl_where}
    ORDER BY hours_behind DESC
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Recent Run History
# MAGIC One row per pipeline execution. Filterable by table and lookback window.

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        start_time,
        source_table,
        target_table,
        status,
        rows_read,
        rows_after_dedup,
        rows_merged,
        round(duration_seconds, 1)  AS duration_s,
        watermark_from,
        watermark_to,
        run_id
    FROM  {log('_run_log')}
    WHERE start_time >= timestampadd(HOUR, -{LOOKBACK_HOURS}, current_timestamp())
      {_tbl_where}
    ORDER BY start_time DESC
    LIMIT 200
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Run Summary — Status Counts

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        status,
        count(*)                                AS run_count,
        round(avg(duration_seconds), 1)         AS avg_duration_s,
        round(max(duration_seconds), 1)         AS max_duration_s,
        sum(rows_read)                          AS total_rows_read,
        sum(rows_merged)                        AS total_rows_merged
    FROM  {log('_run_log')}
    WHERE start_time >= timestampadd(HOUR, -{LOOKBACK_HOURS}, current_timestamp())
      {_tbl_where}
    GROUP BY status
    ORDER BY status
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Error Log
# MAGIC Most recent errors across all tables, with full stack trace.

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        error_time,
        source_table,
        error_type,
        error_message,
        watermark_from,
        watermark_to,
        run_id,
        stack_trace
    FROM  {log('_error_log')}
    WHERE error_time >= timestampadd(HOUR, -{LOOKBACK_HOURS}, current_timestamp())
      {_tbl_where}
    ORDER BY error_time DESC
    LIMIT 50
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Schema Registry Browser
# MAGIC Inspect active column mappings for any table.

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        table_key,
        bronze_table,
        silver_table,
        col_ordinal,
        bronze_col,
        silver_col,
        data_type,
        nullable,
        col_comment,
        is_active,
        updated_at
    FROM  {REGISTRY_FQ}
    WHERE 1=1 {_tbl_where.replace('source_table', 'table_key')}
    ORDER BY table_key, col_ordinal
"""))

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Operations — Reset a Watermark
# MAGIC
# MAGIC Force a table to reprocess from a specific point.
# MAGIC **Use with care in UAT/PROD** — this will cause Silver rows to be re-merged.
# MAGIC
# MAGIC ```sql
# MAGIC -- Reset to epoch (full reload)
# MAGIC UPDATE <catalog>.pipeline_control._watermark_log
# MAGIC SET    last_watermark_ts = CAST('1970-01-01' AS TIMESTAMP),
# MAGIC        updated_at        = current_timestamp()
# MAGIC WHERE  source_table = '<catalog>.bronze.opportunity';
# MAGIC
# MAGIC -- Reset to a specific point in time
# MAGIC UPDATE <catalog>.pipeline_control._watermark_log
# MAGIC SET    last_watermark_ts = CAST('2024-01-01T00:00:00' AS TIMESTAMP),
# MAGIC        updated_at        = current_timestamp()
# MAGIC WHERE  source_table = '<catalog>.bronze.opportunity';
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Operations — Disable / Re-enable a Column
# MAGIC
# MAGIC Takes effect on the next pipeline run — no code changes needed.
# MAGIC
# MAGIC ```sql
# MAGIC -- Disable
# MAGIC UPDATE <catalog>.pipeline_control._schema_registry
# MAGIC SET    is_active  = false,
# MAGIC        updated_at = current_timestamp()
# MAGIC WHERE  table_key  = 'opportunity'
# MAGIC   AND  bronze_col = 'NextStep';
# MAGIC
# MAGIC -- Re-enable
# MAGIC UPDATE <catalog>.pipeline_control._schema_registry
# MAGIC SET    is_active  = true,
# MAGIC        updated_at = current_timestamp()
# MAGIC WHERE  table_key  = 'opportunity'
# MAGIC   AND  bronze_col = 'NextStep';
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Operations — Add a New Column
# MAGIC
# MAGIC ```sql
# MAGIC INSERT INTO <catalog>.pipeline_control._schema_registry VALUES (
# MAGIC     'opportunity',       -- table_key
# MAGIC     'opportunity',       -- bronze_table
# MAGIC     'opportunity',       -- silver_table
# MAGIC     'Id',                -- pk
# MAGIC     'SystemModstamp',    -- watermark_col
# MAGIC     99,                  -- col_ordinal (appended last)
# MAGIC     'NewCustomField__c', -- bronze_col
# MAGIC     'new_custom_field',  -- silver_col
# MAGIC     'STRING',            -- data_type
# MAGIC     true,                -- nullable
# MAGIC     'My new custom field description',  -- col_comment
# MAGIC     true,                -- is_active
# MAGIC     current_timestamp()  -- updated_at
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC Then `ALTER TABLE` the Silver table to add the column before the next pipeline run:
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE <catalog>.silver.opportunity
# MAGIC ADD COLUMN  new_custom_field STRING COMMENT 'My new custom field description';
# MAGIC ```
