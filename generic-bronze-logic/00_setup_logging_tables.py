# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — Logging & Control Tables
# MAGIC
# MAGIC Creates three shared control tables used by every Bronze → Silver run:
# MAGIC
# MAGIC | Table | Purpose |
# MAGIC |---|---|
# MAGIC | `_watermark_log` | Tracks the highest `SystemModstamp` successfully processed per source table |
# MAGIC | `_run_log` | One row per pipeline execution — status, row counts, duration |
# MAGIC | `_error_log` | Captures exceptions with stack traces for alerting / replay |
# MAGIC
# MAGIC Run once per environment. Safe to re-run (uses `CREATE TABLE IF NOT EXISTS`).

# COMMAND ----------

# ─────────────────────────────────────────────────────────────
# CONFIGURATION  — edit these for your environment
# ─────────────────────────────────────────────────────────────
CATALOG        = "my_catalog"       # Unity Catalog name
LOGGING_SCHEMA = "pipeline_control" # Schema that holds all three log tables

# ─────────────────────────────────────────────────────────────
# Bootstrap: catalog + schema must already exist (or create here)
# ─────────────────────────────────────────────────────────────
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{LOGGING_SCHEMA}")
print(f"Schema ready: {CATALOG}.{LOGGING_SCHEMA}")

# COMMAND ----------
# MAGIC %md ### `_watermark_log`
# MAGIC One authoritative row per `source_table`.  
# MAGIC Updated (MERGE) at the **end** of every successful Silver write so a failed run never advances the watermark.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{LOGGING_SCHEMA}._watermark_log (
    source_table        STRING    NOT NULL  COMMENT 'Fully-qualified Bronze table name, e.g. my_catalog.bronze.account',
    watermark_column    STRING    NOT NULL  COMMENT 'Column used as the high-water mark (always SystemModstamp for SF)',
    last_watermark_ts   TIMESTAMP NOT NULL  COMMENT 'Highest SystemModstamp successfully written to Silver',
    updated_at          TIMESTAMP NOT NULL  COMMENT 'Wall-clock time this row was last updated',
    updated_by_run_id   STRING              COMMENT 'run_id from _run_log that set this watermark'
)
USING DELTA
COMMENT 'High-water mark per Bronze table.  Never updated on a failed run.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'false',
    'pipeline.owner'             = 'data_engineering'
)
""")
print("Created: _watermark_log")

# COMMAND ----------
# MAGIC %md ### `_run_log`
# MAGIC One row inserted at the **start** of a run (status = RUNNING), then UPDATED to SUCCEEDED / FAILED on completion.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{LOGGING_SCHEMA}._run_log (
    run_id              STRING    NOT NULL  COMMENT 'UUID generated at pipeline start',
    pipeline_name       STRING    NOT NULL  COMMENT 'e.g. bronze_to_silver_salesforce',
    source_table        STRING    NOT NULL  COMMENT 'Fully-qualified Bronze table being processed',
    target_table        STRING    NOT NULL  COMMENT 'Fully-qualified Silver table being written',
    status              STRING    NOT NULL  COMMENT 'RUNNING | SUCCEEDED | FAILED | SKIPPED',
    watermark_from      TIMESTAMP           COMMENT 'Lower-bound watermark used in this run (exclusive)',
    watermark_to        TIMESTAMP           COMMENT 'Upper-bound watermark used in this run (inclusive)',
    rows_read           LONG                COMMENT 'Rows read from Bronze in this window',
    rows_after_dedup    LONG                COMMENT 'Rows after deduplication (latest per Id)',
    rows_merged         LONG                COMMENT 'Net rows inserted or updated in Silver',
    start_time          TIMESTAMP NOT NULL  COMMENT 'Run start (UTC)',
    end_time            TIMESTAMP           COMMENT 'Run end (UTC)',
    duration_seconds    DOUBLE              COMMENT 'Wall-clock seconds',
    triggered_by        STRING              COMMENT 'job_id, notebook path, or manual',
    notes               STRING              COMMENT 'Free-text — e.g. first-load, backfill'
)
USING DELTA
COMMENT 'One row per pipeline execution per table.  Status updated in-place via MERGE.'
TBLPROPERTIES ('pipeline.owner' = 'data_engineering')
""")
print("Created: _run_log")

# COMMAND ----------
# MAGIC %md ### `_error_log`
# MAGIC Append-only.  Multiple errors can be logged per run (e.g. schema mismatch + retry).

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{LOGGING_SCHEMA}._error_log (
    error_id            STRING    NOT NULL  COMMENT 'UUID for this error event',
    run_id              STRING    NOT NULL  COMMENT 'FK → _run_log.run_id',
    pipeline_name       STRING    NOT NULL,
    source_table        STRING    NOT NULL,
    target_table        STRING,
    error_time          TIMESTAMP NOT NULL  COMMENT 'UTC timestamp of the exception',
    error_type          STRING              COMMENT 'Exception class name',
    error_message       STRING              COMMENT 'str(exception)',
    stack_trace         STRING              COMMENT 'Full traceback',
    watermark_from      TIMESTAMP           COMMENT 'Watermark range active when error occurred',
    watermark_to        TIMESTAMP,
    context_json        STRING              COMMENT 'Optional JSON bag for extra diagnostic key-values'
)
USING DELTA
COMMENT 'Append-only error log.  Never deleted; used for alerting and replay decisions.'
TBLPROPERTIES ('pipeline.owner' = 'data_engineering')
""")
print("Created: _error_log")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Seed initial watermarks (first-time only)
# MAGIC
# MAGIC Inserts a zero-epoch watermark for every Salesforce source table so the pipeline always has a valid starting point.  
# MAGIC Uses `INSERT INTO ... WHERE NOT EXISTS` pattern — safe to re-run.

# COMMAND ----------

SF_BRONZE_TABLES = [
    "account", "contact", "lead", "opportunity", "opportunitylineitem",
    "product2", "pricebook2", "pricebookentry", "case", "casecomment",
    "task", "event", "campaign", "campaignmember", "user"
]

BRONZE_SCHEMA = "bronze"
EPOCH_TS      = "1970-01-01T00:00:00.000+0000"

for tbl in SF_BRONZE_TABLES:
    fq_source = f"{CATALOG}.{BRONZE_SCHEMA}.{tbl}"
    spark.sql(f"""
        MERGE INTO {CATALOG}.{LOGGING_SCHEMA}._watermark_log AS tgt
        USING (
            SELECT
                '{fq_source}'       AS source_table,
                'SystemModstamp'    AS watermark_column,
                CAST('{EPOCH_TS}'   AS TIMESTAMP) AS last_watermark_ts,
                current_timestamp() AS updated_at,
                NULL                AS updated_by_run_id
        ) AS src
        ON tgt.source_table = src.source_table
        WHEN NOT MATCHED THEN INSERT *
    """)

print(f"Seeded {len(SF_BRONZE_TABLES)} watermark rows (skipped any already present).")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Verification

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.{LOGGING_SCHEMA}._watermark_log ORDER BY source_table"))
