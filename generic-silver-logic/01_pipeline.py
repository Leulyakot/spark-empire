# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze → Silver Pipeline
# MAGIC
# MAGIC Fully data-driven — all table names, column mappings, types, and surrogate key
# MAGIC names are read at runtime from `_schema_registry` and `_watermark`.
# MAGIC No schema logic lives in this notebook.
# MAGIC
# MAGIC ### Bronze metadata columns used
# MAGIC | Column | Role |
# MAGIC |---|---|
# MAGIC | `_load_date` | Partition pruning on the Bronze read (avoids full scans) |
# MAGIC | `_ingested_at` | Carried into Silver for lineage |
# MAGIC | `_source_file`, `_row_hash`, `_rescued_data` | Dropped before Silver write |

# COMMAND ----------
# MAGIC %run ./_config

# COMMAND ----------

import uuid
import traceback
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from delta.tables import DeltaTable

# Optional: override which tables to run via job widget
# dbutils.widgets.text("tables", "", "Comma-separated table_keys (blank = all)")

# COMMAND ----------
# MAGIC %md ## Load registry

# COMMAND ----------

def load_registry() -> dict:
    rows = (
        spark.table(log("_schema_registry"))
        .filter(F.col("is_active") == True)
        .orderBy("table_key", "col_ordinal")
        .collect()
    )
    registry = {}
    for r in rows:
        key = r["table_key"]
        if key not in registry:
            registry[key] = {
                "bronze_table"     : r["bronze_table"],
                "silver_table"     : r["silver_table"],
                "pk"               : r["pk"],
                "watermark_col"    : r["watermark_col"],
                "surrogate_key_col": r["surrogate_key_col"],
                "columns"          : [],
            }
        registry[key]["columns"].append((
            r["bronze_col"], r["silver_col"],
            r["data_type"],  r["nullable"], r["col_comment"] or "",
        ))
    print(f"Registry: {len(registry)} tables, "
          f"{sum(len(v['columns']) for v in registry.values())} active columns")
    return registry

TABLE_REGISTRY = load_registry()

# COMMAND ----------
# MAGIC %md ## Watermark helpers

# COMMAND ----------

def _get_watermark(source_fq: str):
    row = spark.sql(f"""
        SELECT last_watermark_ts
        FROM   {log('_watermark')}
        WHERE  source_table = '{source_fq}'
        AND    environment  = '{ENV}'
    """).collect()
    if not row:
        raise ValueError(
            f"No watermark row for [{source_fq}] env=[{ENV}]. "
            "Run 00b_registry_seed first."
        )
    return row[0]["last_watermark_ts"]


def _advance_watermark(source_fq: str, new_ts, run_id: str):
    spark.sql(f"""
        MERGE INTO {log('_watermark')} AS tgt
        USING (
            SELECT
                '{source_fq}'       AS source_table,
                '{ENV}'             AS environment,
                CAST('{new_ts}'     AS TIMESTAMP) AS last_watermark_ts,
                current_timestamp() AS updated_at,
                '{run_id}'          AS updated_by_run_id
        ) AS src
        ON  tgt.source_table = src.source_table
        AND tgt.environment  = src.environment
        WHEN MATCHED THEN UPDATE SET
            tgt.last_watermark_ts = src.last_watermark_ts,
            tgt.updated_at        = src.updated_at,
            tgt.updated_by_run_id = src.updated_by_run_id
    """)

# COMMAND ----------
# MAGIC %md ## Run log helpers

# COMMAND ----------

def _start_run(run_id: str, source_fq: str):
    spark.sql(f"""
        INSERT INTO {log('_run_log')}
            (run_id, object_name, status, started_at, environment)
        VALUES ('{run_id}', '{source_fq}', 'RUNNING', current_timestamp(), '{ENV}')
    """)


def _end_run(run_id: str, status: str, rows_written: int):
    spark.sql(f"""
        MERGE INTO {log('_run_log')} AS tgt
        USING (
            SELECT
                '{run_id}'    AS run_id,
                '{status}'    AS status,
                {rows_written} AS rows_written,
                current_timestamp() AS completed_at
        ) AS src
        ON tgt.run_id = src.run_id
        WHEN MATCHED THEN UPDATE SET
            tgt.status           = src.status,
            tgt.rows_written     = src.rows_written,
            tgt.completed_at     = src.completed_at,
            tgt.duration_seconds = unix_timestamp(src.completed_at)
                                 - unix_timestamp(tgt.started_at)
    """)


def _log_error(run_id: str, source_fq: str, exc: Exception):
    tb    = traceback.format_exc().replace("'", "''")
    msg   = str(exc).replace("'", "''")
    etype = type(exc).__name__
    spark.sql(f"""
        INSERT INTO {log('_error_log')}
            (run_id, object_name, error_type, error_message,
             stack_trace, environment, occurred_at)
        VALUES (
            '{run_id}', '{source_fq}',
            '{etype}', '{msg}', '{tb}',
            '{ENV}', current_timestamp()
        )
    """)

# COMMAND ----------
# MAGIC %md ## Silver DDL builder

# COMMAND ----------

def _ensure_silver_table(cfg: dict):
    """
    CREATE TABLE IF NOT EXISTS for a Silver table.
    Driven entirely by the registry — no column definitions in code.

    Pipeline-managed columns on every Silver table:
      inserted_datetime — set on first INSERT, never updated
      updated_datetime  — refreshed on every MERGE touch
    """
    silver_fq = silver(cfg["silver_table"])
    sk_col    = cfg["surrogate_key_col"]
    seen      = set()

    col_ddl = [
        f"    {sk_col} BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1)"
        f" COMMENT 'Auto-incrementing surrogate key — assigned on INSERT, never updated'",
    ]

    for (_, silver_col, data_type, nullable, comment) in cfg["columns"]:
        if silver_col in seen:
            continue
        seen.add(silver_col)
        null_kw = "" if nullable else " NOT NULL"
        cmt_kw  = f" COMMENT '{comment}'" if comment else ""
        col_ddl.append(f"    {silver_col} {data_type}{null_kw}{cmt_kw}")

    col_ddl += [
        "    inserted_datetime  TIMESTAMP NOT NULL  COMMENT 'When this record first landed in Silver — never updated'",
        "    updated_datetime   TIMESTAMP NOT NULL  COMMENT 'When this record was last updated in Silver'",
    ]

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {silver_fq} (
        {chr(10).join(col_ddl)}
        )
        USING DELTA
        COMMENT 'Silver — {cfg["silver_table"]}. Schema governed by {log("_schema_registry")}.'
        TBLPROPERTIES (
            'delta.enableChangeDataFeed'       = 'true',
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true',
            'pipeline.source_object'           = '{cfg["bronze_table"]}',
            'pipeline.owner'                   = 'data_engineering'
        )
    """)

# COMMAND ----------
# MAGIC %md ## Core ETL

# COMMAND ----------

def process_table(table_key: str):
    cfg       = TABLE_REGISTRY[table_key]
    bronze_fq = bronze(cfg["bronze_table"])
    silver_fq = silver(cfg["silver_table"])
    pk        = cfg["pk"]
    wm_col    = cfg["watermark_col"]
    sk_col    = cfg["surrogate_key_col"]
    run_id    = str(uuid.uuid4())

    # ── Watermark window ─────────────────────────────────────
    wm_from = _get_watermark(bronze_fq)
    wm_to   = spark.sql(f"""
        SELECT timestampadd(MINUTE, -{WATERMARK_LAG_MINUTES}, current_timestamp()) AS t
    """).collect()[0]["t"]

    print(f"\n{'─'*70}")
    print(f"  TABLE   : {bronze_fq}  →  {silver_fq}")
    print(f"  WINDOW  : {wm_from}  →  {wm_to}")

    _start_run(run_id, bronze_fq)

    try:
        # ── Bronze read — partition prune on _load_date first,
        #    then precise filter on watermark column ──────────
        bronze_df = (
            spark.table(bronze_fq)
            .filter(F.col("_load_date") >= F.to_date(F.lit(wm_from)))
            .filter(F.col(wm_col) >  F.lit(wm_from).cast(TimestampType()))
            .filter(F.col(wm_col) <= F.lit(wm_to).cast(TimestampType()))
        )

        rows_read = bronze_df.count()
        print(f"  ROWS IN WINDOW  : {rows_read:,}")

        if SKIP_EMPTY_WINDOWS and rows_read == 0:
            print("  No new rows — skipping.")
            _end_run(run_id, "SKIPPED", 0)
            return

        # ── Deduplication ─────────────────────────────────────
        # Partition by sf_id (Salesforce record ID).
        # Tiebreaker order:
        #   1. SystemModstamp DESC  — system-level last-change stamp
        #   2. LastModifiedDate DESC — user-visible last modification
        # When both are equal the row is a true duplicate and either
        # can be kept safely.
        w = (
            Window
            .partitionBy(pk)                          # sf_id
            .orderBy(
                F.col(wm_col).desc(),                 # SystemModstamp
                F.col("LastModifiedDate").desc(),     # LastModifiedDate
            )
        )
        dedup_df = (
            bronze_df
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        rows_dedup = dedup_df.count()
        print(f"  ROWS AFTER DEDUP: {rows_dedup:,}  ({rows_read - rows_dedup:,} duplicates dropped)")

        # ── Watermark ceiling for this batch ─────────────────
        max_wm_ts = dedup_df.agg(F.max(F.col(wm_col))).collect()[0][0]

        # ── Build staged dataframe from registry columns only ─
        # Bronze metadata columns (_ingested_at, _source_file,
        # _load_date, _row_hash, _rescued_data) are intentionally
        # excluded — they do not belong in Silver.
        select_exprs = []
        seen         = set()

        for (bronze_col, silver_col, data_type, _, _) in cfg["columns"]:
            if silver_col in seen:
                continue
            seen.add(silver_col)
            select_exprs.append(F.col(bronze_col).cast(data_type).alias(silver_col))

        staged_df = dedup_df.select(select_exprs)

        # ── Ensure Silver table exists ────────────────────────
        _ensure_silver_table(cfg)

        # ── MERGE into Silver ─────────────────────────────────
        # INSERT — new sf_id:
        #   inserted_datetime and updated_datetime both set to now.
        #   Surrogate key absent from map → Delta IDENTITY generates it.
        #
        # UPDATE — existing sf_id:
        #   All business columns refreshed.
        #   updated_datetime advances to now.
        #   inserted_datetime is preserved — never overwritten.

        now         = F.current_timestamp()
        update_map  = {c: f"src.{c}" for c in staged_df.columns}
        update_map["updated_datetime"] = "current_timestamp()"

        insert_map  = {c: f"src.{c}" for c in staged_df.columns}
        insert_map["inserted_datetime"] = "current_timestamp()"
        insert_map["updated_datetime"]  = "current_timestamp()"

        (
            DeltaTable.forName(spark, silver_fq).alias("tgt")
            .merge(staged_df.alias("src"), "tgt.sf_id = src.sf_id")
            .whenMatchedUpdate(values=update_map)
            .whenNotMatchedInsert(values=insert_map)
            .execute()
        )

        # ── Merge metrics ─────────────────────────────────────
        metrics      = (
            spark.sql(f"DESCRIBE HISTORY {silver_fq} LIMIT 1")
            .select("operationMetrics")
            .collect()[0]["operationMetrics"]
        )
        rows_written = (
            int(metrics.get("numTargetRowsInserted", 0)) +
            int(metrics.get("numTargetRowsUpdated",  0))
        )
        print(f"  ROWS WRITTEN    : {rows_written:,}")

        # ── Advance watermark (only on success) ───────────────
        _advance_watermark(bronze_fq, max_wm_ts, run_id)
        print(f"  WATERMARK → {max_wm_ts}")

        _end_run(run_id, "SUCCEEDED", rows_written)
        print(f"  STATUS          : SUCCEEDED")

    except Exception as exc:
        _log_error(run_id, bronze_fq, exc)
        _end_run(run_id, "FAILED", 0)
        print(f"  STATUS          : FAILED — {exc}")
        raise

# COMMAND ----------
# MAGIC %md ## Bootstrap Silver schemas

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
for key, cfg in TABLE_REGISTRY.items():
    _ensure_silver_table(cfg)
print("Silver schemas ready.")

# COMMAND ----------
# MAGIC %md ## Run

# COMMAND ----------

# All tables by default; override with job widget:
# TABLES_TO_PROCESS = dbutils.widgets.get("tables").split(",")
TABLES_TO_PROCESS = list(TABLE_REGISTRY.keys())

results = {"SUCCEEDED": [], "FAILED": [], "SKIPPED": []}

for tbl_key in TABLES_TO_PROCESS:
    try:
        process_table(tbl_key)
        results["SUCCEEDED"].append(tbl_key)
    except Exception:
        results["FAILED"].append(tbl_key)

print("\n" + "="*70)
print(f"  ENV       : {ENV.upper()}  |  CATALOG : {CATALOG}")
print(f"  SUCCEEDED : {len(results['SUCCEEDED'])}  {results['SUCCEEDED']}")
print(f"  SKIPPED   : {len(results['SKIPPED'])}   {results['SKIPPED']}")
print(f"  FAILED    : {len(results['FAILED'])}    {results['FAILED']}")
print("="*70)

if results["FAILED"]:
    raise RuntimeError(f"{len(results['FAILED'])} table(s) failed: {results['FAILED']}")
