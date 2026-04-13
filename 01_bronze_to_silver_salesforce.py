# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze → Silver Pipeline — Salesforce
# MAGIC
# MAGIC Schema is driven entirely by `pipeline_control._schema_registry`.
# MAGIC No column definitions live in this notebook.
# MAGIC To add, rename, retype, or disable a column update the registry table and re-run.

# COMMAND ----------
# MAGIC %run ./_config

# COMMAND ----------

import uuid
import traceback
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType
from delta.tables import DeltaTable

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Load Registry from Delta Table

# COMMAND ----------

def load_registry() -> dict:
    """
    Read _schema_registry and return a dict keyed by table_key.

    Each value:
    {
        "bronze_table" : str,
        "silver_table" : str,
        "pk"           : str,
        "watermark_col": str,
        "columns"      : [ (bronze_col, silver_col, data_type_str, nullable, comment), ... ]
                          ordered by col_ordinal ASC
    }
    """
    rows = (
        spark.table(REGISTRY_FQ)
        .filter(F.col("is_active") == True)
        .orderBy("table_key", "col_ordinal")
        .collect()
    )

    registry = {}
    for r in rows:
        key = r["table_key"]
        if key not in registry:
            registry[key] = {
                "bronze_table" : r["bronze_table"],
                "silver_table" : r["silver_table"],
                "pk"           : r["pk"],
                "watermark_col": r["watermark_col"],
                "columns"      : [],
            }
        registry[key]["columns"].append((
            r["bronze_col"],
            r["silver_col"],
            r["data_type"],
            r["nullable"],
            r["col_comment"] or "",
        ))

    print(f"Registry loaded: {len(registry)} tables, "
          f"{sum(len(v['columns']) for v in registry.values())} active columns")
    return registry


TABLE_REGISTRY = load_registry()

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Logging Helpers

# COMMAND ----------

def _get_watermark(source_fq: str):
    row = spark.sql(f"""
        SELECT last_watermark_ts
        FROM   {log('_watermark_log')}
        WHERE  source_table = '{source_fq}'
    """).collect()
    if not row:
        raise ValueError(f"No watermark seed row for {source_fq}. Run 00_setup notebook first.")
    return row[0]["last_watermark_ts"]


def _advance_watermark(source_fq: str, new_ts, run_id: str):
    spark.sql(f"""
        MERGE INTO {log('_watermark_log')} AS tgt
        USING (
            SELECT
                '{source_fq}'       AS source_table,
                CAST('{new_ts}'     AS TIMESTAMP) AS last_watermark_ts,
                current_timestamp() AS updated_at,
                '{run_id}'          AS updated_by_run_id
        ) AS src
        ON tgt.source_table = src.source_table
        WHEN MATCHED THEN UPDATE SET
            tgt.last_watermark_ts  = src.last_watermark_ts,
            tgt.updated_at         = src.updated_at,
            tgt.updated_by_run_id  = src.updated_by_run_id
    """)


def _start_run(run_id, source_fq, target_fq, wm_from, wm_to):
    spark.sql(f"""
        INSERT INTO {log('_run_log')}
            (run_id, pipeline_name, source_table, target_table,
             status, watermark_from, watermark_to, start_time)
        VALUES (
            '{run_id}', '{PIPELINE_NAME}', '{source_fq}', '{target_fq}',
            'RUNNING',
            CAST('{wm_from}' AS TIMESTAMP),
            CAST('{wm_to}'   AS TIMESTAMP),
            current_timestamp()
        )
    """)


def _end_run(run_id, status, rows_read, rows_dedup, rows_merged):
    spark.sql(f"""
        MERGE INTO {log('_run_log')} AS tgt
        USING (
            SELECT
                '{run_id}'    AS run_id,
                '{status}'    AS status,
                {rows_read}   AS rows_read,
                {rows_dedup}  AS rows_after_dedup,
                {rows_merged} AS rows_merged,
                current_timestamp() AS end_time
        ) AS src
        ON tgt.run_id = src.run_id
        WHEN MATCHED THEN UPDATE SET
            tgt.status           = src.status,
            tgt.rows_read        = src.rows_read,
            tgt.rows_after_dedup = src.rows_after_dedup,
            tgt.rows_merged      = src.rows_merged,
            tgt.end_time         = src.end_time,
            tgt.duration_seconds = unix_timestamp(src.end_time) - unix_timestamp(tgt.start_time)
    """)


def _log_error(run_id, source_fq, target_fq, wm_from, wm_to, exc):
    err_id = str(uuid.uuid4())
    tb     = traceback.format_exc().replace("'", "''")
    msg    = str(exc).replace("'", "''")
    etype  = type(exc).__name__
    spark.sql(f"""
        INSERT INTO {log('_error_log')}
            (error_id, run_id, pipeline_name, source_table, target_table,
             error_time, error_type, error_message, stack_trace,
             watermark_from, watermark_to)
        VALUES (
            '{err_id}', '{run_id}', '{PIPELINE_NAME}',
            '{source_fq}', '{target_fq}',
            current_timestamp(),
            '{etype}', '{msg}', '{tb}',
            CAST('{wm_from}' AS TIMESTAMP),
            CAST('{wm_to}'   AS TIMESTAMP)
        )
    """)


# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Silver DDL — driven by registry

# COMMAND ----------

def create_silver_table(cfg: dict):
    """
    Idempotent CREATE TABLE IF NOT EXISTS.
    Column names, types, and nullability all come from _schema_registry rows.
    """
    silver_fq = silver(cfg["silver_table"])
    seen, col_ddl = set(), []

    for (_, silver_col, data_type, nullable, comment) in cfg["columns"]:
        if silver_col in seen:
            continue
        seen.add(silver_col)
        null_kw = "" if nullable else " NOT NULL"
        cmt_kw  = f" COMMENT '{comment}'" if comment else ""
        col_ddl.append(f"    {silver_col} {data_type}{null_kw}{cmt_kw}")

    col_ddl += [
        "    _bronze_ingest_ts  TIMESTAMP COMMENT 'Max SystemModstamp of Bronze rows in this batch'",
        "    _silver_updated_at TIMESTAMP COMMENT 'Wall-clock time this Silver row was last touched'",
        "    _run_id            STRING    COMMENT 'run_id from _run_log for lineage'",
    ]

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {silver_fq} (
        {chr(10).join(col_ddl)}
        )
        USING DELTA
        COMMENT 'Silver — Salesforce {cfg["silver_table"]}. Schema governed by {REGISTRY_FQ}.'
        TBLPROPERTIES (
            'delta.enableChangeDataFeed'        = 'true',
            'delta.autoOptimize.optimizeWrite'  = 'true',
            'delta.autoOptimize.autoCompact'    = 'true',
            'pipeline.source_object'            = '{cfg["bronze_table"]}',
            'pipeline.owner'                    = 'data_engineering',
            'pipeline.registry_table'           = '{REGISTRY_FQ}'
        )
    """)
    print(f"  DDL OK → {silver_fq}")


# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Core ETL

# COMMAND ----------

def process_table(table_key: str):
    cfg       = TABLE_REGISTRY[table_key]
    bronze_fq = bronze(cfg["bronze_table"])
    silver_fq = silver(cfg["silver_table"])
    pk        = cfg["pk"]
    wm_col    = cfg["watermark_col"]
    run_id    = str(uuid.uuid4())

    wm_from = _get_watermark(bronze_fq)
    wm_to   = spark.sql(f"""
        SELECT timestampadd(MINUTE, -{WATERMARK_LAG_MINUTES}, current_timestamp()) AS wm_to
    """).collect()[0]["wm_to"]

    print(f"\n{'─'*70}")
    print(f"  TABLE  : {bronze_fq}  →  {silver_fq}")
    print(f"  WINDOW : {wm_from}  →  {wm_to}")

    _start_run(run_id, bronze_fq, silver_fq, wm_from, wm_to)

    try:
        # ── Incremental read ──────────────────────────────────
        bronze_df = (
            spark.table(bronze_fq)
            .filter(
                (F.col(wm_col) >  F.lit(wm_from).cast(TimestampType())) &
                (F.col(wm_col) <= F.lit(wm_to).cast(TimestampType()))
            )
        )
        rows_read = bronze_df.count()
        print(f"  ROWS READ (window)     : {rows_read:,}")

        if SKIP_EMPTY_WINDOWS and rows_read == 0:
            print("  No new rows — skipping.")
            _end_run(run_id, "SKIPPED", 0, 0, 0)
            return

        # ── Deduplicate — latest record per Id ───────────────
        w = Window.partitionBy(pk).orderBy(F.col(wm_col).desc())
        dedup_df = (
            bronze_df
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        rows_dedup = dedup_df.count()
        print(f"  ROWS AFTER DEDUP       : {rows_dedup:,}")

        # ── Cast + rename columns per registry ───────────────
        max_wm_ts    = dedup_df.agg(F.max(F.col(wm_col))).collect()[0][0]
        select_exprs = []
        seen         = set()

        for (bronze_col, silver_col, data_type, _, _) in cfg["columns"]:
            if silver_col in seen:
                continue
            seen.add(silver_col)
            select_exprs.append(F.col(bronze_col).cast(data_type).alias(silver_col))

        select_exprs += [
            F.lit(max_wm_ts).cast(TimestampType()).alias("_bronze_ingest_ts"),
            F.current_timestamp().alias("_silver_updated_at"),
            F.lit(run_id).alias("_run_id"),
        ]
        staged_df = dedup_df.select(select_exprs)

        # ── Bootstrap Silver table if needed ─────────────────
        if not spark.catalog.tableExists(silver_fq):
            create_silver_table(cfg)

        # ── MERGE into Silver ─────────────────────────────────
        (
            DeltaTable.forName(spark, silver_fq).alias("tgt")
            .merge(staged_df.alias("src"), "tgt.sf_id = src.sf_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        merge_metrics = (
            spark.sql(f"DESCRIBE HISTORY {silver_fq} LIMIT 1")
            .select("operationMetrics")
            .collect()[0]["operationMetrics"]
        )
        rows_merged = (
            int(merge_metrics.get("numTargetRowsInserted", 0)) +
            int(merge_metrics.get("numTargetRowsUpdated",  0))
        )
        print(f"  ROWS MERGED (ins+upd)  : {rows_merged:,}")

        # ── Advance watermark (only on success) ───────────────
        _advance_watermark(bronze_fq, max_wm_ts, run_id)
        print(f"  WATERMARK ADVANCED TO  : {max_wm_ts}")

        _end_run(run_id, "SUCCEEDED", rows_read, rows_dedup, rows_merged)
        print(f"  STATUS                 : SUCCEEDED")

    except Exception as exc:
        _log_error(run_id, bronze_fq, silver_fq, wm_from, wm_to, exc)
        _end_run(run_id, "FAILED", 0, 0, 0)
        print(f"  STATUS                 : FAILED — {exc}")
        raise


# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Bootstrap Silver DDL

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
for key, cfg in TABLE_REGISTRY.items():
    create_silver_table(cfg)

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Run

# COMMAND ----------

TABLES_TO_PROCESS = list(TABLE_REGISTRY.keys())
# Override via job widget: dbutils.widgets.get("tables").split(",")

results = {"SUCCEEDED": [], "FAILED": [], "SKIPPED": []}

for tbl_key in TABLES_TO_PROCESS:
    try:
        process_table(tbl_key)
        results["SUCCEEDED"].append(tbl_key)
    except Exception:
        results["FAILED"].append(tbl_key)

print("\n" + "="*70)
print(f"  ENV       : {ENV.upper()}")
print(f"  CATALOG   : {CATALOG}")
print(f"  SUCCEEDED : {len(results['SUCCEEDED'])}  {results['SUCCEEDED']}")
print(f"  SKIPPED   : {len(results['SKIPPED'])}   {results['SKIPPED']}")
print(f"  FAILED    : {len(results['FAILED'])}    {results['FAILED']}")
print("="*70)

if results["FAILED"]:
    raise RuntimeError(f"{len(results['FAILED'])} table(s) failed: {results['FAILED']}")
