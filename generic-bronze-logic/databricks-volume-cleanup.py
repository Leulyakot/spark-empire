# =============================================================================
# NOTEBOOK:    autoloader_bronze_cleanup.py
# LAYER:       Bronze
# PURPOSE:     Delete raw JSON files from the Databricks Volume that are
#              older than the retention window and have been confirmed
#              ingested into Bronze Delta tables.
# SCHEDULE:    Daily via Databricks Workflow — Task 2
#              Runs in parallel with silver_layer_job after bronze_ingest
#              succeeds.
# =============================================================================


# =============================================================================
# CONFIGURATION
# All environment-specific values live here.
# Change this block only — nothing below this line should need editing.
# =============================================================================

import uuid
import os

# ── CHANGE ME ─────────────────────────────────────────────────────────────────
ENVIRONMENT         = "prod"                              # dev / uat / prod
CATALOG             = "your_catalog"                      # Unity Catalog name
RAW_SCHEMA          = "raw"                               # Schema inside catalog
VOLUME_BASE         = "/Volumes/your_catalog/raw/bronze"  # Root bronze Volume path
FILE_RETENTION_DAYS = 365                                 # Days to keep raw JSON files
# ── END CHANGE ME ─────────────────────────────────────────────────────────────

INGEST_LOG_TABLE   = f"{CATALOG}.{RAW_SCHEMA}._ingest_log"
CLEANUP_LOG_TABLE  = f"{CATALOG}.{RAW_SCHEMA}._cleanup_log"
ERROR_LOG_TABLE    = f"{CATALOG}.{RAW_SCHEMA}._ingest_errors"

RUN_ID = os.environ.get("WORKFLOW_RUN_ID", str(uuid.uuid4()))

# =============================================================================
# END OF CONFIGURATION — do not edit below this line
# =============================================================================


# =============================================================================
# IMPORTS
# =============================================================================

import sys
import traceback
from datetime import datetime, timezone, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, LongType, DoubleType, BooleanType
)

spark = SparkSession.builder.getOrCreate()

RUN_STARTED_AT = datetime.now(timezone.utc)
CUTOFF_DATE    = RUN_STARTED_AT - timedelta(days=FILE_RETENTION_DAYS)

print("=" * 70)
print(f"  Bronze Cleanup — {ENVIRONMENT.upper()}")
print("=" * 70)
print(f"  Run ID          : {RUN_ID}")
print(f"  Started at      : {RUN_STARTED_AT.isoformat()}")
print(f"  Catalog         : {CATALOG}")
print(f"  Volume base     : {VOLUME_BASE}")
print(f"  Retention       : {FILE_RETENTION_DAYS} days")
print(f"  Cutoff date     : {CUTOFF_DATE.date()} — folders older than this are eligible")
print("=" * 70)


# =============================================================================
# BOOTSTRAP CLEANUP LOG TABLE
# =============================================================================

CLEANUP_LOG_SCHEMA = StructType([
    StructField("run_id",           StringType(),    True),
    StructField("object_name",      StringType(),    True),
    StructField("folder_path",      StringType(),    True),
    StructField("folder_date",      StringType(),    True),
    StructField("files_deleted",    LongType(),      True),
    StructField("deleted_at",       TimestampType(), True),
    StructField("environment",      StringType(),    True),
])

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CLEANUP_LOG_TABLE} (
        run_id        STRING     COMMENT 'Workflow run ID',
        object_name   STRING     COMMENT 'Salesforce object name',
        folder_path   STRING     COMMENT 'Full path of deleted folder',
        folder_date   STRING     COMMENT 'Date of deleted folder yyyy-MM-dd',
        files_deleted LONG       COMMENT 'Number of files removed',
        deleted_at    TIMESTAMP  COMMENT 'When the folder was deleted',
        environment   STRING     COMMENT 'dev / uat / prod'
    )
    USING DELTA
    PARTITIONED BY (environment)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

print(f"Cleanup log table ready: {CLEANUP_LOG_TABLE}")


# =============================================================================
# READ SUCCEEDED OBJECTS FROM INGEST LOG
# Only clean up objects that were confirmed ingested in today's run.
# If an object failed ingest its files are left intact for the retry run.
# =============================================================================

print(f"\nReading ingest log for run: {RUN_ID}")

try:
    succeeded_df = (
        spark.read.table(INGEST_LOG_TABLE)
        .filter(
            (col("run_id")      == lit(RUN_ID))   &
            (col("layer")       == lit("bronze"))  &
            (col("status")      == lit("SUCCESS")) &
            (col("environment") == lit(ENVIRONMENT))
        )
        .select("object_name")
    )

    succeeded_objects = [row["object_name"] for row in succeeded_df.collect()]

except Exception as e:
    print(f"ERROR reading ingest log: {e}")
    print("Cannot determine which objects succeeded. Exiting without deleting anything.")
    sys.exit(1)

if not succeeded_objects:
    print(f"No successful objects found in _ingest_log for run_id={RUN_ID}.")
    print("Nothing to clean up. Exiting.")
    sys.exit(0)

print(f"Objects confirmed ingested ({len(succeeded_objects)}): {', '.join(succeeded_objects)}")


# =============================================================================
# HELPER — LIST DATE FOLDERS UNDER AN OBJECT PATH
# Uses Spark binaryFile to list files, then derives folder dates.
# No dbutils dependency.
# =============================================================================

def list_date_folders(object_name: str) -> list:
    """
    Returns a list of dicts:
        { "path": str, "date": datetime.date, "file_count": int }

    Reads the Volume path using Spark binaryFile to list all JSON files,
    extracts the yyyy/MM/dd segment from each file path, groups by date,
    and returns one entry per unique date folder.
    """
    volume_path = f"{VOLUME_BASE}/{object_name}"

    try:
        from pyspark.sql.functions import regexp_extract, to_date, count
        from pyspark.sql.types import IntegerType

        files_df = (
            spark.read
            .format("binaryFile")
            .option("pathGlobFilter",      "*.json")
            .option("recursiveFileLookup", "true")
            .load(volume_path)
            .select(col("path"))
            .withColumn(
                "date_str",
                regexp_extract(col("path"), r"(\d{4}/\d{2}/\d{2})", 1)
            )
            .filter(col("date_str") != "")
            .withColumn(
                "folder_date",
                to_date(col("date_str"), "yyyy/MM/dd")
            )
            .groupBy("date_str", "folder_date")
            .agg(count("*").alias("file_count"))
        )

        rows = files_df.collect()

        result = []
        for row in rows:
            result.append({
                "path":       f"{volume_path}/{row['date_str']}",
                "date":       row["folder_date"],
                "date_str":   str(row["folder_date"]),
                "file_count": row["file_count"],
            })

        return result

    except Exception as e:
        print(f"    WARNING: Could not list folders for {object_name}: {e}")
        return []


# =============================================================================
# HELPER — DELETE A FOLDER AND ALL FILES INSIDE IT
# Uses Spark to delete files individually since rmdir requires empty dirs.
# No dbutils dependency.
# =============================================================================

def delete_folder(folder_path: str) -> int:
    """
    Deletes all JSON files inside folder_path.
    Returns the count of files deleted.
    """
    try:
        files_df = (
            spark.read
            .format("binaryFile")
            .option("pathGlobFilter",      "*.json")
            .option("recursiveFileLookup", "true")
            .load(folder_path)
            .select("path")
        )

        file_paths = [row["path"] for row in files_df.collect()]

        sc   = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        fs   = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
            sc._jvm.java.net.URI.create(folder_path),
            hadoop_conf
        )

        deleted = 0
        for path in file_paths:
            hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(path)
            if fs.exists(hadoop_path):
                fs.delete(hadoop_path, False)
                deleted += 1

        folder_hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(folder_path)
        if fs.exists(folder_hadoop_path):
            fs.delete(folder_hadoop_path, True)

        return deleted

    except Exception as e:
        raise RuntimeError(f"Failed to delete folder {folder_path}: {e}")


# =============================================================================
# MAIN CLEANUP LOOP
# =============================================================================

print(f"\nStarting cleanup")
print(f"{'─' * 70}")
print(f"  {'Object':<28} {'Folder date':<14} {'Files':>6}   Action")
print(f"{'─' * 70}")

deleted_log  = []
total_files  = 0
total_folders = 0
cleanup_errors = []

for obj in succeeded_objects:

    folders = list_date_folders(obj)

    if not folders:
        print(f"  {obj:<28} No folders found — skipping")
        continue

    for folder in folders:

        folder_date  = folder["date"]
        folder_path  = folder["path"]
        folder_date_str = folder["date_str"]
        file_count   = folder["file_count"]

        if folder_date is None:
            print(f"  {obj:<28} Could not parse date from path — skipping {folder_path}")
            continue

        if folder_date >= CUTOFF_DATE.date():
            print(f"  {obj:<28} {folder_date_str:<14} {file_count:>6}   KEEP (within {FILE_RETENTION_DAYS}-day window)")
            continue

        try:
            deleted_count = delete_folder(folder_path)

            deleted_log.append((
                RUN_ID,
                obj,
                folder_path,
                folder_date_str,
                deleted_count,
                datetime.now(timezone.utc),
                ENVIRONMENT,
            ))

            total_files   += deleted_count
            total_folders += 1

            print(f"  {obj:<28} {folder_date_str:<14} {deleted_count:>6}   DELETED")

        except Exception as e:
            error_msg = str(e)
            cleanup_errors.append({
                "object":      obj,
                "folder_path": folder_path,
                "folder_date": folder_date_str,
                "error":       error_msg,
            })
            print(f"  {obj:<28} {folder_date_str:<14} {'':>6}   ERROR — {error_msg[:40]}")


# =============================================================================
# WRITE CLEANUP AUDIT LOG
# =============================================================================

if deleted_log:
    (spark.createDataFrame(deleted_log, schema=CLEANUP_LOG_SCHEMA)
          .write.format("delta")
          .mode("append")
          .saveAsTable(CLEANUP_LOG_TABLE))

    print(f"\nAudit log written to {CLEANUP_LOG_TABLE}")


# =============================================================================
# WRITE ERRORS TO ERROR LOG IF ANY FOLDERS FAILED TO DELETE
# =============================================================================

ERROR_LOG_SCHEMA = StructType([
    StructField("run_id",        StringType(),    True),
    StructField("layer",         StringType(),    True),
    StructField("object_name",   StringType(),    True),
    StructField("error_type",    StringType(),    True),
    StructField("error_message", StringType(),    True),
    StructField("stack_trace",   StringType(),    True),
    StructField("occurred_at",   TimestampType(), True),
    StructField("table_name",    StringType(),    True),
    StructField("environment",   StringType(),    True),
])

if cleanup_errors:
    error_rows = [
        (
            RUN_ID,
            "bronze_cleanup",
            e["object"],
            "CleanupError",
            e["error"],
            "",
            datetime.now(timezone.utc),
            e["folder_path"],
            ENVIRONMENT,
        )
        for e in cleanup_errors
    ]
    (spark.createDataFrame(error_rows, schema=ERROR_LOG_SCHEMA)
          .write.format("delta")
          .mode("append")
          .saveAsTable(ERROR_LOG_TABLE))

    print(f"Cleanup errors written to {ERROR_LOG_TABLE}")


# =============================================================================
# RUN SUMMARY AND EXIT
# =============================================================================

run_completed_at = datetime.now(timezone.utc)
total_duration   = (run_completed_at - RUN_STARTED_AT).total_seconds()

print(f"\n{'─' * 70}")
print(f"  Run ID          : {RUN_ID}")
print(f"  Completed       : {run_completed_at.isoformat()}")
print(f"  Duration        : {total_duration:.1f}s")
print(f"  Folders deleted : {total_folders}")
print(f"  Files deleted   : {total_files}")
print(f"  Errors          : {len(cleanup_errors)}")

if cleanup_errors:
    print(f"\n  Folders that failed to delete:")
    for e in cleanup_errors:
        print(f"    x {e['object']} / {e['folder_date']} — {e['error'][:60]}")
    print(f"\n  See {ERROR_LOG_TABLE} for full detail.")
    sys.exit(1)
