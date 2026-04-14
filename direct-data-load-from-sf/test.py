# =============================================================================
# NOTEBOOK:    autoloader_bronze_ingest.py
# LAYER:       Bronze
# PURPOSE:     Ingest Salesforce CSV files from Databricks Volume into
#              Bronze Delta tables using Auto Loader.
#              Files are named raw_{correlationId}.csv by MuleSoft.
#              Files are left in the Volume after ingest.
# SCHEDULE:    Daily via Databricks Workflow - Task 1
# =============================================================================


# =============================================================================
# CONFIGURATION
# All environment-specific values live here.
# Change this block only - nothing below this line should need editing.
# =============================================================================

import uuid
import os

# -- CHANGE ME -----------------------------------------------------------------
ENVIRONMENT     = "prod"                              # dev / uat / prod
CATALOG         = "your_catalog"                      # Unity Catalog name
RAW_SCHEMA      = "raw"                               # Schema inside catalog
VOLUME_BASE     = "/Volumes/your_catalog/raw/bronze"  # Where MuleSoft writes files
SCHEMA_BASE     = "/Volumes/your_catalog/raw/_schema"      # Auto Loader schema store
CHECKPOINT_BASE = "/Volumes/your_catalog/raw/_checkpoint"  # Auto Loader checkpoint
FILE_FORMAT     = "csv"                               # csv or json
FILE_PREFIX     = "raw_"                              # MuleSoft file naming prefix
# -- END CHANGE ME -------------------------------------------------------------

FILE_GLOB        = f"{FILE_PREFIX}*.{FILE_FORMAT}"    # resolves to raw_*.csv
INGEST_LOG_TABLE = f"{CATALOG}.{RAW_SCHEMA}._ingest_log"
ERROR_LOG_TABLE  = f"{CATALOG}.{RAW_SCHEMA}._ingest_errors"

RUN_ID = os.environ.get("WORKFLOW_RUN_ID", str(uuid.uuid4()))

# =============================================================================
# END OF CONFIGURATION - do not edit below this line
# =============================================================================


# =============================================================================
# OBJECTS TO INGEST
#
# !! CHANGE ME !!
#
# Replace the example objects below with your actual Salesforce objects.
# Each entry is the lowercase name of the object -- this becomes:
#   - the subfolder name in the Volume  e.g. /bronze/account/
#   - the Bronze Delta table name       e.g. catalog.raw.bronze_account
#   - the key in BRONZE_SCHEMAS below
#
# Make sure every name here has a matching entry in BRONZE_SCHEMAS below.
# =============================================================================

OBJECTS = [
    "object_one",    # CHANGE ME
    "object_two",    # CHANGE ME
    "object_three",  # CHANGE ME - add or remove entries to match your objects
]


# =============================================================================
# BRONZE TABLE SCHEMAS
#
# !! CHANGE ME !!
#
# Define one StructType per object in the OBJECTS list above.
# The key must match the object name exactly (lowercase).
#
# Column type rules:
#   Salesforce IDs (18-char)  -> StringType()   always - never cast IDs
#   Free text, picklists      -> StringType()
#   Phone, email              -> StringType()   keep as string
#   Currency, percentages     -> DoubleType()
#   Whole counts              -> IntegerType()
#   True / False fields       -> BooleanType()
#   Date only                 -> DateType()
#   Date + time               -> TimestampType()
#
# Every schema must include these two columns from MuleSoft:
#   sf_id          StringType()     Salesforce record Id
#   _extracted_at  TimestampType()  Set by MuleSoft DataWeave
#
# The five pipeline metadata columns (_ingested_at, _source_file,
# _load_date, _row_hash) are added automatically - do not include them.
# =============================================================================

from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, LongType, DoubleType,
    DateType, BooleanType, IntegerType
)

BRONZE_SCHEMAS = {

    # !! CHANGE ME !! - replace key and fields with your actual object
    "object_one": StructType([
        StructField("sf_id",          StringType(),    True),  # always required
        StructField("_extracted_at",  TimestampType(), True),  # always required
        StructField("FieldOne",       StringType(),    True),  # CHANGE ME
        StructField("FieldTwo",       DoubleType(),    True),  # CHANGE ME
        StructField("FieldThree",     BooleanType(),   True),  # CHANGE ME
        StructField("FieldDate",      DateType(),      True),  # CHANGE ME
        StructField("FieldTimestamp", TimestampType(), True),  # CHANGE ME
        StructField("RelatedId",      StringType(),    True),  # SF IDs always STRING
    ]),

    # !! CHANGE ME !!
    "object_two": StructType([
        StructField("sf_id",         StringType(),    True),  # always required
        StructField("_extracted_at", TimestampType(), True),  # always required
        StructField("FieldOne",      StringType(),    True),  # CHANGE ME
        StructField("FieldTwo",      DoubleType(),    True),  # CHANGE ME
    ]),

    # !! CHANGE ME !!
    "object_three": StructType([
        StructField("sf_id",         StringType(),    True),  # always required
        StructField("_extracted_at", TimestampType(), True),  # always required
        StructField("FieldOne",      StringType(),    True),  # CHANGE ME
        StructField("FieldTwo",      StringType(),    True),  # CHANGE ME
    ]),

    # !! CHANGE ME !! - copy a block above for each additional object
    # and add the object name to the OBJECTS list
}


# =============================================================================
# IMPORTS
# =============================================================================

import sys
import traceback
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    current_timestamp, col, lit, to_date,
    md5, struct, to_json, regexp_extract
)

spark = SparkSession.builder.getOrCreate()

RUN_STARTED_AT = datetime.now(timezone.utc)

print("=" * 70)
print(f"  Bronze Ingest - {ENVIRONMENT.upper()}")
print("=" * 70)
print(f"  Run ID          : {RUN_ID}")
print(f"  Started at      : {RUN_STARTED_AT.isoformat()}")
print(f"  Catalog         : {CATALOG}")
print(f"  Schema          : {RAW_SCHEMA}")
print(f"  Volume base     : {VOLUME_BASE}")
print(f"  File pattern    : {FILE_GLOB}")
print(f"  Objects ({len(OBJECTS)})     : {', '.join(OBJECTS)}")
print("=" * 70)


# =============================================================================
# VALIDATION
# Fail fast if any object in OBJECTS has no schema in BRONZE_SCHEMAS.
# =============================================================================

missing_schemas = [o for o in OBJECTS if o not in BRONZE_SCHEMAS]
if missing_schemas:
    raise ValueError(
        f"Objects in OBJECTS list have no schema defined in BRONZE_SCHEMAS: "
        f"{missing_schemas}. Add a StructType entry for each one."
    )

print(f"Config validated - all {len(OBJECTS)} objects have schemas defined.\n")


# =============================================================================
# LOG TABLE SCHEMAS
# =============================================================================

INGEST_LOG_SCHEMA = StructType([
    StructField("run_id",            StringType(),    True),
    StructField("layer",             StringType(),    True),
    StructField("object_name",       StringType(),    True),
    StructField("status",            StringType(),    True),
    StructField("rows_written",      LongType(),      True),
    StructField("source_file_count", LongType(),      True),
    StructField("started_at",        TimestampType(), True),
    StructField("completed_at",      TimestampType(), True),
    StructField("duration_seconds",  DoubleType(),    True),
    StructField("table_name",        StringType(),    True),
    StructField("volume_path",       StringType(),    True),
    StructField("error_message",     StringType(),    True),
    StructField("environment",       StringType(),    True),
])

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


# =============================================================================
# BOOTSTRAP LOG TABLES
# =============================================================================

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {INGEST_LOG_TABLE} (
        run_id            STRING     COMMENT 'Workflow run ID',
        layer             STRING     COMMENT 'bronze / silver / gold',
        object_name       STRING     COMMENT 'Salesforce object name',
        status            STRING     COMMENT 'SUCCESS or FAILURE',
        rows_written      LONG       COMMENT 'Rows written this run',
        source_file_count LONG       COMMENT 'Files present in Volume path',
        started_at        TIMESTAMP  COMMENT 'Object stream start time',
        completed_at      TIMESTAMP  COMMENT 'Object stream end time',
        duration_seconds  DOUBLE     COMMENT 'Wall-clock seconds',
        table_name        STRING     COMMENT 'Target Delta table',
        volume_path       STRING     COMMENT 'Source Volume path',
        error_message     STRING     COMMENT 'Short error if FAILURE',
        environment       STRING     COMMENT 'dev / uat / prod'
    )
    USING DELTA
    PARTITIONED BY (environment)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {ERROR_LOG_TABLE} (
        run_id        STRING     COMMENT 'Workflow run ID',
        layer         STRING     COMMENT 'bronze / silver / gold',
        object_name   STRING     COMMENT 'Salesforce object name',
        error_type    STRING     COMMENT 'Python exception class name',
        error_message STRING     COMMENT 'Exception message',
        stack_trace   STRING     COMMENT 'Full Python stack trace',
        occurred_at   TIMESTAMP  COMMENT 'When error was caught',
        table_name    STRING     COMMENT 'Target Delta table',
        environment   STRING     COMMENT 'dev / uat / prod'
    )
    USING DELTA
    PARTITIONED BY (environment)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

print(f"Log tables ready:\n  {INGEST_LOG_TABLE}\n  {ERROR_LOG_TABLE}\n")


# =============================================================================
# LOGGING HELPERS
# =============================================================================

def log_success(
    object_name:       str,
    rows_written:      int,
    source_file_count: int,
    started_at:        datetime,
    table_name:        str,
    volume_path:       str,
) -> None:
    completed_at     = datetime.now(timezone.utc)
    duration_seconds = (completed_at - started_at).total_seconds()

    row = [(
        RUN_ID, "bronze", object_name, "SUCCESS",
        int(rows_written), int(source_file_count),
        started_at, completed_at, round(duration_seconds, 2),
        table_name, volume_path, None,
        ENVIRONMENT,
    )]

    (spark.createDataFrame(row, schema=INGEST_LOG_SCHEMA)
          .write.format("delta")
          .mode("append")
          .saveAsTable(INGEST_LOG_TABLE))

    print(f"  [OK]   {object_name:<28} {rows_written:>8,} rows   {duration_seconds:>6.1f}s")


def log_failure(
    object_name: str,
    error:       Exception,
    started_at:  datetime,
    table_name:  str,
    volume_path: str,
) -> None:
    occurred_at      = datetime.now(timezone.utc)
    duration_seconds = (occurred_at - started_at).total_seconds()
    error_message    = str(error)
    stack            = traceback.format_exc()
    error_type       = type(error).__name__

    ingest_row = [(
        RUN_ID, "bronze", object_name, "FAILURE",
        0, 0,
        started_at, occurred_at, round(duration_seconds, 2),
        table_name, volume_path, error_message,
        ENVIRONMENT,
    )]
    (spark.createDataFrame(ingest_row, schema=INGEST_LOG_SCHEMA)
          .write.format("delta")
          .mode("append")
          .saveAsTable(INGEST_LOG_TABLE))

    error_row = [(
        RUN_ID, "bronze", object_name,
        error_type, error_message, stack,
        occurred_at, table_name,
        ENVIRONMENT,
    )]
    (spark.createDataFrame(error_row, schema=ERROR_LOG_SCHEMA)
          .write.format("delta")
          .mode("append")
          .saveAsTable(ERROR_LOG_TABLE))

    print(f"  [FAIL] {object_name:<28} {error_type}: {error_message[:55]}")


# =============================================================================
# BRONZE TABLE BOOTSTRAP
# Creates each Bronze Delta table if it does not already exist.
# appendOnly = true is a hard guardrail - no UPDATE or DELETE ever
# reaches Bronze. Source columns added on first ingest via mergeSchema.
# =============================================================================

def ensure_bronze_table(object_name: str, table_name: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            sf_id          STRING     COMMENT 'Salesforce record Id',
            _extracted_at  TIMESTAMP  COMMENT 'Extraction time set by MuleSoft',
            _ingested_at   TIMESTAMP  COMMENT 'Auto Loader write timestamp',
            _source_file   STRING     COMMENT 'Full Volume path including filename',
            _load_date     DATE       COMMENT 'Partition key from file path',
            _row_hash      STRING     COMMENT 'MD5 hash for Silver change detection'
        )
        USING DELTA
        PARTITIONED BY (_load_date)
        COMMENT 'Bronze raw table for Salesforce {object_name}. Append-only.'
        TBLPROPERTIES (
            'delta.appendOnly'                   = 'true',
            'delta.autoOptimize.optimizeWrite'   = 'true',
            'delta.autoOptimize.autoCompact'     = 'true',
            'delta.logRetentionDuration'         = 'interval 365 days',
            'delta.deletedFileRetentionDuration' = 'interval 365 days',
            'pipeline.object'                    = '{object_name}',
            'pipeline.environment'               = '{ENVIRONMENT}',
            'pipeline.layer'                     = 'bronze'
        )
    """)


# =============================================================================
# METRICS HELPERS
# =============================================================================

def get_rows_written(table_name: str, started_at: datetime) -> int:
    """Count rows appended to this Bronze table during the current run."""
    try:
        return (
            spark.read.table(table_name)
            .filter(col("_ingested_at") >= lit(started_at.isoformat()))
            .count()
        )
    except Exception:
        return 0


def get_file_count(volume_path: str) -> int:
    """Count matching raw_*.csv files in the object Volume folder."""
    try:
        return (
            spark.read
            .format("binaryFile")
            .option("pathGlobFilter",      FILE_GLOB)
            .option("recursiveFileLookup", "true")
            .load(volume_path)
            .count()
        )
    except Exception:
        return 0


# =============================================================================
# AUTO LOADER STREAM BUILDER
# =============================================================================

def build_bronze_stream(object_name: str):
    """
    Build and start an Auto Loader streaming query for one Salesforce object.

    File pattern:   raw_{correlationId}.csv  matched via FILE_GLOB
    Schema:         Explicit StructType from BRONZE_SCHEMAS
    Evolution mode: rescue - unknown fields do not fail the stream
    Trigger:        availableNow - process pending files then stop
    Checkpoint:     persists between runs, prevents re-ingestion
    """
    volume_path = f"{VOLUME_BASE}/{object_name}"
    schema_loc  = f"{SCHEMA_BASE}/{object_name}"
    chk_loc     = f"{CHECKPOINT_BASE}/{object_name}"
    table_name  = f"{CATALOG}.{RAW_SCHEMA}.bronze_{object_name}"

    ensure_bronze_table(object_name, table_name)

    df: DataFrame = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",              FILE_FORMAT)
        .option("cloudFiles.schemaLocation",      schema_loc)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.pathGlobFilter",      FILE_GLOB)
        .option("ignoreCorruptFiles",             "true")
        .option("ignoreMissingFiles",             "true")
        .option("header",                         "true")
        .option("escape",                         '"')
        .option("quote",                          '"')
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        .option("dateFormat",      "yyyy-MM-dd")
        .schema(BRONZE_SCHEMAS[object_name])
        .load(volume_path)

        .withColumn("_ingested_at",
            current_timestamp())

        .withColumn("_source_file",
            col("_metadata.file_path"))
            # e.g. /Volumes/your_catalog/raw/bronze/account/2026/03/29/raw_uuid.csv
            # The MuleSoft correlation ID is embedded in the filename

        .withColumn("_load_date",
            to_date(
                regexp_extract(
                    col("_metadata.file_path"),
                    r"(\d{4}/\d{2}/\d{2})", 1
                ),
                "yyyy/MM/dd"
            )
        )

        .withColumn("_row_hash",
            md5(to_json(struct(
                col("sf_id"),
                col("_extracted_at"),
                col("_source_file"),
            )))
        )
    )

    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", chk_loc)
        .option("mergeSchema",        "true")
        .trigger(availableNow=True)
        .queryName(f"bronze_{object_name}_{RUN_ID[:8]}")
        .toTable(table_name)
    )

    return query, table_name, volume_path


# =============================================================================
# MAIN INGEST LOOP
# =============================================================================

print(f"Starting ingest - {len(OBJECTS)} objects")
print(f"{'─' * 70}")
print(f"  {'Object':<28} {'Rows':>8}   {'Duration':>8}   Status")
print(f"{'─' * 70}")

succeeded = []
failed    = []

for obj in OBJECTS:
    obj_started_at = datetime.now(timezone.utc)
    table_name     = f"{CATALOG}.{RAW_SCHEMA}.bronze_{obj}"
    volume_path    = f"{VOLUME_BASE}/{obj}"

    try:
        query, table_name, volume_path = build_bronze_stream(obj)

        query.awaitTermination()
        # Blocks here until all pending raw_*.csv files for this object
        # are committed to the Bronze Delta table and checkpoint is updated.

        if query.exception():
            raise RuntimeError(str(query.exception()))

        rows  = get_rows_written(table_name, obj_started_at)
        files = get_file_count(volume_path)

        log_success(
            object_name       = obj,
            rows_written      = rows,
            source_file_count = files,
            started_at        = obj_started_at,
            table_name        = table_name,
            volume_path       = volume_path,
        )
        succeeded.append(obj)

    except Exception as e:
        log_failure(
            object_name = obj,
            error       = e,
            started_at  = obj_started_at,
            table_name  = table_name,
            volume_path = volume_path,
        )
        failed.append(obj)
        # One failure does not stop the remaining objects


# =============================================================================
# RUN SUMMARY AND EXIT
# =============================================================================

run_completed_at = datetime.now(timezone.utc)
total_duration   = (run_completed_at - RUN_STARTED_AT).total_seconds()

print(f"{'─' * 70}")
print(f"\n  Run ID      : {RUN_ID}")
print(f"  Completed   : {run_completed_at.isoformat()}")
print(f"  Duration    : {total_duration:.1f}s  ({total_duration / 60:.1f} min)")
print(f"  Succeeded   : {len(succeeded)} / {len(OBJECTS)}")
print(f"  Failed      : {len(failed)} / {len(OBJECTS)}")

if failed:
    print(f"\n  Failed objects:")
    for o in failed:
        print(f"    x {o}  - see {ERROR_LOG_TABLE}")
    sys.exit(1)
