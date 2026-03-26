# =============================================================================
# sf_load.py
# Schedule : Hourly
# Purpose  : Load Salesforce data into Delta Lake using schema from the monthly
#            catalog. First run = full load. Subsequent runs = incremental load
#            using a watermark table.
#
# Improvements over the current version:
# - serial execution instead of thread-based parallel object loading
# - deterministic incremental ordering
# - safer watermark updates based on loaded source data, not notebook start time
# - proper SOQL datetime quoting
# - Delta merge without invalid builder options
# - object-level run logging
# - Databricks-friendly widgets and config
# =============================================================================

from datetime import datetime, timezone
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import types as T

# -----------------------------------------------------------------------------
# ENVIRONMENT CONFIG
# Keep all environment-specific values here
# -----------------------------------------------------------------------------
SF_SECRET_SCOPE = "salesforce"
SF_USERNAME_KEY = "username"
SF_PASSWORD_KEY = "password"
SF_SECURITY_TOKEN_KEY = "security_token"

TARGET_DB = "salesforce_bronze"

CATALOG_TABLE = f"{TARGET_DB}._catalog"
WATERMARK_TABLE = f"{TARGET_DB}._watermark"
JOB_LOG_TABLE = f"{TARGET_DB}._job_log"

CHUNK_SIZE = "100000"

DEFAULT_OBJECT_FILTER = ""

# -----------------------------------------------------------------------------
# NOTEBOOK PARAMETERS
# -----------------------------------------------------------------------------
dbutils.widgets.text("object_name", DEFAULT_OBJECT_FILTER)
OBJECT_FILTER = dbutils.widgets.get("object_name").strip()

# -----------------------------------------------------------------------------
# SALESFORCE AUTH
# Username + password + security token flow
# -----------------------------------------------------------------------------
SF_USERNAME = dbutils.secrets.get(SF_SECRET_SCOPE, SF_USERNAME_KEY)
SF_PASSWORD = dbutils.secrets.get(SF_SECRET_SCOPE, SF_PASSWORD_KEY)
SF_SECURITY_TOKEN = dbutils.secrets.get(SF_SECRET_SCOPE, SF_SECURITY_TOKEN_KEY)

# -----------------------------------------------------------------------------
# SPARK / DELTA SETTINGS
# -----------------------------------------------------------------------------
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
def now_utc():
    """
    Return current UTC time as a Python datetime.
    """
    return datetime.now(timezone.utc)


def table_exists(table_name):
    """
    Safe table-exists helper.
    """
    return spark.catalog.tableExists(table_name)


def ensure_prerequisites():
    """
    Fail early if required control tables are missing.
    """
    if not table_exists(CATALOG_TABLE):
        raise RuntimeError(f"Catalog not found: {CATALOG_TABLE}. Run sf_catalog.py first.")

    if not table_exists(WATERMARK_TABLE):
        raise RuntimeError(f"Watermark table not found: {WATERMARK_TABLE}.")

    if not table_exists(JOB_LOG_TABLE):
        raise RuntimeError(f"Job log table not found: {JOB_LOG_TABLE}.")


def get_objects_from_catalog():
    """
    Return all cataloged objects with at least one queryable field.
    """
    query = f"""
        SELECT DISTINCT sf_object
        FROM {CATALOG_TABLE}
        WHERE is_queryable = true
        ORDER BY sf_object
    """
    return [r["sf_object"] for r in spark.sql(query).collect()]


def get_fields_from_catalog(object_name):
    """
    Return queryable field list for one object from the catalog.
    """
    query = f"""
        SELECT field_name
        FROM {CATALOG_TABLE}
        WHERE sf_object = '{object_name.replace("'", "''")}'
          AND is_queryable = true
        ORDER BY field_name
    """
    return [r["field_name"] for r in spark.sql(query).collect()]


def get_watermark(object_name):
    """
    Read the most recent watermark row for an object.

    Expected watermark table columns:
    - sf_object
    - last_loaded_at
    - last_loaded_id

    Returns:
    - (last_loaded_at, last_loaded_id)
    - (None, None) if the object has not been loaded before
    """
    query = f"""
        SELECT last_loaded_at, last_loaded_id
        FROM {WATERMARK_TABLE}
        WHERE sf_object = '{object_name.replace("'", "''")}'
    """
    rows = spark.sql(query).collect()

    if not rows:
        return None, None

    return rows[0]["last_loaded_at"], rows[0]["last_loaded_id"]


def set_watermark(object_name, loaded_at, loaded_id):
    """
    Upsert the watermark using the max source watermark actually loaded.

    This is safer than writing notebook start time.
    """
    rows = [(object_name, loaded_at, loaded_id)]
    schema = T.StructType([
        T.StructField("sf_object", T.StringType(), False),
        T.StructField("last_loaded_at", T.TimestampType(), True),
        T.StructField("last_loaded_id", T.StringType(), True),
    ])
    source_df = spark.createDataFrame(rows, schema=schema)

    (
        DeltaTable.forName(spark, WATERMARK_TABLE)
        .alias("t")
        .merge(source_df.alias("s"), "t.sf_object = s.sf_object")
        .whenMatchedUpdate(set={
            "last_loaded_at": "s.last_loaded_at",
            "last_loaded_id": "s.last_loaded_id"
        })
        .whenNotMatchedInsert(values={
            "sf_object": "s.sf_object",
            "last_loaded_at": "s.last_loaded_at",
            "last_loaded_id": "s.last_loaded_id"
        })
        .execute()
    )


def log_run(object_name, status, row_count, load_type, started_at, ended_at, message=None):
    """
    Append one row into the job log table.

    Expected job log columns:
    - sf_object
    - status
    - row_count
    - load_type
    - started_at
    - ended_at
    - message
    """
    rows = [(
        object_name,
        status,
        int(row_count),
        load_type,
        started_at,
        ended_at,
        message
    )]

    schema = T.StructType([
        T.StructField("sf_object", T.StringType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("row_count", T.LongType(), False),
        T.StructField("load_type", T.StringType(), True),
        T.StructField("started_at", T.TimestampType(), False),
        T.StructField("ended_at", T.TimestampType(), False),
        T.StructField("message", T.StringType(), True),
    ])

    spark.createDataFrame(rows, schema=schema).write.mode("append").saveAsTable(JOB_LOG_TABLE)


def format_soql_timestamp(ts):
    """
    Format a Python/Spark timestamp into SOQL UTC format.
    """
    if ts is None:
        return None

    if hasattr(ts, "tzinfo") and ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)

    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_soql(object_name, fields, watermark_ts=None, watermark_id=None):
    """
    Build SOQL for full or incremental loads.

    Full load:
      SELECT fields FROM Object

    Incremental load:
      WHERE LastModifiedDate > 'ts'
         OR (LastModifiedDate = 'ts' AND Id > 'last_id')
      ORDER BY LastModifiedDate ASC, Id ASC

    Notes:
    - If watermark_ts is present but watermark_id is null, we use >= to avoid
      accidental skipping on boundary ties.
    - The downstream merge on Id makes duplicate rereads tolerable.
    """
    field_list = ", ".join(fields)
    soql = f"SELECT {field_list} FROM {object_name}"

    has_lmd = "LastModifiedDate" in fields
    has_id = "Id" in fields

    if watermark_ts and has_lmd:
        ts = format_soql_timestamp(watermark_ts)

        if watermark_id and has_id:
            escaped_id = watermark_id.replace("'", "\\'")
            soql += (
                f" WHERE (LastModifiedDate > '{ts}'"
                f" OR (LastModifiedDate = '{ts}' AND Id > '{escaped_id}'))"
            )
        else:
            soql += f" WHERE LastModifiedDate >= '{ts}'"

        if has_id:
            soql += " ORDER BY LastModifiedDate ASC, Id ASC"
        else:
            soql += " ORDER BY LastModifiedDate ASC"

    return soql


def read_from_salesforce(object_name, watermark_ts=None, watermark_id=None):
    """
    Pull data from Salesforce using the schema stored in the catalog table.

    Uses incremental filter only if LastModifiedDate exists on the object.
    """
    fields = get_fields_from_catalog(object_name)

    if not fields:
        raise RuntimeError(f"No queryable fields found in catalog for {object_name}")

    # Ensure Id is available for merge if possible
    if "Id" not in fields:
        raise RuntimeError(f"Object {object_name} does not have Id in the catalog field list.")

    has_lmd = "LastModifiedDate" in fields
    soql = build_soql(
        object_name=object_name,
        fields=fields,
        watermark_ts=watermark_ts if has_lmd else None,
        watermark_id=watermark_id if has_lmd else None
    )

    load_type = "incremental" if (watermark_ts and has_lmd) else "full"

    print(f"📥 [{object_name}] {load_type} load | {len(fields)} fields")

    df = (
        spark.read
        .format("com.springml.spark.salesforce")
        .option("username", SF_USERNAME)
        .option("password", SF_PASSWORD + SF_SECURITY_TOKEN)
        .option("soql", soql)
        .option("bulk", "true")
        .option("pkChunking", "true")
        .option("chunkSize", CHUNK_SIZE)
        .load()
    )

    # Add lineage columns
    df = (
        df.withColumn("_sf_object", F.lit(object_name))
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_load_type", F.lit(load_type))
    )

    return df, load_type


def get_watermark_from_loaded_df(df):
    """
    Compute the next watermark from the actual data loaded.

    Returns:
    - (max_last_modified_date, max_id_at_that_timestamp)

    If LastModifiedDate is not available, returns (None, None).
    """
    if "LastModifiedDate" not in df.columns:
        return None, None

    max_ts_row = df.select(F.max("LastModifiedDate").alias("max_ts")).collect()[0]
    max_ts = max_ts_row["max_ts"]

    if max_ts is None:
        return None, None

    # Among rows sharing the latest timestamp, choose the highest Id
    max_id_row = (
        df.filter(F.col("LastModifiedDate") == F.lit(max_ts))
          .select(F.max("Id").alias("max_id"))
          .collect()[0]
    )

    return max_ts, max_id_row["max_id"]


def write_to_delta(object_name, df):
    """
    Write data into the target Delta table.

    First run:
    - overwrite/create table

    Subsequent runs:
    - merge on Id
    """
    table_name = f"{TARGET_DB}.{object_name.lower()}"

    if not table_exists(table_name):
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )

        spark.sql(f"""
            ALTER TABLE {table_name}
            SET TBLPROPERTIES (
              'delta.autoOptimize.optimizeWrite' = 'true',
              'delta.autoOptimize.autoCompact' = 'true',
              'delta.enableChangeDataFeed' = 'true'
            )
        """)
    else:
        target = DeltaTable.forName(spark, table_name)

        (
            target.alias("target")
            .merge(df.alias("source"), "target.Id = source.Id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


def copy_object(object_name):
    """
    End-to-end load for one object.

    Steps:
    1. Read watermark
    2. Read from Salesforce
    3. Merge into Delta
    4. Update watermark using actual loaded source values
    5. Log result
    """
    started_at = now_utc()

    try:
        watermark_ts, watermark_id = get_watermark(object_name)

        df, load_type = read_from_salesforce(object_name, watermark_ts, watermark_id)

        row_count = df.count()

        if row_count == 0:
            ended_at = now_utc()
            print(f"⏭️ {object_name:<40} no new records")
            log_run(
                object_name=object_name,
                status="skipped",
                row_count=0,
                load_type=load_type,
                started_at=started_at,
                ended_at=ended_at,
                message="no new records"
            )
            return (object_name, "skipped", 0)

        write_to_delta(object_name, df)

        next_watermark_ts, next_watermark_id = get_watermark_from_loaded_df(df)

        # Update watermark only if we actually loaded rows with LastModifiedDate
        if next_watermark_ts is not None:
            set_watermark(object_name, next_watermark_ts, next_watermark_id)

        ended_at = now_utc()

        print(f"✅ {object_name:<40} {row_count:>10,} rows")
        log_run(
            object_name=object_name,
            status="success",
            row_count=row_count,
            load_type=load_type,
            started_at=started_at,
            ended_at=ended_at,
            message=None
        )

        return (object_name, "success", row_count)

    except Exception as e:
        ended_at = now_utc()
        err = str(e)

        print(f"❌ {object_name:<40} {err[:120]}")
        log_run(
            object_name=object_name,
            status="failed",
            row_count=0,
            load_type=None,
            started_at=started_at,
            ended_at=ended_at,
            message=err
        )

        return (object_name, "failed", err)


def load_all(objects=None):
    """
    Serial hourly load.

    This intentionally runs one object at a time for:
    - simpler troubleshooting
    - more predictable Salesforce pressure
    - easier operational behavior in Databricks jobs
    """
    ensure_prerequisites()

    targets = objects or get_objects_from_catalog()

    print(f"\n{'═' * 60}")
    print(f"Salesforce Load - {now_utc().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'═' * 60}\n")
    print(f"Loading {len(targets)} objects\n")

    results = []

    for obj in targets:
        results.append(copy_object(obj))

    success = [r for r in results if r[1] == "success"]
    skipped = [r for r in results if r[1] == "skipped"]
    failed = [r for r in results if r[1] == "failed"]

    total_rows = sum(r[2] for r in success if isinstance(r[2], int))

    print(f"\n{'═' * 60}")
    print(f"✅ Loaded  : {len(success)} objects ({total_rows:,} rows)")
    print(f"⏭️ Skipped : {len(skipped)} objects")
    print(f"❌ Failed  : {len(failed)} objects")

    if failed:
        print("\nFailed objects:")
        for obj, _, err in failed:
            print(f"  {obj}: {err}")

    print(f"{'═' * 60}\n")

    return results


# -----------------------------------------------------------------------------
# RUN
# -----------------------------------------------------------------------------
if OBJECT_FILTER:
    load_all([OBJECT_FILTER])
else:
    load_all()
