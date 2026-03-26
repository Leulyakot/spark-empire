# =============================================================================
# sf_load.py
# Schedule : Hourly
# Purpose  : Load Salesforce data into Delta Lake using schema from the monthly
#            catalog. First run = full load. Subsequent runs = incremental load
#            using a watermark table.
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

CATALOG_TABLE   = f"{TARGET_DB}._catalog"
WATERMARK_TABLE = f"{TARGET_DB}._watermark"
JOB_LOG_TABLE   = f"{TARGET_DB}._job_log"

CHUNK_SIZE = "100000"

DEFAULT_OBJECT_FILTER = ""

# DDL for control tables — created automatically on first run
WATERMARK_TABLE_DDL = f"""
    CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
        sf_object      STRING    NOT NULL,
        last_loaded_at TIMESTAMP,
        last_loaded_id STRING
    )
    USING DELTA
"""

JOB_LOG_TABLE_DDL = f"""
    CREATE TABLE IF NOT EXISTS {JOB_LOG_TABLE} (
        sf_object  STRING    NOT NULL,
        status     STRING    NOT NULL,
        row_count  BIGINT    NOT NULL,
        load_type  STRING,
        started_at TIMESTAMP NOT NULL,
        ended_at   TIMESTAMP NOT NULL,
        message    STRING
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
"""

# -----------------------------------------------------------------------------
# NOTEBOOK PARAMETERS
# -----------------------------------------------------------------------------
try:
    dbutils.widgets.text("object_name", DEFAULT_OBJECT_FILTER)
except Exception:
    pass  # Widget already exists on notebook re-run
OBJECT_FILTER = dbutils.widgets.get("object_name").strip()

# -----------------------------------------------------------------------------
# SALESFORCE AUTH
# Username + password + security token flow
# -----------------------------------------------------------------------------
SF_USERNAME       = dbutils.secrets.get(SF_SECRET_SCOPE, SF_USERNAME_KEY)
SF_PASSWORD       = dbutils.secrets.get(SF_SECRET_SCOPE, SF_PASSWORD_KEY)
SF_SECURITY_TOKEN = dbutils.secrets.get(SF_SECRET_SCOPE, SF_SECURITY_TOKEN_KEY)

# -----------------------------------------------------------------------------
# SPARK / DELTA SETTINGS
# -----------------------------------------------------------------------------
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
def now_utc():
    # Naive UTC datetime — Spark TimestampType doesn't accept timezone-aware datetimes
    return datetime.now(timezone.utc).replace(tzinfo=None)


def table_exists(table_name):
    return spark.catalog.tableExists(table_name)


def ensure_prerequisites():
    # Catalog must exist before we can load anything — it drives object/field selection
    if not table_exists(CATALOG_TABLE):
        raise RuntimeError(f"Catalog not found: {CATALOG_TABLE}. Run sf_catalog.py first.")

    # Watermark and job log tables are auto-created on first run
    print(f"Ensuring control tables exist in {TARGET_DB}...")
    spark.sql(WATERMARK_TABLE_DDL)
    spark.sql(JOB_LOG_TABLE_DDL)
    print("Control tables ready.")


def get_objects_from_catalog():
    # Pull the list of objects we should load — only those with at least one queryable field
    query = f"""
        SELECT DISTINCT sf_object
        FROM {CATALOG_TABLE}
        WHERE is_queryable = true
        ORDER BY sf_object
    """
    objects = [r["sf_object"] for r in spark.sql(query).collect()]
    print(f"Found {len(objects)} objects in catalog.")
    return objects


def get_fields_from_catalog(object_name):
    # Get the field list for this object — only fields marked queryable in the catalog
    query = f"""
        SELECT field_name
        FROM {CATALOG_TABLE}
        WHERE sf_object = '{object_name.replace("'", "''")}'
          AND is_queryable = true
        ORDER BY field_name
    """
    return [r["field_name"] for r in spark.sql(query).collect()]


def get_watermark(object_name):
    # Check if we've loaded this object before — if not, we'll do a full load
    query = f"""
        SELECT last_loaded_at, last_loaded_id
        FROM {WATERMARK_TABLE}
        WHERE sf_object = '{object_name.replace("'", "''")}'
    """
    rows = spark.sql(query).collect()

    if not rows:
        print(f"  No watermark found for {object_name} — will run full load.")
        return None, None

    wm_ts, wm_id = rows[0]["last_loaded_at"], rows[0]["last_loaded_id"]
    print(f"  Watermark: {wm_ts} | last id: {wm_id}")
    return wm_ts, wm_id


def set_watermark(object_name, loaded_at, loaded_id):
    # Advance the watermark to the highest LastModifiedDate/Id we actually loaded
    # Using source data max (not notebook start time) avoids gaps on slow runs
    print(f"  Updating watermark -> {loaded_at} | id: {loaded_id}")
    rows = [(object_name, loaded_at, loaded_id)]
    schema = T.StructType([
        T.StructField("sf_object",      T.StringType(),    False),
        T.StructField("last_loaded_at", T.TimestampType(), True),
        T.StructField("last_loaded_id", T.StringType(),    True),
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
            "sf_object":      "s.sf_object",
            "last_loaded_at": "s.last_loaded_at",
            "last_loaded_id": "s.last_loaded_id"
        })
        .execute()
    )


def log_run(object_name, status, row_count, load_type, started_at, ended_at, message=None):
    # Append one row to the job log so every run is auditable
    rows = [(object_name, status, int(row_count), load_type, started_at, ended_at, message)]

    schema = T.StructType([
        T.StructField("sf_object",  T.StringType(),    False),
        T.StructField("status",     T.StringType(),    False),
        T.StructField("row_count",  T.LongType(),      False),
        T.StructField("load_type",  T.StringType(),    True),
        T.StructField("started_at", T.TimestampType(), False),
        T.StructField("ended_at",   T.TimestampType(), False),
        T.StructField("message",    T.StringType(),    True),
    ])

    spark.createDataFrame(rows, schema=schema).write.mode("append").saveAsTable(JOB_LOG_TABLE)


def format_soql_timestamp(ts):
    # Salesforce SOQL expects UTC timestamps in the format: 2024-01-01T00:00:00Z
    if ts is None:
        return None
    if hasattr(ts, "tzinfo") and ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_soql(object_name, fields, watermark_ts=None, watermark_id=None):
    # Full load: SELECT all fields, no filter
    # Incremental: WHERE LastModifiedDate > ts OR (= ts AND Id > last_id)
    # The OR clause handles boundary ties so we don't skip records at the watermark edge
    field_list = ", ".join(fields)
    soql = f"SELECT {field_list} FROM {object_name}"

    has_lmd = "LastModifiedDate" in fields
    has_id  = "Id" in fields

    if watermark_ts and has_lmd:
        ts = format_soql_timestamp(watermark_ts)

        if watermark_id and has_id:
            # Precise boundary: skip everything strictly before the watermark
            escaped_id = watermark_id.replace("'", "\\'")
            soql += (
                f" WHERE (LastModifiedDate > '{ts}'"
                f" OR (LastModifiedDate = '{ts}' AND Id > '{escaped_id}'))"
            )
        else:
            # No prior Id — use >= to avoid skipping records on the watermark timestamp
            soql += f" WHERE LastModifiedDate >= '{ts}'"

        soql += " ORDER BY LastModifiedDate ASC, Id ASC" if has_id else " ORDER BY LastModifiedDate ASC"

    print(f"  SOQL: {soql[:200]}{'...' if len(soql) > 200 else ''}")
    return soql


def read_from_salesforce(object_name, watermark_ts=None, watermark_id=None):
    fields = get_fields_from_catalog(object_name)

    if not fields:
        raise RuntimeError(f"No queryable fields found in catalog for {object_name}")

    if "Id" not in fields:
        raise RuntimeError(f"{object_name} has no Id field in the catalog — cannot merge.")

    has_lmd   = "LastModifiedDate" in fields
    load_type = "incremental" if (watermark_ts and has_lmd) else "full"

    soql = build_soql(
        object_name=object_name,
        fields=fields,
        watermark_ts=watermark_ts if has_lmd else None,
        watermark_id=watermark_id if has_lmd else None
    )

    print(f"  Querying Salesforce via Bulk API ({load_type}, {len(fields)} fields)...")

    # pkChunking splits large full-load queries into parallel chunks by Id range.
    # It's incompatible with ORDER BY, so we only enable it for full loads.
    use_pk_chunking = load_type == "full"

    reader = (
        spark.read
        .format("com.springml.spark.salesforce")
        .option("username",   SF_USERNAME)
        .option("password",   SF_PASSWORD + SF_SECURITY_TOKEN)
        .option("soql",       soql)
        .option("bulk",       "true")
        .option("pkChunking", str(use_pk_chunking).lower())
    )

    if use_pk_chunking:
        reader = reader.option("chunkSize", CHUNK_SIZE)

    df = reader.load()

    # Tag every row with lineage info so downstream consumers know where it came from
    df = (
        df.withColumn("_sf_object",    F.lit(object_name))
          .withColumn("_ingested_at",  F.current_timestamp())
          .withColumn("_load_type",    F.lit(load_type))
    )

    return df, load_type


def get_load_stats(df):
    # Single Spark action to get both row count and watermark boundary.
    # max(struct(LastModifiedDate, Id)) works because Spark compares structs
    # field-by-field — so we naturally get max LMD, then max Id among ties.
    has_lmd = "LastModifiedDate" in df.columns
    has_id  = "Id" in df.columns

    exprs = [F.count("*").alias("row_count")]

    if has_lmd and has_id:
        exprs.append(F.max(F.struct("LastModifiedDate", "Id")).alias("wm"))
    elif has_lmd:
        exprs.append(F.max("LastModifiedDate").alias("max_lmd"))

    row       = df.select(*exprs).collect()[0]
    row_count = row["row_count"]

    if has_lmd and has_id:
        wm = row["wm"]
        if wm:
            return row_count, wm["LastModifiedDate"], wm["Id"]
    elif has_lmd and row["max_lmd"]:
        return row_count, row["max_lmd"], None

    return row_count, None, None


def write_to_delta(object_name, df):
    table_name = f"{TARGET_DB}.{object_name.lower()}"

    if not table_exists(table_name):
        # First time we've seen this object — create the table and set Delta properties
        print(f"  Table {table_name} not found. Creating it now...")
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
              'delta.autoOptimize.autoCompact'   = 'true',
              'delta.enableChangeDataFeed'        = 'true'
            )
        """)
        print(f"  Table created: {table_name}")
    else:
        # Table already exists — merge new/updated rows on Id (upsert)
        print(f"  Merging into {table_name}...")
        target = DeltaTable.forName(spark, table_name)
        (
            target.alias("target")
            .merge(df.alias("source"), "target.Id = source.Id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


def copy_object(object_name):
    print(f"\n--- {object_name} ---")
    started_at = now_utc()

    try:
        # Step 1: check if we have a previous load to resume from
        watermark_ts, watermark_id = get_watermark(object_name)

        # Step 2: pull data from Salesforce (full or incremental depending on watermark)
        df, load_type = read_from_salesforce(object_name, watermark_ts, watermark_id)

        # Step 3: count rows and compute the next watermark — single Spark action
        row_count, next_watermark_ts, next_watermark_id = get_load_stats(df)

        if row_count == 0:
            ended_at = now_utc()
            print(f"  No new records — skipping write.")
            log_run(
                object_name=object_name, status="skipped", row_count=0,
                load_type=load_type, started_at=started_at, ended_at=ended_at,
                message="no new records"
            )
            return (object_name, "skipped", 0)

        # Step 4: write to Delta (create or merge)
        write_to_delta(object_name, df)

        # Step 5: advance the watermark so the next run picks up from here
        if next_watermark_ts is not None:
            set_watermark(object_name, next_watermark_ts, next_watermark_id)

        ended_at = now_utc()
        print(f"✅ {object_name:<40} {row_count:>10,} rows  [{load_type}]")
        log_run(
            object_name=object_name, status="success", row_count=row_count,
            load_type=load_type, started_at=started_at, ended_at=ended_at
        )
        return (object_name, "success", row_count)

    except Exception as e:
        ended_at = now_utc()
        err      = str(e)
        print(f"❌ {object_name:<40} ERROR: {err[:120]}")
        log_run(
            object_name=object_name, status="failed", row_count=0,
            load_type=None, started_at=started_at, ended_at=ended_at, message=err
        )
        return (object_name, "failed", err)


def load_all(objects=None):
    # Runs one object at a time — simpler to debug and easier on Salesforce API limits
    print(f"\n{'═' * 60}")
    print(f"Salesforce Load — {now_utc().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'═' * 60}")

    # Make sure catalog exists and control tables are in place before we start
    ensure_prerequisites()

    targets = objects or get_objects_from_catalog()
    print(f"Loading {len(targets)} object(s)...\n")

    results = []
    for obj in targets:
        results.append(copy_object(obj))

    # Tally up what happened
    success = [r for r in results if r[1] == "success"]
    skipped = [r for r in results if r[1] == "skipped"]
    failed  = [r for r in results if r[1] == "failed"]
    total_rows = sum(r[2] for r in success if isinstance(r[2], int))

    print(f"\n{'═' * 60}")
    print(f"✅ Loaded  : {len(success)} objects  ({total_rows:,} rows)")
    print(f"⏭️  Skipped : {len(skipped)} objects")
    print(f"❌ Failed  : {len(failed)} objects")

    if failed:
        print("\nFailed objects:")
        for obj, _, err in failed:
            print(f"  {obj}: {err}")

    print(f"{'═' * 60}\n")

    # Raise so the Databricks job shows red and alerts fire — don't silently succeed
    if failed:
        names = ", ".join(obj for obj, _, _ in failed)
        raise RuntimeError(f"{len(failed)} object(s) failed to load: {names}")

    return results


# -----------------------------------------------------------------------------
# RUN
# -----------------------------------------------------------------------------
if OBJECT_FILTER:
    print(f"Running single-object mode: {OBJECT_FILTER}")
    load_all([OBJECT_FILTER])
else:
    load_all()
