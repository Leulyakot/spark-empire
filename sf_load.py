# =============================================================================
# sf_load.py
# Schedule : Hourly
# Purpose  : Load Salesforce data into Delta Lake using schema from the catalog.
#            First run = full load. Subsequent runs = incremental CDC via
#            LastModifiedDate watermark persisted in Delta.
#            No describe() API calls made — schema read entirely from catalog.
# =============================================================================

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit, max as spark_max
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────

SF_USERNAME       = dbutils.secrets.get("salesforce", "username")
SF_PASSWORD       = dbutils.secrets.get("salesforce", "password")
SF_SECURITY_TOKEN = dbutils.secrets.get("salesforce", "security_token")

TARGET_DB         = "salesforce_bronze"
MAX_WORKERS       = 10
CHUNK_SIZE        = "100000"

# ── Init ──────────────────────────────────────────────────────────────────────

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# ── Watermark ─────────────────────────────────────────────────────────────────

def get_watermark(object_name):
    """
    Read the last successful load timestamp for this object from the watermark table.
    Returns None on first run → triggers a full load.
    """
    try:
        row = spark.sql(f"""
            SELECT last_loaded_at
            FROM {TARGET_DB}._watermark
            WHERE sf_object = '{object_name}'
        """).collect()
        return row[0]["last_loaded_at"] if row else None
    except Exception:
        return None     # watermark table doesn't exist yet → full load


def set_watermark(object_name, timestamp):
    """Upsert the watermark for this object after a successful load."""
    watermark_table = f"{TARGET_DB}._watermark"

    wm_df = spark.createDataFrame(
        [(object_name, timestamp)],
        ["sf_object", "last_loaded_at"]
    )

    if not spark.catalog.tableExists(watermark_table):
        (wm_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(watermark_table)
        )
    else:
        (DeltaTable.forName(spark, watermark_table).alias("target")
            .merge(wm_df.alias("source"), "target.sf_object = source.sf_object")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

# ── Catalog ───────────────────────────────────────────────────────────────────

def get_objects_from_catalog():
    """All objects in the catalog that have at least one queryable field."""
    return [
        row["sf_object"] for row in spark.sql(f"""
            SELECT DISTINCT sf_object
            FROM {TARGET_DB}._catalog
            WHERE is_queryable = true
            ORDER BY sf_object
        """).collect()
    ]


def get_fields_from_catalog(object_name):
    """Read queryable fields for an object from the catalog. Zero API calls."""
    rows = spark.sql(f"""
        SELECT field_name
        FROM {TARGET_DB}._catalog
        WHERE sf_object    = '{object_name}'
          AND is_queryable = true
        ORDER BY field_name
    """).collect()
    return [row["field_name"] for row in rows]

# ── Read ──────────────────────────────────────────────────────────────────────

def build_soql(object_name, fields, watermark=None):
    """
    Build SOQL from catalog fields.
    Full load if no watermark, incremental if watermark exists.
    """
    field_list = ", ".join(fields)
    soql       = f"SELECT {field_list} FROM {object_name}"

    if watermark:
        # Format watermark for SOQL: 2024-01-15T08:00:00Z
        ts   = watermark.strftime("%Y-%m-%dT%H:%M:%SZ")
        soql += f" WHERE LastModifiedDate > {ts}"

    return soql


def read_from_salesforce(object_name, watermark=None):
    """
    Pull data from Salesforce.
    Uses LastModifiedDate filter for incremental loads.
    Falls back to full load if no watermark.
    """
    fields = get_fields_from_catalog(object_name)

    # Only add LastModifiedDate filter if the field exists on this object
    has_lmd = "LastModifiedDate" in fields
    soql    = build_soql(object_name, fields, watermark if has_lmd else None)

    load_type = "incremental" if (watermark and has_lmd) else "full"
    print(f"   [{object_name}] {load_type} load — {len(fields)} fields")

    df = (spark.read
        .format("com.springml.spark.salesforce")
        .option("username",   SF_USERNAME)
        .option("password",   SF_PASSWORD + SF_SECURITY_TOKEN)
        .option("soql",       soql)
        .option("bulk",       "true")
        .option("pkChunking", "true")
        .option("chunkSize",  CHUNK_SIZE)
        .load()
    )

    return (df
        .withColumn("_sf_object",   lit(object_name))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_load_type",   lit(load_type))
    )

# ── Write ─────────────────────────────────────────────────────────────────────

def write_to_delta(object_name, df):
    """
    Write to Delta Lake.
    First run  → full load, creates table.
    Subsequent → upsert on Id, auto-evolves schema for new fields.
    """
    table = f"{TARGET_DB}.{object_name.lower()}"

    if not spark.catalog.tableExists(table):
        # ── First run: create table ───────────────────────────────────────────
        (df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table)
        )
        spark.sql(f"""
            ALTER TABLE {table} SET TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact'   = 'true',
                'delta.enableChangeDataFeed'       = 'true'
            )
        """)

    else:
        # ── Incremental: upsert + schema evolution ────────────────────────────
        (DeltaTable.forName(spark, table).alias("target")
            .merge(df.alias("source"), "target.Id = source.Id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .option("mergeSchema", "true")      # auto-add new fields from catalog
            .execute()
        )

# ── Core ──────────────────────────────────────────────────────────────────────

def copy_object(object_name):
    """
    End-to-end load for one object:
      1. Read watermark
      2. Pull from Salesforce (full or incremental)
      3. Write to Delta
      4. Update watermark
    """
    run_start = datetime.now(timezone.utc)

    try:
        watermark = get_watermark(object_name)
        df        = read_from_salesforce(object_name, watermark)
        count     = df.count()

        if count == 0:
            print(f"   ⏭️   {object_name:<40} no new records")
            return object_name, "skipped", 0

        write_to_delta(object_name, df)
        set_watermark(object_name, run_start)

        print(f"   ✅  {object_name:<40} {count:>10,} rows")
        return object_name, "success", count

    except Exception as e:
        print(f"   ❌  {object_name:<40} {str(e)[:80]}")
        return object_name, "failed", str(e)


def load_all(objects=None):
    """
    Hourly load — reads schema from catalog, no Salesforce describe() calls.
    Full load on first run per object, incremental on all subsequent runs.
    """
    # Guard: ensure catalog exists before loading
    if not spark.catalog.tableExists(f"{TARGET_DB}._catalog"):
        raise RuntimeError(
            f"Catalog not found. Run sf_catalog.py first to discover your Salesforce schema."
        )

    targets = objects or get_objects_from_catalog()

    print(f"\n{'═'*60}")
    print(f"  Salesforce Load  —  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'═'*60}\n")
    print(f"  Loading {len(targets)} objects\n")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(copy_object, obj): obj for obj in targets}
        for future in as_completed(futures):
            results.append(future.result())

    # ── Summary ───────────────────────────────────────────────────────────────
    success = [r for r in results if r[1] == "success"]
    skipped = [r for r in results if r[1] == "skipped"]
    failed  = [r for r in results if r[1] == "failed"]
    total   = sum(r[2] for r in success if isinstance(r[2], int))

    print(f"\n{'═'*60}")
    print(f"  ✅  Loaded  : {len(success)} objects  ({total:,} rows)")
    print(f"  ⏭️   Skipped : {len(skipped)} objects  (no new records)")
    print(f"  ❌  Failed  : {len(failed)} objects")
    if failed:
        print(f"\n  Failed objects:")
        for obj, _, err in failed:
            print(f"     {obj}: {err}")
    print(f"{'═'*60}\n")

    return results


# ── Run ───────────────────────────────────────────────────────────────────────

load_all()

# Or load specific objects only
# load_all(["Account", "Contact", "Opportunity"])
