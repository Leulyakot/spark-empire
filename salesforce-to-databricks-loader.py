# ─────────────────────────────────────────────────────────────────────────────
# Salesforce → Databricks Bronze Copy
# Pulls all objects and fields as-is. No transformations.
# ─────────────────────────────────────────────────────────────────────────────

from simple_salesforce import Salesforce
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Config ────────────────────────────────────────────────────────────────────

SF_USERNAME       = dbutils.secrets.get("salesforce", "username")
SF_PASSWORD       = dbutils.secrets.get("salesforce", "password")
SF_SECURITY_TOKEN = dbutils.secrets.get("salesforce", "security_token")

TARGET_DB         = "salesforce_bronze"
MAX_WORKERS       = 10                          # parallel threads
CHUNK_SIZE        = "100000"                    # rows per Bulk API chunk

# Field types that cannot be queried in SOQL
SKIP_TYPES        = {"address", "location", "base64", "anyType"}

# ── Init ──────────────────────────────────────────────────────────────────────

sf = Salesforce(
    username       = SF_USERNAME,
    password       = SF_PASSWORD,
    security_token = SF_SECURITY_TOKEN
)

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_DB}")

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_objects():
    """Return all queryable, replicateable Salesforce objects in this org."""
    result = sf.describe()
    return [
        obj["name"] for obj in result["sobjects"]
        if obj["queryable"]
        and obj["replicateable"]
        and not obj["deprecatedAndHidden"]
    ]


def get_fields(object_name):
    """Return all safe, queryable field names for a Salesforce object."""
    meta = getattr(sf, object_name).describe()
    return [
        f["name"] for f in meta["fields"]
        if f["type"] not in SKIP_TYPES
        and not f.get("deprecatedAndHidden", False)
    ]


def read_from_salesforce(object_name):
    """Read all fields from Salesforce via Bulk API. Returns a Spark DataFrame."""
    fields = get_fields(object_name)
    soql   = f"SELECT {', '.join(fields)} FROM {object_name}"

    df = (spark.read
        .format("com.springml.spark.salesforce")
        .option("username",    SF_USERNAME)
        .option("password",    SF_PASSWORD + SF_SECURITY_TOKEN)
        .option("soql",        soql)
        .option("bulk",        "true")
        .option("pkChunking",  "true")
        .option("chunkSize",   CHUNK_SIZE)
        .load()
    )

    # Attach audit columns — never touch source columns
    return (df
        .withColumn("_sf_object",   lit(object_name))
        .withColumn("_ingested_at", current_timestamp())
    )


def write_to_delta(object_name, df):
    """
    Write DataFrame to Delta Lake as-is.
      - First run  → creates the table
      - Subsequent → upserts on Id, auto-adds new columns (schema evolution)
    """
    table = f"{TARGET_DB}.{object_name.lower()}"

    if not spark.catalog.tableExists(table):
        # ── First load: create table ──────────────────────────────────────────
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
        # ── Incremental: upsert + auto schema evolution ───────────────────────
        (DeltaTable.forName(spark, table).alias("target")
            .merge(df.alias("source"), "target.Id = source.Id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .option("mergeSchema", "true")          # auto-add new SF fields
            .execute()
        )


# ── Core ──────────────────────────────────────────────────────────────────────

def copy_object(object_name):
    """End-to-end copy of a single Salesforce object."""
    try:
        df    = read_from_salesforce(object_name)
        count = df.count()
        write_to_delta(object_name, df)
        print(f"✅  {object_name:<40} {count:>10,} rows")
        return object_name, "success", count
    except Exception as e:
        print(f"❌  {object_name:<40} {str(e)[:80]}")
        return object_name, "failed", str(e)


def copy_all(objects=None):
    """
    Copy all (or specified) Salesforce objects to Delta Lake in parallel.

    Usage:
        copy_all()                                      # copy everything
        copy_all(["Account", "Contact", "Opportunity"]) # copy specific objects
    """
    targets = objects or get_objects()
    print(f"\nCopying {len(targets)} Salesforce objects → {TARGET_DB}\n{'─'*60}")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(copy_object, obj): obj for obj in targets}
        for future in as_completed(futures):
            results.append(future.result())

    success = [r for r in results if r[1] == "success"]
    failed  = [r for r in results if r[1] == "failed"]

    print(f"\n{'─'*60}")
    print(f"✅  Success : {len(success)}")
    print(f"❌  Failed  : {len(failed)}")
    if failed:
        print("\nFailed objects:")
        for obj, _, err in failed:
            print(f"   {obj}: {err}")

    return results


# ── Run ───────────────────────────────────────────────────────────────────────

# Copy everything
copy_all()

# Copy specific objects only
# copy_all(["Account", "Contact", "Opportunity", "Lead"])

# Copy a single object
# copy_object("Opportunity")
