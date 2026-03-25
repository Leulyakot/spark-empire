# =============================================================================
# sf_catalog.py
# Schedule : Monthly
# Purpose  : Discover all Salesforce objects and fields, save schema to Delta.
#            Run once to bootstrap, then monthly to pick up schema changes.
# =============================================================================

from simple_salesforce import Salesforce
from pyspark.sql.functions import current_timestamp
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────

SF_USERNAME       = dbutils.secrets.get("salesforce", "username")
SF_PASSWORD       = dbutils.secrets.get("salesforce", "password")
SF_SECURITY_TOKEN = dbutils.secrets.get("salesforce", "security_token")

TARGET_DB         = "salesforce_bronze"
MAX_WORKERS       = 20          # aggressive — these are lightweight API calls only
SKIP_TYPES        = {"address", "location", "base64", "anyType"}

# ── Init ──────────────────────────────────────────────────────────────────────

sf = Salesforce(
    username       = SF_USERNAME,
    password       = SF_PASSWORD,
    security_token = SF_SECURITY_TOKEN
)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_DB}")

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_queryable_objects():
    """Return all queryable, replicateable objects in the org."""
    result = sf.describe()
    return [
        obj["name"] for obj in result["sobjects"]
        if obj["queryable"]
        and obj["replicateable"]
        and not obj["deprecatedAndHidden"]
    ]


def describe_object(object_name):
    """Return one row per field for a given Salesforce object."""
    try:
        meta = getattr(sf, object_name).describe()
        return [
            {
                "sf_object":    object_name,
                "field_name":   f["name"],
                "field_type":   f["type"],
                "field_label":  f["label"],
                "is_queryable": f["type"] not in SKIP_TYPES
                                and not f.get("deprecatedAndHidden", False),
                "is_nullable":  f["nillable"],
                "is_custom":    f["custom"],
                "length":       f.get("length", 0),
            }
            for f in meta["fields"]
        ]
    except Exception as e:
        print(f"❌  describe failed [{object_name}]: {e}")
        return []


def diff_catalog(new_df):
    """
    Compare incoming schema against the existing catalog.
    Prints a summary of new objects, new fields, and removed fields.
    """
    catalog_table = f"{TARGET_DB}._catalog"

    if not spark.catalog.tableExists(catalog_table):
        print("ℹ️   No existing catalog found — this is the first run.")
        return

    existing = spark.table(catalog_table)

    # New objects
    existing_objects = set(r["sf_object"] for r in existing.select("sf_object").distinct().collect())
    new_objects      = set(r["sf_object"] for r in new_df.select("sf_object").distinct().collect())
    added_objects    = new_objects - existing_objects
    removed_objects  = existing_objects - new_objects

    # New / removed fields (per object)
    existing_fields = set((r["sf_object"], r["field_name"]) for r in existing.select("sf_object", "field_name").collect())
    new_fields_set  = set((r["sf_object"], r["field_name"]) for r in new_df.select("sf_object", "field_name").collect())
    added_fields    = new_fields_set  - existing_fields
    removed_fields  = existing_fields - new_fields_set

    print(f"\n{'─'*60}")
    print(f"📊  Schema Diff")
    print(f"{'─'*60}")
    print(f"   New objects     : {len(added_objects)}")
    print(f"   Removed objects : {len(removed_objects)}")
    print(f"   New fields      : {len(added_fields)}")
    print(f"   Removed fields  : {len(removed_fields)}")

    if added_objects:
        print(f"\n   ➕ New objects:")
        for obj in sorted(added_objects):
            print(f"      {obj}")

    if removed_objects:
        print(f"\n   ➖ Removed objects:")
        for obj in sorted(removed_objects):
            print(f"      {obj}")

    if added_fields:
        print(f"\n   ➕ New fields:")
        for obj, field in sorted(added_fields):
            print(f"      {obj}.{field}")

    if removed_fields:
        print(f"\n   ➖ Removed fields (will remain as null columns in Delta):")
        for obj, field in sorted(removed_fields):
            print(f"      {obj}.{field}")

    print(f"{'─'*60}\n")

    # Save diff to audit table for historical tracking
    if added_fields or removed_fields:
        audit_rows = (
            [{"sf_object": o, "field_name": f, "change_type": "added",   "detected_at": None} for o, f in added_fields] +
            [{"sf_object": o, "field_name": f, "change_type": "removed",  "detected_at": None} for o, f in removed_fields]
        )
        audit_df = spark.createDataFrame(pd.DataFrame(audit_rows))
        audit_df = audit_df.withColumn("detected_at", current_timestamp())

        (audit_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(f"{TARGET_DB}._catalog_audit")
        )
        print(f"📝  Diff saved → {TARGET_DB}._catalog_audit")


# ── Main ──────────────────────────────────────────────────────────────────────

def build_catalog():
    """
    Discover all Salesforce objects and fields.
    Diffs against existing catalog, then overwrites with latest schema.
    """
    print(f"\n{'═'*60}")
    print(f"  Salesforce Schema Discovery")
    print(f"{'═'*60}\n")

    objects = get_queryable_objects()
    print(f"Found {len(objects)} queryable objects in org\n")

    # Describe all objects in parallel
    all_rows = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(describe_object, obj): obj for obj in objects}
        for future in as_completed(futures):
            rows = future.result()
            all_rows.extend(rows)
            if rows:
                obj_name  = rows[0]["sf_object"]
                total     = len(rows)
                queryable = sum(1 for r in rows if r["is_queryable"])
                print(f"   ✅  {obj_name:<40} {queryable}/{total} fields")

    # Build catalog DataFrame
    catalog_df = spark.createDataFrame(pd.DataFrame(all_rows))
    catalog_df = catalog_df.withColumn("cataloged_at", current_timestamp())

    # Diff against existing catalog before overwriting
    diff_catalog(catalog_df)

    # Overwrite catalog with latest schema
    (catalog_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{TARGET_DB}._catalog")
    )

    total_objects = catalog_df.select("sf_object").distinct().count()
    total_fields  = catalog_df.count()

    print(f"\n{'═'*60}")
    print(f"  ✅ Catalog updated → {TARGET_DB}._catalog")
    print(f"     {total_objects} objects  |  {total_fields} total fields")
    print(f"{'═'*60}\n")


# ── Run ───────────────────────────────────────────────────────────────────────

build_catalog()
