# =============================================================================
# sf_catalog.py
# Schedule : Monthly
# Purpose  : Discover Salesforce objects and fields, then persist the latest
#            schema catalog into Delta for downstream loaders.
#
# Improvements over the current version:
# - clearer environment config at the top
# - safer object and field filtering
# - serializable output structure
# - better Databricks friendliness
# - less fragile pandas usage
# - preserves the same monthly catalog-building logic
# =============================================================================

from simple_salesforce import Salesforce
from concurrent.futures import ThreadPoolExecutor, as_completed
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
CATALOG_AUDIT_TABLE = f"{TARGET_DB}._catalog_audit"

MAX_WORKERS = 12

# Field types that are commonly troublesome for generic Spark loads
SKIP_TYPES = {"address", "location", "base64", "anyType"}

# -----------------------------------------------------------------------------
# SALESFORCE AUTH
# Username + password + security token flow
# -----------------------------------------------------------------------------
SF_USERNAME = dbutils.secrets.get(SF_SECRET_SCOPE, SF_USERNAME_KEY)
SF_PASSWORD = dbutils.secrets.get(SF_SECRET_SCOPE, SF_PASSWORD_KEY)
SF_SECURITY_TOKEN = dbutils.secrets.get(SF_SECRET_SCOPE, SF_SECURITY_TOKEN_KEY)

sf = Salesforce(
    username=SF_USERNAME,
    password=SF_PASSWORD,
    security_token=SF_SECURITY_TOKEN
)

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
def get_queryable_objects():
    """
    Return Salesforce objects that are queryable, replicateable,
    and not deprecated/hidden.

    This keeps the catalog useful for downstream bulk extraction.
    """
    result = sf.describe()

    objects = []
    for obj in result["sobjects"]:
        if (
            obj.get("queryable") is True
            and obj.get("replicateable") is True
            and obj.get("deprecatedAndHidden") is False
        ):
            objects.append(obj["name"])

    return sorted(objects)


def describe_object(object_name):
    """
    Describe one Salesforce object and return one row per field.

    If describe fails for an object, return an empty list and continue.
    """
    try:
        meta = getattr(sf, object_name).describe()

        rows = []
        for field in meta["fields"]:
            field_type = field.get("type")
            is_queryable = (
                field_type not in SKIP_TYPES
                and not field.get("deprecatedAndHidden", False)
            )

            rows.append(
                {
                    "sf_object": object_name,
                    "field_name": field.get("name"),
                    "field_type": field_type,
                    "field_label": field.get("label"),
                    "is_queryable": bool(is_queryable),
                    "is_nullable": bool(field.get("nillable", False)),
                    "is_custom": bool(field.get("custom", False)),
                    "length": int(field.get("length") or 0),
                }
            )

        return rows

    except Exception as e:
        print(f"❌ describe failed [{object_name}]: {e}")
        return []


def build_catalog_df(rows):
    """
    Convert Python rows into a Spark DataFrame with a stable schema.
    """
    schema = T.StructType([
        T.StructField("sf_object", T.StringType(), False),
        T.StructField("field_name", T.StringType(), False),
        T.StructField("field_type", T.StringType(), True),
        T.StructField("field_label", T.StringType(), True),
        T.StructField("is_queryable", T.BooleanType(), False),
        T.StructField("is_nullable", T.BooleanType(), False),
        T.StructField("is_custom", T.BooleanType(), False),
        T.StructField("length", T.IntegerType(), True),
    ])

    return (
        spark.createDataFrame(rows, schema=schema)
        .withColumn("cataloged_at", F.current_timestamp())
    )


def diff_catalog(new_df):
    """
    Compare the new catalog against the existing catalog and print a summary.

    Also appends detected adds/removals to the audit table if there are changes.
    """
    if not spark.catalog.tableExists(CATALOG_TABLE):
        print("ℹ️ No existing catalog found. This looks like the first run.")
        return

    existing = spark.table(CATALOG_TABLE)

    existing_objects = set(r["sf_object"] for r in existing.select("sf_object").distinct().collect())
    new_objects = set(r["sf_object"] for r in new_df.select("sf_object").distinct().collect())

    added_objects = new_objects - existing_objects
    removed_objects = existing_objects - new_objects

    existing_fields = set(
        (r["sf_object"], r["field_name"])
        for r in existing.select("sf_object", "field_name").collect()
    )
    new_fields = set(
        (r["sf_object"], r["field_name"])
        for r in new_df.select("sf_object", "field_name").collect()
    )

    added_fields = new_fields - existing_fields
    removed_fields = existing_fields - new_fields

    print(f"\n{'─' * 60}")
    print("Schema Diff")
    print(f"{'─' * 60}")
    print(f"New objects     : {len(added_objects)}")
    print(f"Removed objects : {len(removed_objects)}")
    print(f"New fields      : {len(added_fields)}")
    print(f"Removed fields  : {len(removed_fields)}")

    if added_objects:
        print("\n➕ New objects:")
        for obj in sorted(added_objects):
            print(f"  {obj}")

    if removed_objects:
        print("\n➖ Removed objects:")
        for obj in sorted(removed_objects):
            print(f"  {obj}")

    if added_fields:
        print("\n➕ New fields:")
        for obj, field in sorted(added_fields):
            print(f"  {obj}.{field}")

    if removed_fields:
        print("\n➖ Removed fields:")
        for obj, field in sorted(removed_fields):
            print(f"  {obj}.{field}")

    print(f"{'─' * 60}\n")

    if added_fields or removed_fields:
        audit_rows = []
        for obj, field in sorted(added_fields):
            audit_rows.append((obj, field, "added"))
        for obj, field in sorted(removed_fields):
            audit_rows.append((obj, field, "removed"))

        audit_schema = T.StructType([
            T.StructField("sf_object", T.StringType(), False),
            T.StructField("field_name", T.StringType(), False),
            T.StructField("change_type", T.StringType(), False),
        ])

        audit_df = (
            spark.createDataFrame(audit_rows, schema=audit_schema)
            .withColumn("detected_at", F.current_timestamp())
        )

        audit_df.write.format("delta").mode("append").saveAsTable(CATALOG_AUDIT_TABLE)
        print(f"Diff saved -> {CATALOG_AUDIT_TABLE}")


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def build_catalog():
    """
    Discover queryable Salesforce objects, describe them, compare with the
    existing catalog, and overwrite the catalog with the latest snapshot.
    """
    print(f"\n{'═' * 60}")
    print("Salesforce Schema Discovery")
    print(f"{'═' * 60}\n")

    objects = get_queryable_objects()
    print(f"Found {len(objects)} queryable objects\n")

    all_rows = []

    # Parallelism is acceptable here because describe calls are lightweight
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(describe_object, obj): obj for obj in objects}

        for future in as_completed(futures):
            rows = future.result()
            all_rows.extend(rows)

            if rows:
                obj_name = rows[0]["sf_object"]
                total = len(rows)
                queryable = sum(1 for r in rows if r["is_queryable"])
                print(f"✅ {obj_name:<40} {queryable}/{total} fields")

    if not all_rows:
        raise RuntimeError("No catalog rows were discovered from Salesforce.")

    catalog_df = build_catalog_df(all_rows)

    diff_catalog(catalog_df)

    (
        catalog_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(CATALOG_TABLE)
    )

    total_objects = catalog_df.select("sf_object").distinct().count()
    total_fields = catalog_df.count()

    print(f"\n{'═' * 60}")
    print(f"✅ Catalog updated -> {CATALOG_TABLE}")
    print(f"{total_objects} objects | {total_fields} total fields")
    print(f"{'═' * 60}\n")


# -----------------------------------------------------------------------------
# RUN
# -----------------------------------------------------------------------------
build_catalog()
