# =============================================================================
# sf_catalog.py
# Schedule : Monthly
# Purpose  : Discover Salesforce objects and fields, then persist the latest
#            schema catalog into Delta for downstream loaders.
# =============================================================================

from simple_salesforce import Salesforce
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import functions as F
from pyspark.sql import types as T

# -----------------------------------------------------------------------------
# ENVIRONMENT CONFIG
# Keep all environment-specific values here
# -----------------------------------------------------------------------------
SF_SECRET_SCOPE     = "salesforce"
SF_USERNAME_KEY     = "username"
SF_PASSWORD_KEY     = "password"
SF_SECURITY_TOKEN_KEY = "security_token"

TARGET_DB           = "salesforce_bronze"
CATALOG_TABLE       = f"{TARGET_DB}._catalog"
CATALOG_AUDIT_TABLE = f"{TARGET_DB}._catalog_audit"

MAX_WORKERS = 12

# These field types cause problems with generic Spark reads — skip them
SKIP_TYPES = {"address", "location", "base64", "anyType"}

# -----------------------------------------------------------------------------
# SALESFORCE AUTH
# -----------------------------------------------------------------------------
SF_USERNAME       = dbutils.secrets.get(SF_SECRET_SCOPE, SF_USERNAME_KEY)
SF_PASSWORD       = dbutils.secrets.get(SF_SECRET_SCOPE, SF_PASSWORD_KEY)
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
    # Ask Salesforce for all objects, then keep only ones we can bulk-replicate
    # queryable + replicateable = safe to extract; skip deprecated/hidden ones
    result  = sf.describe()
    objects = [
        obj["name"]
        for obj in result["sobjects"]
        if obj.get("queryable") is True
        and obj.get("replicateable") is True
        and obj.get("deprecatedAndHidden") is False
    ]
    return sorted(objects)


def describe_object(object_name):
    # Fetch field metadata for one object and map it to our catalog row format.
    # Returns an empty list on failure so one bad object doesn't stop the whole run.
    try:
        meta = getattr(sf, object_name).describe()
        rows = []
        for field in meta["fields"]:
            field_type   = field.get("type")
            # Mark field as non-queryable if its type is known to cause load issues
            is_queryable = (
                field_type not in SKIP_TYPES
                and not field.get("deprecatedAndHidden", False)
            )
            rows.append({
                "sf_object":   object_name,
                "field_name":  field.get("name"),
                "field_type":  field_type,
                "field_label": field.get("label"),
                "is_queryable": bool(is_queryable),
                "is_nullable":  bool(field.get("nillable", False)),
                "is_custom":    bool(field.get("custom", False)),
                "length":       int(field.get("length") or 0),
            })
        return rows

    except Exception as e:
        print(f"❌ describe failed [{object_name}]: {e}")
        return []


def build_catalog_df(rows):
    # Convert the collected Python dicts into a Spark DataFrame with a fixed schema.
    # Explicit schema prevents type inference surprises when field lists change.
    schema = T.StructType([
        T.StructField("sf_object",    T.StringType(),  False),
        T.StructField("field_name",   T.StringType(),  False),
        T.StructField("field_type",   T.StringType(),  True),
        T.StructField("field_label",  T.StringType(),  True),
        T.StructField("is_queryable", T.BooleanType(), False),
        T.StructField("is_nullable",  T.BooleanType(), False),
        T.StructField("is_custom",    T.BooleanType(), False),
        T.StructField("length",       T.IntegerType(), True),
    ])
    return (
        spark.createDataFrame(rows, schema=schema)
        .withColumn("cataloged_at", F.current_timestamp())
    )


def diff_catalog(new_df):
    # Compare the new catalog against what's currently stored.
    # Uses Spark left-anti joins so we only collect the delta to the driver,
    # not all fields — safe for large orgs with hundreds of objects.
    if not spark.catalog.tableExists(CATALOG_TABLE):
        print("ℹ️  No existing catalog found — this looks like the first run.")
        return

    print("Comparing new catalog against existing catalog...")

    existing        = spark.table(CATALOG_TABLE)
    existing_fields = existing.select("sf_object", "field_name")
    new_fields      = new_df.select("sf_object", "field_name")
    existing_objects = existing.select("sf_object").distinct()
    new_objects      = new_df.select("sf_object").distinct()

    # Find what changed — only the delta is collected, not the full catalog
    added_objects_df   = new_objects.join(existing_objects, "sf_object", "left_anti")
    removed_objects_df = existing_objects.join(new_objects, "sf_object", "left_anti")
    added_fields_df    = new_fields.join(existing_fields, ["sf_object", "field_name"], "left_anti")
    removed_fields_df  = existing_fields.join(new_fields, ["sf_object", "field_name"], "left_anti")

    added_objects   = sorted(r["sf_object"] for r in added_objects_df.collect())
    removed_objects = sorted(r["sf_object"] for r in removed_objects_df.collect())
    added_fields    = sorted((r["sf_object"], r["field_name"]) for r in added_fields_df.collect())
    removed_fields  = sorted((r["sf_object"], r["field_name"]) for r in removed_fields_df.collect())

    print(f"\n{'─' * 60}")
    print("Schema Diff")
    print(f"{'─' * 60}")
    print(f"New objects     : {len(added_objects)}")
    print(f"Removed objects : {len(removed_objects)}")
    print(f"New fields      : {len(added_fields)}")
    print(f"Removed fields  : {len(removed_fields)}")

    if added_objects:
        print("\n➕ New objects:")
        for obj in added_objects:
            print(f"  {obj}")

    if removed_objects:
        print("\n➖ Removed objects:")
        for obj in removed_objects:
            print(f"  {obj}")

    if added_fields:
        print("\n➕ New fields:")
        for obj, field in added_fields:
            print(f"  {obj}.{field}")

    if removed_fields:
        print("\n➖ Removed fields:")
        for obj, field in removed_fields:
            print(f"  {obj}.{field}")

    print(f"{'─' * 60}\n")

    # Write the audit trail directly from Spark — no need to round-trip through Python
    if added_fields or removed_fields:
        print("Writing schema changes to audit table...")
        audit_df = (
            added_fields_df.withColumn("change_type", F.lit("added"))
            .unionAll(removed_fields_df.withColumn("change_type", F.lit("removed")))
            .withColumn("detected_at", F.current_timestamp())
        )
        audit_df.write.format("delta").mode("append").saveAsTable(CATALOG_AUDIT_TABLE)
        print(f"Audit written -> {CATALOG_AUDIT_TABLE}")


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def build_catalog():
    print(f"\n{'═' * 60}")
    print("Salesforce Schema Discovery")
    print(f"{'═' * 60}\n")

    # Step 1: ask Salesforce which objects are safe to replicate
    print("Fetching list of queryable objects from Salesforce...")
    objects = get_queryable_objects()
    print(f"Found {len(objects)} queryable objects.\n")

    all_rows       = []
    failed_objects = []

    # Step 2: describe each object in parallel — these are lightweight REST calls
    print(f"Describing objects using {MAX_WORKERS} parallel workers...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(describe_object, obj): obj for obj in objects}

        for future in as_completed(futures):
            obj  = futures[future]
            rows = future.result()
            if rows:
                all_rows.extend(rows)
                total     = len(rows)
                queryable = sum(1 for r in rows if r["is_queryable"])
                print(f"  ✅ {obj:<40} {queryable}/{total} fields queryable")
            else:
                # describe_object already printed the error — just track the failure
                failed_objects.append(obj)

    # Step 3: check if too many objects failed — could indicate a connectivity issue
    if failed_objects:
        failure_rate = len(failed_objects) / len(objects)
        print(f"\n⚠️  {len(failed_objects)} object(s) failed to describe.")
        if failure_rate > 0.1:
            raise RuntimeError(
                f"Describe failure rate too high ({len(failed_objects)}/{len(objects)}). "
                "Check Salesforce connectivity or permissions."
            )

    if not all_rows:
        raise RuntimeError("No catalog rows discovered. Check Salesforce credentials.")

    # Step 4: build the Spark DataFrame and compare against the previous catalog
    print("\nBuilding catalog DataFrame...")
    catalog_df = build_catalog_df(all_rows)

    diff_catalog(catalog_df)

    # Step 5: overwrite the catalog table with the latest snapshot
    print(f"Writing new catalog to {CATALOG_TABLE}...")
    (
        catalog_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(CATALOG_TABLE)
    )

    # Single aggregation to get both object count and field count
    stats         = catalog_df.agg(
        F.countDistinct("sf_object").alias("total_objects"),
        F.count("*").alias("total_fields")
    ).collect()[0]
    total_objects = stats["total_objects"]
    total_fields  = stats["total_fields"]

    print(f"\n{'═' * 60}")
    print(f"✅ Catalog updated -> {CATALOG_TABLE}")
    print(f"   {total_objects} objects | {total_fields} total fields")
    if failed_objects:
        print(f"   ⚠️  {len(failed_objects)} object(s) skipped due to describe errors")
    print(f"{'═' * 60}\n")


# -----------------------------------------------------------------------------
# RUN
# -----------------------------------------------------------------------------
build_catalog()
