# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Table Relationship Scanner
# MAGIC Scans all bronze tables, detects Salesforce ID columns by value pattern,
# MAGIC cross-references them across all tables, and writes confirmed relationships
# MAGIC into `silver._schema_table`.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

# ── Edit these ──────────────────────────────────────────────────────────────

BRONZE_SCHEMA     = "bronze"          # schema where bronze tables live
SILVER_SCHEMA     = "silver"          # schema where _schema_table lives
SAMPLE_SIZE       = 200               # rows sampled per column
MATCH_THRESHOLD   = 0.80              # 80%+ SF ID values = FK candidate
MIN_NONNULL_ROWS  = 10                # skip columns with fewer non-null rows

# Known standard SF object prefixes — extend as needed
KNOWN_SF_PREFIXES = {
    "001": "account",
    "003": "contact",
    "005": "user",
    "006": "opportunity",
    "007": "activity",
    "00B": "report",
    "00D": "organization",
    "00E": "profile",
    "00G": "group",
    "00Q": "lead",
    "00T": "task",
    "00U": "event",
    "01I": "pricebook2",
    "01s": "pricebook_entry",
    "02i": "opportunity_line_item",
    "500": "case",
    "701": "campaign",
    "00a": "document",
}

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Setup — Create `_schema_table` if not exists

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}._schema_table (
        object_name      STRING   NOT NULL,   -- source table
        column_name      STRING   NOT NULL,   -- column in source table
        data_type        STRING,              -- spark data type
        is_pk            BOOLEAN  DEFAULT false,
        is_fk            BOOLEAN  DEFAULT false,
        fk_references    STRING,              -- e.g. 'bronze.account.sf_id'
        sf_prefix        STRING,              -- 3-char SF object prefix
        confidence       STRING,              -- HIGH / MEDIUM / LOW
        match_rate       DOUBLE,              -- % of values matching SF ID pattern
        null_rate        DOUBLE,              -- % of null values in column
        sample_size      INT,                 -- rows sampled
        notes            STRING,              -- e.g. POLYMORPHIC, UNRESOLVED
        scanned_at       TIMESTAMP DEFAULT current_timestamp()
    )
    USING DELTA
""")

print("✅ _schema_table ready")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Scanner Functions

# COMMAND ----------

import re
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from datetime import datetime

# ── SF ID pattern ────────────────────────────────────────────────────────────
# 15 or 18 alphanumeric chars, starts with 3-char prefix
SF_ID_REGEX = re.compile(r'^[a-zA-Z0-9]{15}([a-zA-Z0-9]{3})?$')


def is_sf_id(value: str) -> bool:
    """Check if a single value looks like a Salesforce ID."""
    if not value or not isinstance(value, str):
        return False
    return bool(SF_ID_REGEX.match(value)) and len(value) in (15, 18)


def get_sf_prefix(value: str) -> str:
    """Extract 3-char object key prefix from SF ID."""
    if value and len(value) >= 3:
        return value[:3]
    return None


def scan_column(table_name: str, col_name: str, sample_size: int) -> dict:
    """
    Sample a column and assess whether it contains SF IDs.
    Returns a result dict with match_rate, prefix, null_rate.
    """
    df = spark.sql(f"""
        SELECT `{col_name}`
        FROM {table_name}
        LIMIT {sample_size}
    """)

    total_rows   = df.count()
    if total_rows == 0:
        return None

    null_rows    = df.filter(F.col(f"`{col_name}`").isNull()).count()
    nonnull_rows = total_rows - null_rows
    null_rate    = round(null_rows / total_rows, 4)

    if nonnull_rows < MIN_NONNULL_ROWS:
        return None

    # Collect non-null values for pattern check
    values = [
        row[col_name] for row in
        df.filter(F.col(f"`{col_name}`").isNotNull())
          .select(col_name)
          .collect()
    ]

    sf_matches  = [v for v in values if is_sf_id(str(v))]
    match_rate  = round(len(sf_matches) / nonnull_rows, 4)

    if match_rate < MATCH_THRESHOLD:
        return None

    # Extract dominant prefix
    prefixes    = [get_sf_prefix(str(v)) for v in sf_matches if v]
    prefix_counts = {}
    for p in prefixes:
        prefix_counts[p] = prefix_counts.get(p, 0) + 1
    dominant_prefix = max(prefix_counts, key=prefix_counts.get) if prefix_counts else None

    return {
        "match_rate":      match_rate,
        "null_rate":       null_rate,
        "sf_prefix":       dominant_prefix,
        "sample_size":     total_rows,
        "nonnull_rows":    nonnull_rows,
    }


def resolve_prefix_to_table(prefix: str, bronze_tables: list) -> tuple:
    """
    Try to resolve a 3-char SF prefix to a bronze table.
    1. Check known standard prefix registry
    2. Cross-reference values against all bronze sf_id columns
    Returns (resolved_table, confidence)
    """
    # Standard prefix match
    if prefix in KNOWN_SF_PREFIXES:
        known = KNOWN_SF_PREFIXES[prefix]
        if known in bronze_tables:
            return (f"{BRONZE_SCHEMA}.{known}", "HIGH")
        else:
            return (known, "MEDIUM")  # known object but not in our 16 tables

    # Custom object prefix — cross-reference values
    return (None, "LOW")


def cross_reference_values(
    source_table: str,
    source_col:   str,
    bronze_tables: list,
    sample_size:  int
) -> tuple:
    """
    For LOW/unresolved columns, sample values and check which bronze
    table's sf_id column they overlap with most.
    Returns (best_match_table, overlap_rate)
    """
    source_vals = spark.sql(f"""
        SELECT DISTINCT `{source_col}`
        FROM {source_table}
        WHERE `{source_col}` IS NOT NULL
        LIMIT {sample_size}
    """)

    best_table    = None
    best_overlap  = 0.0
    source_count  = source_vals.count()

    if source_count == 0:
        return (None, 0.0)

    for table in bronze_tables:
        full_table = f"{BRONZE_SCHEMA}.{table}"

        # Skip self-join
        if full_table == source_table:
            continue

        try:
            target_vals = spark.sql(f"SELECT DISTINCT sf_id FROM {full_table}")
            overlap     = source_vals.join(
                target_vals,
                source_vals[source_col] == target_vals["sf_id"],
                "inner"
            ).count()

            overlap_rate = round(overlap / source_count, 4)
            if overlap_rate > best_overlap:
                best_overlap = overlap_rate
                best_table   = full_table
        except Exception:
            continue  # table may not have sf_id yet

    return (best_table, best_overlap)


def detect_polymorphic(
    source_table: str,
    source_col:   str,
    sample_size:  int
) -> bool:
    """
    Check if a column contains SF IDs from multiple different prefixes.
    If yes, it's likely polymorphic (e.g. WhatId, WhoId).
    """
    values = spark.sql(f"""
        SELECT DISTINCT LEFT(`{source_col}`, 3) AS prefix
        FROM {source_table}
        WHERE `{source_col}` IS NOT NULL
        LIMIT {sample_size}
    """).collect()

    unique_prefixes = set([r["prefix"] for r in values if r["prefix"]])
    return len(unique_prefixes) > 1

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Run Scanner

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, IntegerType, TimestampType

# ── Get all bronze tables ────────────────────────────────────────────────────
bronze_tables_df = spark.sql(f"SHOW TABLES IN {BRONZE_SCHEMA}")
bronze_tables    = [row["tableName"] for row in bronze_tables_df.collect()]
print(f"📋 Found {len(bronze_tables)} bronze tables: {bronze_tables}")

# ── Results collector ────────────────────────────────────────────────────────
results = []

# ── Scan each table ──────────────────────────────────────────────────────────
for table in bronze_tables:
    full_table = f"{BRONZE_SCHEMA}.{table}"
    print(f"\n🔍 Scanning {full_table}...")

    # Get columns and their types
    columns_df = spark.sql(f"DESCRIBE TABLE {full_table}")
    columns    = [(row["col_name"], row["data_type"]) for row in columns_df.collect()
                  if not row["col_name"].startswith("#")]  # skip partition headers

    for col_name, data_type in columns:

        # Only scan string-type columns — SF IDs are always strings
        if "string" not in data_type.lower() and "varchar" not in data_type.lower():
            continue

        # Skip obvious non-FK columns
        if col_name.lower() in ("sf_id", "_ingested_at", "_source", "record_type"):
            continue

        print(f"   · {col_name}...", end=" ")

        scan_result = scan_column(full_table, col_name, SAMPLE_SIZE)

        if not scan_result:
            print("skip")
            continue

        print(f"match_rate={scan_result['match_rate']} prefix={scan_result['sf_prefix']}")

        # ── Determine if PK or FK ────────────────────────────────────────────
        is_pk = col_name.lower() in ("id", "sf_id")
        is_fk = not is_pk

        # ── Resolve FK target ────────────────────────────────────────────────
        fk_references = None
        confidence    = None
        notes         = None

        if is_fk:
            prefix = scan_result["sf_prefix"]

            # Check for polymorphic
            if detect_polymorphic(full_table, col_name, SAMPLE_SIZE):
                notes         = "POLYMORPHIC"
                confidence    = "LOW"
                fk_references = None
            else:
                # Try prefix resolution first
                fk_references, confidence = resolve_prefix_to_table(prefix, bronze_tables)

                # If LOW confidence (custom object), cross-reference values
                if confidence == "LOW":
                    matched_table, overlap_rate = cross_reference_values(
                        full_table, col_name, bronze_tables, SAMPLE_SIZE
                    )
                    if matched_table and overlap_rate >= 0.5:
                        fk_references = f"{matched_table}.sf_id"
                        confidence    = "MEDIUM"
                        notes         = f"value_overlap={overlap_rate}"
                    elif matched_table and overlap_rate > 0:
                        fk_references = f"{matched_table}.sf_id"
                        confidence    = "LOW"
                        notes         = f"value_overlap={overlap_rate}"
                    else:
                        notes = "UNRESOLVED"
                else:
                    fk_references = f"{fk_references}.sf_id" if fk_references else None

        results.append({
            "object_name":   table,
            "column_name":   col_name,
            "data_type":     data_type,
            "is_pk":         is_pk,
            "is_fk":         is_fk,
            "fk_references": fk_references,
            "sf_prefix":     scan_result["sf_prefix"],
            "confidence":    confidence,
            "match_rate":    scan_result["match_rate"],
            "null_rate":     scan_result["null_rate"],
            "sample_size":   scan_result["sample_size"],
            "notes":         notes,
            "scanned_at":    datetime.utcnow().isoformat(),
        })

print(f"\n✅ Scan complete — {len(results)} FK candidates found")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Write Results to `_schema_table`

# COMMAND ----------

if results:
    results_df = spark.createDataFrame(results)

    # Overwrite today's scan results — keeps _schema_table fresh
    results_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"scanned_at >= '{datetime.utcnow().date()}'") \
        .saveAsTable(f"{SILVER_SCHEMA}._schema_table")

    print(f"✅ {len(results)} rows written to {SILVER_SCHEMA}._schema_table")
else:
    print("⚠️ No FK candidates found — check BRONZE_SCHEMA and MATCH_THRESHOLD")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Relationship Summary Report

# COMMAND ----------

print("=" * 60)
print("RELATIONSHIP SCAN REPORT")
print("=" * 60)

# ── HIGH confidence relationships ───────────────────────────────────────────
print("\n🟢 HIGH Confidence Relationships:")
spark.sql(f"""
    SELECT object_name, column_name, fk_references, match_rate, null_rate
    FROM {SILVER_SCHEMA}._schema_table
    WHERE confidence = 'HIGH' AND is_fk = true
    ORDER BY object_name, column_name
""").show(truncate=False)

# ── MEDIUM confidence relationships ──────────────────────────────────────────
print("\n🟡 MEDIUM Confidence Relationships (verify before using):")
spark.sql(f"""
    SELECT object_name, column_name, fk_references, match_rate, notes
    FROM {SILVER_SCHEMA}._schema_table
    WHERE confidence = 'MEDIUM' AND is_fk = true
    ORDER BY object_name, column_name
""").show(truncate=False)

# ── LOW / Unresolved ─────────────────────────────────────────────────────────
print("\n🔴 LOW Confidence / Unresolved (manual review needed):")
spark.sql(f"""
    SELECT object_name, column_name, sf_prefix, match_rate, notes
    FROM {SILVER_SCHEMA}._schema_table
    WHERE (confidence = 'LOW' OR notes = 'UNRESOLVED' OR notes = 'POLYMORPHIC')
    AND is_fk = true
    ORDER BY object_name, column_name
""").show(truncate=False)

# ── Full graph summary ───────────────────────────────────────────────────────
print("\n📊 Full Relationship Graph:")
spark.sql(f"""
    SELECT
        object_name                          AS child_table,
        column_name                          AS fk_column,
        fk_references                        AS parent_table,
        confidence,
        ROUND(match_rate * 100, 1)           AS match_pct,
        ROUND(null_rate  * 100, 1)           AS null_pct,
        notes
    FROM {SILVER_SCHEMA}._schema_table
    WHERE is_fk = true
    ORDER BY confidence, object_name
""").show(50, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Export FK_RELATIONS Config — Paste into silver_pipeline.py

# COMMAND ----------

# Auto-generate FK_RELATIONS config blocks ready to paste into silver_pipeline.py

print("=" * 60)
print("AUTO-GENERATED FK_RELATIONS — paste into silver_pipeline.py")
print("=" * 60)

fk_rows = spark.sql(f"""
    SELECT object_name, column_name, fk_references, confidence
    FROM {SILVER_SCHEMA}._schema_table
    WHERE is_fk = true
      AND confidence IN ('HIGH', 'MEDIUM')
      AND fk_references IS NOT NULL
    ORDER BY object_name, column_name
""").collect()

# Group by object
fk_map = {}
for row in fk_rows:
    obj = row["object_name"]
    if obj not in fk_map:
        fk_map[obj] = []
    # Extract parent object name from fk_references (bronze.<parent>.sf_id)
    parent = row["fk_references"].split(".")[1] if row["fk_references"] else "unknown"
    fk_map[obj].append((row["column_name"], parent, row["confidence"]))

for obj, fks in fk_map.items():
    print(f"\n# {obj.upper()}")
    print(f'OBJECT_NAME  = "{obj}"')
    print(f'FK_RELATIONS = [')
    for fk_col, parent, confidence in fks:
        print(f'    ("{fk_col}", "{parent}"),  # {confidence}')
    print(f']')
