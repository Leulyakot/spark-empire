# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Pipeline — Salesforce Objects
# MAGIC Generic, repeatable pipeline for all 16 Salesforce objects.
# MAGIC
# MAGIC **Steps per object:**
# MAGIC 1. Register new SF IDs into `_id_map`
# MAGIC 2. Deduplicate bronze
# MAGIC 3. Resolve FK surrogate keys
# MAGIC 4. Merge into silver
# MAGIC 5. Log orphaned FKs to `_error_log`

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Configuration — Edit Here

# COMMAND ----------

# ── Widget parameters (set by Databricks Workflows job) ────────────────────
dbutils.widgets.text("object_name",   "<object_name>")   # e.g. "contact"
dbutils.widgets.text("watermark_col", "last_modified_date")

OBJECT_NAME   = dbutils.widgets.get("object_name")
WATERMARK_COL = dbutils.widgets.get("watermark_col")
BRONZE_TABLE  = f"bronze.{OBJECT_NAME}"
SILVER_TABLE  = f"silver.{OBJECT_NAME}"
MERGE_KEY     = "sf_id"                                   # always sf_id

# FK relationships — list of (fk_col_in_bronze, parent_object_name)
# e.g. [("account_id", "account"), ("owner_id", "user")]
# Leave empty [] if object has no FKs
FK_RELATIONS  = [
    ("<parent_id_col>", "<parent_object_name>"),
]

# Business columns to sync — exclude sf_id, is_deleted, system fields
BUSINESS_COLS = [
    "<col_1>",
    "<col_2>",
    # add more...
]

# ── Fixed metadata columns — do not edit ───────────────────────────────────
META_COLS = ["_created_at", "_updated_at", "_is_deleted"]

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Metadata Tables DDL — Run Once

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver._id_map (
        silver_id    BIGINT       GENERATED ALWAYS AS IDENTITY,
        object_name  STRING       NOT NULL,
        sf_id        VARCHAR(18)  NOT NULL,
        created_at   TIMESTAMP    NOT NULL DEFAULT current_timestamp()
    )
    USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver._watermark_log (
        object_name       STRING     NOT NULL,
        last_watermark    TIMESTAMP  NOT NULL,
        updated_at        TIMESTAMP  NOT NULL DEFAULT current_timestamp()
    )
    USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver._run_log (
        run_id         BIGINT     GENERATED ALWAYS AS IDENTITY,
        object_name    STRING     NOT NULL,
        status         STRING     NOT NULL,   -- STARTED, SUCCESS, FAILED
        rows_processed INT,
        started_at     TIMESTAMP  NOT NULL DEFAULT current_timestamp(),
        completed_at   TIMESTAMP
    )
    USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver._error_log (
        error_id          BIGINT      GENERATED ALWAYS AS IDENTITY,
        object_name       STRING      NOT NULL,
        sf_id             VARCHAR(18),
        fk_column         STRING,
        unresolved_sf_id  VARCHAR(18),
        error_type        STRING      NOT NULL,  -- ORPHAN_FK, DUPLICATE, etc.
        logged_at         TIMESTAMP   NOT NULL DEFAULT current_timestamp()
    )
    USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver._schema_table (
        object_name   STRING  NOT NULL,
        column_name   STRING  NOT NULL,
        data_type     STRING,
        is_pk         BOOLEAN DEFAULT false,
        is_fk         BOOLEAN DEFAULT false,
        fk_references STRING,              -- e.g. 'silver.account.silver_id'
        notes         STRING
    )
    USING DELTA
""")

print("✅ Metadata tables ready")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Silver Object Table DDL — Run Once Per Object

# COMMAND ----------

# ── Build FK column definitions ─────────────────────────────────────────────
fk_col_defs   = "\n    ".join([f"{parent}_silver_id  BIGINT," for _, parent in FK_RELATIONS])
fk_constraint = "\n    ".join([
    f"CONSTRAINT fk_{OBJECT_NAME}_{parent} FOREIGN KEY ({parent}_silver_id) REFERENCES silver.{parent}(silver_id)"
    for _, parent in FK_RELATIONS
])

# ── Build business column definitions ───────────────────────────────────────
business_col_defs = "\n    ".join([f"{col}  STRING," for col in BUSINESS_COLS])

# ── DDL ─────────────────────────────────────────────────────────────────────
ddl = f"""
    CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
        silver_id    BIGINT       NOT NULL,
        sf_id        VARCHAR(18)  NOT NULL,

        -- FK columns (surrogate)
        {fk_col_defs if fk_col_defs else "-- no FKs"}

        -- Business columns
        {business_col_defs}

        -- Metadata
        _created_at  TIMESTAMP    NOT NULL,
        _updated_at  TIMESTAMP    NOT NULL,
        _is_deleted  BOOLEAN      NOT NULL DEFAULT false,

        CONSTRAINT pk_{OBJECT_NAME} PRIMARY KEY (silver_id),
        CONSTRAINT uq_{OBJECT_NAME}_sf_id UNIQUE (sf_id)
        {", " + fk_constraint if fk_constraint else ""}
    )
    USING DELTA
"""

spark.sql(ddl)
print(f"✅ silver.{OBJECT_NAME} table ready")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Pipeline Functions

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

def get_watermark(object_name: str) -> str:
    """Get last watermark for object. Returns epoch start if first run."""
    result = spark.sql(f"""
        SELECT last_watermark FROM silver._watermark_log
        WHERE object_name = '{object_name}'
    """).collect()
    return result[0][0] if result else "1970-01-01T00:00:00"


def set_watermark(object_name: str, watermark: str):
    """Upsert watermark for object."""
    spark.sql(f"""
        MERGE INTO silver._watermark_log AS target
        USING (
            SELECT '{object_name}' AS object_name,
            CAST('{watermark}' AS TIMESTAMP) AS last_watermark
        ) AS source
        ON target.object_name = source.object_name
        WHEN MATCHED THEN UPDATE SET
            target.last_watermark = source.last_watermark,
            target.updated_at     = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (object_name, last_watermark)
        VALUES (source.object_name, source.last_watermark)
    """)


def log_run(object_name: str, status: str, rows: int = 0, started_at: str = None):
    """Insert a run log entry."""
    spark.sql(f"""
        INSERT INTO silver._run_log (object_name, status, rows_processed, started_at, completed_at)
        VALUES (
            '{object_name}',
            '{status}',
            {rows},
            {'CAST("' + started_at + '" AS TIMESTAMP)' if started_at else 'current_timestamp()'},
            {'current_timestamp()' if status != 'STARTED' else 'NULL'}
        )
    """)


def register_id_map(object_name: str, bronze_table: str):
    """Register new SF IDs into _id_map. Skips existing."""
    spark.sql(f"""
        INSERT INTO silver._id_map (object_name, sf_id)
        SELECT DISTINCT '{object_name}', sf_id
        FROM {bronze_table}
        WHERE sf_id NOT IN (
            SELECT sf_id FROM silver._id_map
            WHERE object_name = '{object_name}'
        )
    """)
    print(f"✅ _id_map updated for {object_name}")


def log_orphan_fks(object_name: str, bronze_table: str, fk_relations: list):
    """Log any unresolved FK references to _error_log."""
    for fk_col, parent_object in fk_relations:
        spark.sql(f"""
            INSERT INTO silver._error_log (object_name, sf_id, fk_column, unresolved_sf_id, error_type)
            SELECT
                '{object_name}',
                b.sf_id,
                '{parent_object}_silver_id',
                b.{fk_col},
                'ORPHAN_FK'
            FROM {bronze_table} b
            LEFT JOIN silver._id_map p
                ON p.sf_id        = b.{fk_col}
                AND p.object_name = '{parent_object}'
            WHERE p.sf_id IS NULL
              AND b.{fk_col} IS NOT NULL
        """)
    print(f"✅ Orphan FK check complete for {object_name}")


def build_source_query(
    object_name: str,
    bronze_table: str,
    fk_relations: list,
    business_cols: list,
    watermark_col: str,
    watermark: str
) -> str:
    """Build deduplicated + FK-resolved source query for MERGE."""

    fk_joins = "\n        ".join([
        f"LEFT JOIN silver._id_map {parent}_map "
        f"ON {parent}_map.sf_id = b.{fk_col} "
        f"AND {parent}_map.object_name = '{parent}'"
        for fk_col, parent in fk_relations
    ])

    fk_selects = "\n            ".join([
        f"{parent}_map.silver_id AS {parent}_silver_id,"
        for _, parent in fk_relations
    ])

    business_selects = "\n            ".join([f"b.{col}," for col in business_cols])

    return f"""
        SELECT * FROM (
            SELECT
                m.silver_id,
                b.sf_id,
                {fk_selects}
                {business_selects}
                b.is_deleted AS _is_deleted,
                b.{watermark_col},
                ROW_NUMBER() OVER (
                    PARTITION BY b.sf_id
                    ORDER BY b.{watermark_col} DESC
                ) AS rn
            FROM {bronze_table} b
            JOIN silver._id_map m
                ON m.sf_id        = b.sf_id
                AND m.object_name = '{object_name}'
            {fk_joins}
            WHERE b.{watermark_col} > CAST('{watermark}' AS TIMESTAMP)
        ) WHERE rn = 1
    """


def run_merge(
    object_name: str,
    silver_table: str,
    source_query: str,
    fk_relations: list,
    business_cols: list
) -> int:
    """Execute MERGE into silver table. Returns row count."""

    fk_updates = "\n        ".join([
        f"target.{parent}_silver_id = source.{parent}_silver_id,"
        for _, parent in fk_relations
    ])
    business_updates = "\n        ".join([
        f"target.{col} = source.{col},"
        for col in business_cols
    ])
    fk_insert_cols = (", ".join([f"{parent}_silver_id" for _, parent in fk_relations]) + ",") if fk_relations else ""
    fk_insert_vals = (", ".join([f"source.{parent}_silver_id" for _, parent in fk_relations]) + ",") if fk_relations else ""
    biz_insert_cols = ", ".join(business_cols)
    biz_insert_vals = ", ".join([f"source.{col}" for col in business_cols])

    merge_sql = f"""
        MERGE INTO {silver_table} AS target
        USING ({source_query}) AS source
        ON target.sf_id = source.sf_id
        WHEN MATCHED THEN UPDATE SET
            {fk_updates}
            {business_updates}
            target._updated_at = current_timestamp(),
            target._is_deleted = source._is_deleted
        WHEN NOT MATCHED THEN INSERT (
            silver_id, sf_id,
            {fk_insert_cols}
            {biz_insert_cols},
            _created_at, _updated_at, _is_deleted
        ) VALUES (
            source.silver_id, source.sf_id,
            {fk_insert_vals}
            {biz_insert_vals},
            current_timestamp(), current_timestamp(), source._is_deleted
        )
    """
    spark.sql(merge_sql)
    rows = spark.sql(f"SELECT COUNT(*) FROM {silver_table}").collect()[0][0]
    return rows

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Run Pipeline — Same for all 16 objects

# COMMAND ----------

started_at = datetime.utcnow().isoformat()

try:
    log_run(OBJECT_NAME, "STARTED", started_at=started_at)

    # ── Get watermark ────────────────────────────────────────────────────────
    watermark = get_watermark(OBJECT_NAME)
    print(f"⏱ Watermark: {watermark}")

    # ── Step 1: Register new SF IDs ──────────────────────────────────────────
    register_id_map(OBJECT_NAME, BRONZE_TABLE)

    # ── Step 2: Build source (dedup + FK resolve) ────────────────────────────
    source_query = build_source_query(
        OBJECT_NAME, BRONZE_TABLE, FK_RELATIONS,
        BUSINESS_COLS, WATERMARK_COL, watermark
    )

    # ── Step 3: Merge into silver ────────────────────────────────────────────
    rows = run_merge(OBJECT_NAME, SILVER_TABLE, source_query, FK_RELATIONS, BUSINESS_COLS)
    print(f"✅ Merge complete — {rows} rows in {SILVER_TABLE}")

    # ── Step 4: Log orphaned FKs ─────────────────────────────────────────────
    log_orphan_fks(OBJECT_NAME, BRONZE_TABLE, FK_RELATIONS)

    # ── Step 5: Update watermark ─────────────────────────────────────────────
    new_watermark = spark.sql(
        f"SELECT MAX({WATERMARK_COL}) FROM {BRONZE_TABLE}"
    ).collect()[0][0]
    set_watermark(OBJECT_NAME, new_watermark)
    print(f"⏱ Watermark updated to: {new_watermark}")

    log_run(OBJECT_NAME, "SUCCESS", rows=rows, started_at=started_at)
    print(f"🎉 Pipeline complete for {OBJECT_NAME}")

except Exception as e:
    log_run(OBJECT_NAME, "FAILED", started_at=started_at)
    print(f"❌ Pipeline failed for {OBJECT_NAME}: {str(e)}")
    raise

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Backfill Orphaned FKs — Run after parent table loads

# COMMAND ----------

# Re-resolve NULL FK columns after parent table catches up
for fk_col, parent_object in FK_RELATIONS:
    spark.sql(f"""
        MERGE INTO {SILVER_TABLE} AS target
        USING (
            SELECT
                c.sf_id,
                p.silver_id AS {parent_object}_silver_id
            FROM {SILVER_TABLE} c
            JOIN silver._id_map p
                ON p.sf_id        = c.sf_id
                AND p.object_name = '{parent_object}'
            WHERE c.{parent_object}_silver_id IS NULL
        ) AS source
        ON target.sf_id = source.sf_id
        WHEN MATCHED THEN UPDATE SET
            target.{parent_object}_silver_id = source.{parent_object}_silver_id,
            target._updated_at               = current_timestamp()
    """)
    print(f"✅ Backfill complete for {OBJECT_NAME}.{parent_object}_silver_id")
