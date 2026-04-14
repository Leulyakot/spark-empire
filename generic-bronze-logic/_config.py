# Databricks notebook source
# MAGIC %md
# MAGIC # _config · Environment & Catalog Configuration
# MAGIC
# MAGIC `%run`-ed at the top of every pipeline notebook.
# MAGIC Sets all catalog, schema, and pipeline constants for the current environment.
# MAGIC
# MAGIC **Resolution order for ENV:**
# MAGIC 1. Databricks job widget  (`ENV` parameter on the job run)
# MAGIC 2. Cluster / notebook environment variable  (`os.environ["ENV"]`)
# MAGIC 3. Default → `"dev"`
# MAGIC
# MAGIC **Valid values:** `dev` | `uat` | `prod`

# COMMAND ----------

import os

# ── Resolve environment ───────────────────────────────────────
try:
    ENV = dbutils.widgets.get("ENV").strip().lower()
except Exception:
    ENV = os.environ.get("ENV", "dev").strip().lower()

_VALID_ENVS = {"dev", "uat", "prod"}
if ENV not in _VALID_ENVS:
    raise ValueError(f"ENV='{ENV}' is not valid. Must be one of: {_VALID_ENVS}")

print(f"Environment : {ENV.upper()}")

# COMMAND ----------

# ── Catalog map ───────────────────────────────────────────────
# Each environment owns its own Unity Catalog.
# Adjust names to match your organisation's naming convention.
_CATALOG_MAP = {
    "dev" : "salesforce_dev",
    "uat" : "salesforce_uat",
    "prod": "salesforce_prod",
}

CATALOG = _CATALOG_MAP[ENV]

# ── Schema names (same across all envs) ──────────────────────
BRONZE_SCHEMA  = "bronze"
SILVER_SCHEMA  = "silver"
LOGGING_SCHEMA = "pipeline_control"

# ── Pipeline constants ────────────────────────────────────────
PIPELINE_NAME         = "bronze_to_silver_salesforce"
WATERMARK_LAG_MINUTES = 5
SKIP_EMPTY_WINDOWS    = True

# ── Fully-qualified name helpers ─────────────────────────────
def bronze(t): return f"{CATALOG}.{BRONZE_SCHEMA}.{t}"
def silver(t): return f"{CATALOG}.{SILVER_SCHEMA}.{t}"
def log(t):    return f"{CATALOG}.{LOGGING_SCHEMA}.{t}"

REGISTRY_FQ = log("_schema_registry")

# ── Summary ───────────────────────────────────────────────────
print(f"Catalog     : {CATALOG}")
print(f"Bronze      : {CATALOG}.{BRONZE_SCHEMA}")
print(f"Silver      : {CATALOG}.{SILVER_SCHEMA}")
print(f"Control     : {CATALOG}.{LOGGING_SCHEMA}")
print(f"Registry    : {REGISTRY_FQ}")
