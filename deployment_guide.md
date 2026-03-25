# Salesforce → Databricks Pipeline — Deployment Guide

## Overview

This guide covers everything needed to deploy the Salesforce Bronze Copy
pipeline to production. It is split by team so each group can work through
their section independently.

The pipeline consists of two scheduled scripts:

| Script | What it does | Who deploys it |
|---|---|---|
| `sf_catalog.py` | Discovers all Salesforce objects and fields, saves schema to Delta | Data Engineering |
| `sf_load.py` | Incremental CDC load using schema from catalog | Data Engineering |

**Total setup time: approximately 2–3 hours across all teams.**

---

## Table of Contents

1. Salesforce Admin Setup
2. Databricks Admin Setup
3. Data Engineering Deployment
4. Validation and Sign-off
5. Ongoing Operations

---

---

# Section 1 — Salesforce Admin

**Who:** Salesforce System Administrator
**Time:** 30 minutes
**Dependency:** Must be completed before any Databricks work begins

---

### 1.1 — Create a Dedicated Ingestion User

Create a service account specifically for Databricks. Never use a personal user
account — if that person leaves, the pipeline breaks.

```
Salesforce Setup → Users → New User

  First Name    : Databricks
  Last Name     : Ingest
  Email         : databricks-ingest@yourcompany.com
  Username      : databricks-ingest@yourcompany.com
  Role          : None
  Profile       : System Administrator
                  (or a custom profile — see 1.2 below)
  Active        : ✅ Yes
```

Set a strong password and store it in your company's secrets vault. The
Databricks team will need:
- Username
- Password
- Security token (generated in step 1.3)

---

### 1.2 — Minimum Required Permissions (Custom Profile)

If you prefer not to use System Administrator, create a custom profile with
these permissions at minimum:

```
System Permissions:
  ✅ API Enabled
  ✅ View All Data          ← required to read all objects
  ✅ Query All Files        ← required for file-related objects

Object Permissions (for every object to be ingested):
  ✅ Read
  ✅ View All Records
```

---

### 1.3 — Generate a Security Token

The security token is appended to the password for API authentication.

```
Log in as the ingestion user →
Settings (top right) →
My Personal Information →
Reset My Security Token →
Click "Reset Security Token"
```

The token will be emailed to the ingestion user's email address. Save it
alongside the password — both are needed.

---

### 1.4 — Confirm API Access

Verify API access is working from a browser or Postman:

```
POST https://login.salesforce.com/services/oauth2/token
  grant_type    = password
  client_id     = <your connected app or use simple_salesforce defaults>
  username      = databricks-ingest@yourcompany.com
  password      = <password><security_token>
```

A successful response returns an `access_token`. If this fails, check the
profile's API Enabled permission.

---

### Salesforce Admin Handoff Checklist

```
[ ] Ingestion user created: databricks-ingest@yourcompany.com
[ ] Profile has API Enabled + View All Data
[ ] Security token generated and saved
[ ] Credentials (username, password, token) shared securely with Databricks Admin
[ ] API access confirmed via test login
```

---

---

# Section 2 — Databricks Admin

**Who:** Databricks Workspace Administrator
**Time:** 30–45 minutes
**Dependency:** Requires credentials from Salesforce Admin (Section 1)

---

### 2.1 — Create the Target Database

Open a Databricks notebook or SQL editor and run:

```sql
CREATE DATABASE IF NOT EXISTS salesforce_bronze
COMMENT 'Raw Salesforce data — Bronze layer. Do not modify directly.';
```

---

### 2.2 — Create a Databricks Secret Scope

Secrets are used to store Salesforce credentials securely. Scripts read from
secrets at runtime — credentials are never hardcoded.

**Install the Databricks CLI if not already installed:**
```bash
pip install databricks-cli
databricks configure --token
# Enter your workspace URL and personal access token when prompted
```

**Create the secret scope:**
```bash
databricks secrets create-scope --scope salesforce
```

**Store the Salesforce credentials:**
```bash
databricks secrets put --scope salesforce --key username
# Paste: databricks-ingest@yourcompany.com

databricks secrets put --scope salesforce --key password
# Paste: <password>

databricks secrets put --scope salesforce --key security_token
# Paste: <security token from step 1.3>
```

**Verify secrets are stored (values are never shown, only keys):**
```bash
databricks secrets list --scope salesforce
```

Expected output:
```
Key Name          Last Updated At
username          2024-01-15 09:00:00
password          2024-01-15 09:00:00
security_token    2024-01-15 09:00:00
```

---

### 2.3 — Create a Cluster for the Pipeline

The pipeline runs on a standard job cluster. Recommended config:

```
Cluster name     : salesforce-pipeline-cluster
Cluster mode     : Job (single node is fine for most orgs)
Databricks Runtime: 13.3 LTS or later (includes Delta Lake)
Node type        : i3.xlarge (or equivalent — 4 cores, 30GB RAM)
Auto-terminate   : 30 minutes

Libraries to install:
  Source : PyPI
  Package: simple-salesforce

  Source : Maven
  Package: com.springml:spark-salesforce_2.12:1.1.3
```

---

### 2.4 — Upload the Pipeline Scripts

Upload both scripts to Databricks Workspace or a connected Git repo:

**Option A — Workspace Upload (quickest)**
```
Workspace → (right click your folder) → Import →
Upload sf_catalog.py
Upload sf_load.py
```

**Option B — Git Integration (recommended for production)**
```
Workspace → Repos → Add Repo → connect your Git repo
Push sf_catalog.py and sf_load.py to the repo
```

---

### 2.5 — Grant Permissions on the Database

Grant the engineering team access to the Bronze database:

```sql
-- Allow data engineers to read and write
GRANT ALL PRIVILEGES ON DATABASE salesforce_bronze TO `data-engineering@yourcompany.com`;

-- Allow analysts to read only
GRANT SELECT ON DATABASE salesforce_bronze TO `analysts@yourcompany.com`;
```

---

### Databricks Admin Handoff Checklist

```
[ ] salesforce_bronze database created
[ ] Secret scope created: salesforce
[ ] Three secrets stored: username, password, security_token
[ ] Secrets verified via CLI list command
[ ] Cluster created with simple-salesforce and spark-salesforce libraries
[ ] Scripts uploaded to Workspace or Git repo
[ ] Permissions granted to engineering and analyst teams
```

---

---

# Section 3 — Data Engineering

**Who:** Data Engineer
**Time:** 45–60 minutes
**Dependency:** Sections 1 and 2 must be complete

---

### 3.1 — Verify Secrets Are Accessible

Open a Databricks notebook on the pipeline cluster and test:

```python
# This should print the username without exposing the password
username = dbutils.secrets.get("salesforce", "username")
print(f"Connected as: {username}")
# → Connected as: databricks-ingest@yourcompany.com
```

If this fails, ask the Databricks Admin to check the secret scope permissions.

---

### 3.2 — Test the Salesforce Connection

Run this in a notebook to confirm the Salesforce API is reachable:

```python
from simple_salesforce import Salesforce

sf = Salesforce(
    username       = dbutils.secrets.get("salesforce", "username"),
    password       = dbutils.secrets.get("salesforce", "password"),
    security_token = dbutils.secrets.get("salesforce", "security_token")
)

# Describe a single object to confirm connection
result = sf.Account.describe()
print(f"✅ Connected. Account has {len(result['fields'])} fields.")
# → ✅ Connected. Account has 98 fields.
```

---

### 3.3 — Run sf_catalog.py (First Time)

This must be run once before sf_load.py — it builds the schema catalog that
the load script depends on.

**Run manually first to verify output:**
```python
# In a notebook, run the full sf_catalog.py script
# Expected output:
#
# Found 215 queryable objects in org
#    ✅  Account                    96/98 fields
#    ✅  Opportunity                117/120 fields
#    ✅  Contact                    85/87 fields
#    ...
# ℹ️   No existing catalog found — this is the first run.
# ═══════════════════════════════════════════════
#   ✅ Catalog updated → salesforce_bronze._catalog
#      215 objects  |  12,450 total fields
# ═══════════════════════════════════════════════
```

**Verify the catalog table was created:**
```sql
SELECT sf_object, COUNT(*) as field_count
FROM salesforce_bronze._catalog
WHERE is_queryable = true
GROUP BY sf_object
ORDER BY field_count DESC
LIMIT 10;
```

---

### 3.4 — Run sf_load.py (First Time)

This is the initial full load. It will take longer than subsequent runs as
every object is loaded from scratch.

**Run manually first:**
```python
# In a notebook, run the full sf_load.py script
# Expected output:
#
# ════════════════════════════════════════════════
#   Salesforce Load  —  2024-01-15 10:00 UTC
# ════════════════════════════════════════════════
#
#   Loading 215 objects
#    ✅  Account                    12,450 rows
#    ✅  Opportunity                89,200 rows
#    ✅  Contact                    34,100 rows
#    ⏭️   RecordType                 no new records
#    ...
#
# ════════════════════════════════════════════════
#   ✅  Loaded  : 198 objects  (1,234,567 rows)
#   ⏭️   Skipped : 12 objects  (no new records)
#   ❌  Failed  : 5 objects
# ════════════════════════════════════════════════
```

Review any failed objects — see Section 4 for debugging steps.

---

### 3.5 — Create the Databricks Jobs

Set up both scripts as scheduled Databricks Jobs.

**Job 1 — Monthly Catalog Job**

```
Databricks → Workflows → Create Job

  Name          : SF Catalog — Monthly Schema Discovery
  Task type     : Notebook (or Python script)
  Source        : Workspace path or Git repo
  Script        : /path/to/sf_catalog.py
  Cluster       : salesforce-pipeline-cluster

  Schedule:
    Type        : Scheduled
    Cron        : 0 2 1 * *     ← 2:00 AM on the 1st of every month
    Timezone    : UTC

  Notifications:
    On failure  : data-engineering@yourcompany.com
    On success  : data-engineering@yourcompany.com
```

**Job 2 — Hourly Load Job**

```
Databricks → Workflows → Create Job

  Name          : SF Load — Hourly CDC
  Task type     : Notebook (or Python script)
  Source        : Workspace path or Git repo
  Script        : /path/to/sf_load.py
  Cluster       : salesforce-pipeline-cluster

  Schedule:
    Type        : Scheduled
    Cron        : 0 * * * *     ← top of every hour
    Timezone    : UTC

  Notifications:
    On failure  : data-engineering@yourcompany.com

  Max concurrent runs: 1        ← prevent overlapping runs
```

---

### 3.6 — Set Up Job Dependencies

The load job should not run if the catalog does not exist. This is already
handled in `sf_load.py` with:

```python
if not spark.catalog.tableExists(f"{TARGET_DB}._catalog"):
    raise RuntimeError(
        "Catalog not found. Run sf_catalog.py first."
    )
```

Optionally, add the catalog job as an upstream dependency of the load job in
the first month's run via Databricks Workflows.

---

### Data Engineering Handoff Checklist

```
[ ] Secret access verified from notebook
[ ] Salesforce API connection tested successfully
[ ] sf_catalog.py run manually — catalog table created
[ ] sf_load.py run manually — initial full load completed
[ ] Failed objects reviewed and documented
[ ] Databricks Job created for sf_catalog.py — monthly schedule
[ ] Databricks Job created for sf_load.py — hourly schedule
[ ] Both jobs triggered manually and verified successful
[ ] Max concurrent runs set to 1 on the load job
```

---

---

# Section 4 — Validation and Sign-off

**Who:** Data Engineering + Data/Analytics team
**Time:** 30 minutes

---

### 4.1 — Verify Tables Were Created

```sql
SHOW TABLES IN salesforce_bronze;
```

Expected output includes:
```
_catalog
_catalog_audit
_watermark
account
opportunity
contact
lead
case
task
...
```

---

### 4.2 — Spot-Check Row Counts Against Salesforce

Pick 3–5 key objects and compare row counts between Salesforce and Databricks:

```sql
SELECT
  'account'     AS object, COUNT(*) AS rows FROM salesforce_bronze.account     UNION ALL
SELECT
  'opportunity' AS object, COUNT(*) AS rows FROM salesforce_bronze.opportunity  UNION ALL
SELECT
  'contact'     AS object, COUNT(*) AS rows FROM salesforce_bronze.contact      UNION ALL
SELECT
  'lead'        AS object, COUNT(*) AS rows FROM salesforce_bronze.lead
ORDER BY rows DESC;
```

Cross-check these numbers against Salesforce reports for the same objects.
Counts will not be exact (deleted records, timing) but should be very close.

---

### 4.3 — Verify Incremental Load is Working

1. Create or update a record in Salesforce (e.g. rename an Account)
2. Wait for the next hourly run (or trigger the job manually)
3. Verify the change appears in Databricks:

```sql
SELECT Id, Name, LastModifiedDate, _ingested_at
FROM salesforce_bronze.account
WHERE LastModifiedDate > current_timestamp() - INTERVAL 2 HOURS
ORDER BY LastModifiedDate DESC
LIMIT 10;
```

---

### 4.4 — Verify the Watermark is Updating

```sql
SELECT
  sf_object,
  last_loaded_at,
  timestampdiff(MINUTE, last_loaded_at, current_timestamp()) AS minutes_since_load
FROM salesforce_bronze._watermark
ORDER BY last_loaded_at DESC;
```

All objects should show a `last_loaded_at` within the last hour.

---

### 4.5 — Check for Failed Objects

```sql
-- Objects in catalog that have no watermark (never successfully loaded)
SELECT c.sf_object
FROM (
  SELECT DISTINCT sf_object FROM salesforce_bronze._catalog WHERE is_queryable = true
) c
LEFT JOIN salesforce_bronze._watermark w ON c.sf_object = w.sf_object
WHERE w.sf_object IS NULL
ORDER BY c.sf_object;
```

Review each failed object. Common causes:

| Error | Cause | Fix |
|---|---|---|
| `pkChunking not supported` | Object too small or doesn't support chunking | Set `pkChunking=false` for that object |
| `SOQL query too long` | Object has 500+ fields | Split the query into batches |
| `No such column: LastModifiedDate` | Object has no timestamp field | Full load only — no incremental possible |
| `Insufficient privileges` | SF user lacks access to this object | Ask SF Admin to grant Read access |

---

### Sign-off Checklist

```
[ ] All major objects present in salesforce_bronze (account, opportunity, contact, lead)
[ ] Row counts match Salesforce within acceptable tolerance (< 1% difference)
[ ] Incremental load verified — changed record appeared in next run
[ ] Watermark updating correctly for all objects
[ ] Failed objects documented and actioned
[ ] Monthly catalog job confirmed scheduled for 1st of each month
[ ] Hourly load job confirmed running on schedule
[ ] On-failure alerts confirmed working (send a test failure)
[ ] Sign-off from Data Engineering Lead
[ ] Sign-off from Analytics / Data Consumer team
```

---

---

# Section 5 — Ongoing Operations

---

### Monthly — Catalog Refresh

The catalog job runs automatically on the 1st of every month. After it runs:

```sql
-- Check what changed this month
SELECT
  sf_object,
  field_name,
  change_type,
  detected_at
FROM salesforce_bronze._catalog_audit
WHERE detected_at > current_timestamp() - INTERVAL 31 DAYS
ORDER BY detected_at DESC;
```

New fields will automatically appear in Delta tables on the next load run.
No manual action needed unless a field type changed (see below).

---

### If a Salesforce Field Type Changes

Delta will reject writes if a field changes type (e.g. string → integer).
Handle it manually:

```python
# Cast the new incoming type to match the existing Delta column
from pyspark.sql.functions import col

df = df.withColumn("ProblemField", col("ProblemField").cast("string"))
```

Then re-run the load for that object.

---

### If a New Salesforce Object is Added

1. It will be picked up automatically on the next monthly catalog run
2. The load script will detect it has no watermark and do a full load
3. No action needed — fully automatic

---

### Triggering a Full Refresh for One Object

If a table gets out of sync (e.g. hard deletes were missed), force a full
reload by deleting its watermark:

```sql
-- Remove watermark for one object — next run will do a full load
DELETE FROM salesforce_bronze._watermark
WHERE sf_object = 'Opportunity';
```

Then trigger the load job manually.

---

### Monitoring Dashboard Query

Run this weekly to get a health overview:

```sql
SELECT
  w.sf_object,
  w.last_loaded_at,
  timestampdiff(HOUR, w.last_loaded_at, current_timestamp())  AS hours_since_load,
  c.field_count,
  CASE
    WHEN timestampdiff(HOUR, w.last_loaded_at, current_timestamp()) > 2
    THEN '⚠️  STALE'
    ELSE '✅  OK'
  END AS status
FROM salesforce_bronze._watermark w
JOIN (
  SELECT sf_object, COUNT(*) AS field_count
  FROM salesforce_bronze._catalog
  WHERE is_queryable = true
  GROUP BY sf_object
) c ON w.sf_object = c.sf_object
ORDER BY hours_since_load DESC;
```

---

### Contacts and Escalation

| Issue | Contact |
|---|---|
| Salesforce API credentials expired or locked | Salesforce Admin |
| Databricks cluster down or secrets inaccessible | Databricks Admin |
| Pipeline logic bugs or schema issues | Data Engineering |
| Data quality questions | Data Engineering + Analytics |
