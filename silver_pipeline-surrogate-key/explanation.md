# Silver Pipeline — Explanation & Design Decisions

## Overview

This pipeline moves data from a **bronze (append-only)** layer to a **silver (conformed)** layer for 16 Salesforce objects on Databricks Delta. It is generic and repeatable — the same notebook runs for all 16 objects by changing only the configuration section.

---

## Architecture

### Medallion Layers

| Layer  | Description |
|--------|-------------|
| Bronze | Append-only incremental dump from Salesforce. Raw, no deduplication. Multiple versions of the same record over time. |
| Silver | Deduplicated, FK-resolved, surrogate-keyed Delta tables. One row per Salesforce record (latest version). |

---

## Key Design Decisions

### 1. Surrogate Keys via `_id_map`

Salesforce uses 18-character string IDs. Silver replaces these with `BIGINT` surrogate keys for performance and platform independence.

**Why `_id_map` over hashing (`xxhash64`)?**

- Zero collision risk — hashing has near-zero but non-zero collision probability
- Stable across full reloads — if silver is truncated and reloaded, `silver_id` values never change
- Full auditability — any `silver_id` can be traced back to its `sf_id` instantly
- Downstream gold tables are insulated — if hashing strategy ever changes, gold is unaffected

**Pattern:**

| Column | Type | Purpose |
|--------|------|---------|
| `silver_id` | BIGINT | Surrogate PK, used for all joins in silver/gold |
| `sf_id` | VARCHAR(18) | Source of truth for MERGE operations |
| `<parent>_silver_id` | BIGINT | FK columns pointing to parent `silver_id` |

---

### 2. Deduplication Strategy

Bronze is append-only so the same Salesforce record can appear multiple times. Silver keeps only the latest version using a window function:

```sql
ROW_NUMBER() OVER (
    PARTITION BY sf_id
    ORDER BY last_modified_date DESC
) AS rn
WHERE rn = 1
```

---

### 3. Incremental Loads via Watermark

Each run only processes records newer than the last known watermark, stored in `_watermark_log`. On first run the watermark defaults to `1970-01-01` to process all records.

---

### 4. FK Resolution

Bronze stores raw Salesforce IDs in lookup columns (e.g. `account_id = '0016g000002XxYZAA0'`). Silver resolves these to `silver_id` BigInts via a JOIN to `_id_map` at load time.

**Orphan FKs** (child arrives before parent due to incremental timing) are:
1. Loaded with `NULL` FK column rather than rejected
2. Logged to `_error_log` with `error_type = 'ORPHAN_FK'`
3. Resolved in a backfill step after the parent table loads

---

### 5. Constraints in Databricks

Databricks Delta does **not enforce** UNIQUE or FOREIGN KEY constraints — they are informational only (used by Unity Catalog for lineage). Integrity is enforced in pipeline logic instead:

- **Dedup** prevents duplicate `sf_id` rows
- **Pre-merge assertions** can catch duplicates before they reach silver
- **`_error_log`** captures orphaned FKs

---

## Metadata Tables

| Table | Purpose |
|-------|---------|
| `silver._id_map` | Maps `sf_id → silver_id` for all 16 objects. Persists across reloads. |
| `silver._watermark_log` | Tracks last processed `last_modified_date` per object for incremental pulls. |
| `silver._run_log` | Records every pipeline run: object, status, rows processed, start/end time. |
| `silver._error_log` | Captures orphaned FKs, duplicates, and other data quality issues. |
| `silver._schema_table` | Catalogs silver table columns, data types, PK/FK relationships. |

---

## Silver Table Structure

Every silver table follows the same pattern:

```sql
CREATE TABLE silver.<object> (
    silver_id          BIGINT       NOT NULL,   -- surrogate PK from _id_map
    sf_id              VARCHAR(18)  NOT NULL,   -- merge key, source of truth
    <parent>_silver_id BIGINT,                  -- resolved FK (one per lookup)
    <business_cols>    STRING/TIMESTAMP/...,    -- object-specific fields
    _created_at        TIMESTAMP    NOT NULL,   -- first seen in silver
    _updated_at        TIMESTAMP    NOT NULL,   -- last updated in silver
    _is_deleted        BOOLEAN      NOT NULL    -- Salesforce soft-delete flag
)
USING DELTA
```

---

## Pipeline Execution Order (Dependency Tiers)

Parents must load before children to minimize orphan FKs.

```
Tier 1 — No dependencies (run in parallel):
    account, user, product2, pricebook2

Tier 2 — Depend on Tier 1:
    contact          → account
    opportunity      → account
    pricebook_entry  → pricebook2, product2

Tier 3 — Depend on Tier 2:
    opportunity_line_item  → opportunity, pricebook_entry
    case                   → account, contact
    task                   → opportunity, account
```

---

## Scheduling

The notebook is parameterized via `dbutils.widgets` so a single notebook can be scheduled across all 16 objects in **Databricks Workflows**:

```
Job: silver_pipeline  (e.g. every 6 hours)
├── Tier 1 tasks run in parallel
├── Tier 2 tasks run after Tier 1 completes
└── Tier 3 tasks run after Tier 2 completes
```

Each task passes `object_name` as a job parameter:

```json
{ "object_name": "contact" }
```

---

## How to Use This Notebook

1. **Run Section 1 once** to create all metadata tables
2. **For each of the 16 objects:**
   - Edit Section 0: set `OBJECT_NAME`, `FK_RELATIONS`, `BUSINESS_COLS`
   - Run Section 2 once to create the silver table DDL
3. **For scheduled runs:** Sections 3 and 4 run every increment
4. **After parent table loads:** Run Section 5 to backfill orphaned FKs

---

## Known Limitations & Mitigations

| Issue | Mitigation |
|-------|-----------|
| UNIQUE/FK not enforced in Databricks | Pre-merge dedup assertion + `_error_log` |
| Child may arrive before parent | Orphan logged + backfill job after parent loads |
| `GENERATED ALWAYS AS IDENTITY` unreliable in concurrent MERGE | `_id_map` populated separately before MERGE |
| Polymorphic fields (`WhatId`, `WhoId`) | Add `<col>_object_type STRING` alongside the FK column |
