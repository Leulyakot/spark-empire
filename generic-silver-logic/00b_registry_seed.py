# Databricks notebook source
# MAGIC %md
# MAGIC # 00b · Schema Registry Seed
# MAGIC
# MAGIC Populates `_schema_registry` and seeds a zero-epoch watermark row in `_watermark`
# MAGIC for every Salesforce Bronze table.
# MAGIC
# MAGIC **Re-runnable.** Both operations MERGE on natural keys — nothing is duplicated
# MAGIC and existing watermark positions are never reset.
# MAGIC
# MAGIC To add a new object: add an entry to `REGISTRY` and re-run.

# COMMAND ----------
# MAGIC %run ./_config

# COMMAND ----------
from delta.tables import DeltaTable

# ── Type shorthands ───────────────────────────────────────────
S  = "STRING"
B  = "BOOLEAN"
L  = "BIGINT"
D  = "DECIMAL(18,4)"
DT = "DATE"
TS = "TIMESTAMP"

# ── Audit columns present on every Salesforce object ─────────
AUDIT = [
    ("Id",               "sf_id",               S,  False, "Salesforce 18-char record ID (PK)"),
    ("CreatedDate",      "created_date",         TS, False, "Record creation timestamp (UTC)"),
    ("LastModifiedDate", "last_modified_date",   TS, True,  "Last user-visible modification (UTC)"),
    ("SystemModstamp",   "system_modstamp",      TS, False, "System-level last-change stamp; watermark column"),
    ("IsDeleted",        "is_deleted",           B,  False, "Salesforce soft-delete flag"),
    ("CreatedById",      "created_by_id",        S,  True,  "FK → User.Id"),
    ("LastModifiedById", "last_modified_by_id",  S,  True,  "FK → User.Id"),
]

# ── Registry ──────────────────────────────────────────────────
# (bronze_table, silver_table, pk, watermark_col, surrogate_key_col, [columns])
# columns: (bronze_col, silver_col, data_type, nullable, comment)

REGISTRY = {
    "account": ("account", "account", "Id", "SystemModstamp", "account_sk", [
        ("Name",              "name",                S, False, "Account / company name"),
        ("Type",              "type",                S, True,  "Prospect | Customer | Partner"),
        ("Industry",          "industry",            S, True,  "Industry picklist"),
        ("AnnualRevenue",     "annual_revenue",      D, True,  "Self-reported annual revenue"),
        ("NumberOfEmployees", "number_of_employees", L, True,  "Self-reported headcount"),
        ("Rating",            "rating",              S, True,  "Hot | Warm | Cold"),
        ("BillingStreet",     "billing_street",      S, True,  ""),
        ("BillingCity",       "billing_city",        S, True,  ""),
        ("BillingState",      "billing_state",       S, True,  ""),
        ("BillingPostalCode", "billing_postal_code", S, True,  ""),
        ("BillingCountry",    "billing_country",     S, True,  ""),
        ("Phone",             "phone",               S, True,  ""),
        ("Website",           "website",             S, True,  ""),
        ("OwnerId",           "owner_id",            S, True,  "FK → User.Id"),
        ("ParentId",          "parent_id",           S, True,  "FK → Account.Id"),
        ("AccountSource",     "account_source",      S, True,  "Lead source picklist"),
        ("Description",       "description",         S, True,  ""),
    ] + AUDIT),

    "contact": ("contact", "contact", "Id", "SystemModstamp", "contact_sk", [
        ("AccountId",          "account_id",             S, True,  "FK → Account.Id"),
        ("FirstName",          "first_name",             S, True,  ""),
        ("LastName",           "last_name",              S, False, ""),
        ("Email",              "email",                  S, True,  ""),
        ("Phone",              "phone",                  S, True,  ""),
        ("MobilePhone",        "mobile_phone",           S, True,  ""),
        ("Title",              "title",                  S, True,  ""),
        ("Department",         "department",             S, True,  ""),
        ("LeadSource",         "lead_source",            S, True,  ""),
        ("MailingCity",        "mailing_city",           S, True,  ""),
        ("MailingState",       "mailing_state",          S, True,  ""),
        ("MailingCountry",     "mailing_country",        S, True,  ""),
        ("OwnerId",            "owner_id",               S, True,  "FK → User.Id"),
        ("DoNotCall",          "do_not_call",            B, True,  "Opt-out flag"),
        ("HasOptedOutOfEmail", "has_opted_out_of_email", B, True,  "Email opt-out"),
        ("Description",        "description",            S, True,  ""),
    ] + AUDIT),

    "lead": ("lead", "lead", "Id", "SystemModstamp", "lead_sk", [
        ("FirstName",             "first_name",              S, True,  ""),
        ("LastName",              "last_name",               S, False, ""),
        ("Email",                 "email",                   S, True,  ""),
        ("Phone",                 "phone",                   S, True,  ""),
        ("Company",               "company",                 S, True,  ""),
        ("Title",                 "title",                   S, True,  ""),
        ("Industry",              "industry",                S, True,  ""),
        ("AnnualRevenue",         "annual_revenue",          D, True,  ""),
        ("NumberOfEmployees",     "number_of_employees",     L, True,  ""),
        ("LeadSource",            "lead_source",             S, True,  ""),
        ("Status",                "status",                  S, True,  "Open | Working | Converted | Unqualified"),
        ("Rating",                "rating",                  S, True,  ""),
        ("IsConverted",           "is_converted",            B, False, "TRUE once converted"),
        ("ConvertedDate",         "converted_date",          DT,True,  ""),
        ("ConvertedAccountId",    "converted_account_id",    S, True,  "FK → Account.Id"),
        ("ConvertedContactId",    "converted_contact_id",    S, True,  "FK → Contact.Id"),
        ("ConvertedOpportunityId","converted_opportunity_id",S, True,  "FK → Opportunity.Id"),
        ("OwnerId",               "owner_id",                S, True,  "FK → User.Id"),
        ("HasOptedOutOfEmail",    "has_opted_out_of_email",  B, True,  ""),
        ("DoNotCall",             "do_not_call",             B, True,  ""),
        ("City",                  "city",                    S, True,  ""),
        ("State",                 "state",                   S, True,  ""),
        ("Country",               "country",                 S, True,  ""),
        ("Description",           "description",             S, True,  ""),
    ] + AUDIT),

    "opportunity": ("opportunity", "opportunity", "Id", "SystemModstamp", "opportunity_sk", [
        ("AccountId",        "account_id",       S, True,  "FK → Account.Id"),
        ("Name",             "name",             S, False, ""),
        ("StageName",        "stage_name",       S, False, "Prospecting | … | Closed Won"),
        ("Amount",           "amount",           D, True,  ""),
        ("CloseDate",        "close_date",       DT,False, ""),
        ("Probability",      "probability",      D, True,  "0-100 %"),
        ("ForecastCategory", "forecast_category",S, True,  ""),
        ("Type",             "type",             S, True,  "New Business | Renewal | Upsell"),
        ("LeadSource",       "lead_source",      S, True,  ""),
        ("IsClosed",         "is_closed",        B, False, ""),
        ("IsWon",            "is_won",           B, False, ""),
        ("OwnerId",          "owner_id",         S, True,  "FK → User.Id"),
        ("CampaignId",       "campaign_id",      S, True,  "FK → Campaign.Id"),
        ("Pricebook2Id",     "pricebook2_id",    S, True,  "FK → Pricebook2.Id"),
        ("ExpectedRevenue",  "expected_revenue", D, True,  ""),
        ("NextStep",         "next_step",        S, True,  ""),
        ("Description",      "description",      S, True,  ""),
    ] + AUDIT),

    "opportunitylineitem": ("opportunitylineitem", "opportunity_line_item", "Id", "SystemModstamp", "opportunity_line_item_sk", [
        ("OpportunityId",    "opportunity_id",    S, False, "FK → Opportunity.Id"),
        ("Product2Id",       "product2_id",       S, True,  "FK → Product2.Id"),
        ("PricebookEntryId", "pricebook_entry_id",S, True,  "FK → PricebookEntry.Id"),
        ("Name",             "name",              S, True,  ""),
        ("Quantity",         "quantity",          D, True,  ""),
        ("UnitPrice",        "unit_price",        D, True,  ""),
        ("ListPrice",        "list_price",        D, True,  ""),
        ("TotalPrice",       "total_price",       D, True,  ""),
        ("Discount",         "discount",          D, True,  ""),
        ("ServiceDate",      "service_date",      DT,True,  ""),
        ("SortOrder",        "sort_order",        L, True,  ""),
        ("Description",      "description",       S, True,  ""),
    ] + AUDIT),

    "product2": ("product2", "product", "Id", "SystemModstamp", "product_sk", [
        ("Name",                  "name",                     S, False, ""),
        ("ProductCode",           "product_code",             S, True,  ""),
        ("Description",           "description",              S, True,  ""),
        ("IsActive",              "is_active",                B, False, ""),
        ("Family",                "family",                   S, True,  ""),
        ("QuantityUnitOfMeasure", "quantity_unit_of_measure", S, True,  ""),
        ("StockKeepingUnit",      "stock_keeping_unit",       S, True,  ""),
    ] + AUDIT),

    "pricebook2": ("pricebook2", "pricebook", "Id", "SystemModstamp", "pricebook_sk", [
        ("Name",       "name",        S, False, ""),
        ("Description","description", S, True,  ""),
        ("IsActive",   "is_active",   B, False, ""),
        ("IsStandard", "is_standard", B, False, "TRUE = Standard Price Book"),
        ("IsArchived", "is_archived", B, True,  ""),
    ] + AUDIT),

    "pricebookentry": ("pricebookentry", "pricebook_entry", "Id", "SystemModstamp", "pricebook_entry_sk", [
        ("Pricebook2Id",    "pricebook2_id",     S, False, "FK → Pricebook2.Id"),
        ("Product2Id",      "product2_id",       S, False, "FK → Product2.Id"),
        ("CurrencyIsoCode", "currency_iso_code", S, True,  "ISO 4217"),
        ("UnitPrice",       "unit_price",        D, False, ""),
        ("IsActive",        "is_active",         B, False, ""),
        ("UseStandardPrice","use_standard_price",B, False, ""),
    ] + AUDIT),

    "case": ("case", "support_case", "Id", "SystemModstamp", "support_case_sk", [
        ("CaseNumber", "case_number", S, False, "Auto-number"),
        ("AccountId",  "account_id",  S, True,  "FK → Account.Id"),
        ("ContactId",  "contact_id",  S, True,  "FK → Contact.Id"),
        ("Subject",    "subject",     S, True,  ""),
        ("Description","description", S, True,  ""),
        ("Status",     "status",      S, False, "New | Working | Escalated | Closed"),
        ("Priority",   "priority",    S, True,  "Low | Medium | High | Critical"),
        ("Origin",     "origin",      S, True,  "Email | Phone | Web | Chat"),
        ("Type",       "type",        S, True,  ""),
        ("IsEscalated","is_escalated",B, False, ""),
        ("IsClosed",   "is_closed",   B, False, ""),
        ("ClosedDate", "closed_date", TS,True,  ""),
        ("OwnerId",    "owner_id",    S, True,  "FK → User.Id or Queue.Id"),
        ("ParentId",   "parent_id",   S, True,  "FK → Case.Id"),
    ] + AUDIT),

    "casecomment": ("casecomment", "case_comment", "Id", "SystemModstamp", "case_comment_sk", [
        ("ParentId",   "case_id",      S, False, "FK → Case.Id"),
        ("CommentBody","comment_body", S, True,  ""),
        ("IsPublished","is_published", B, False, "FALSE = internal note"),
    ] + AUDIT),

    "task": ("task", "task", "Id", "SystemModstamp", "task_sk", [
        ("WhoId",                 "who_id",                S, True,  "FK → Contact.Id or Lead.Id"),
        ("WhatId",                "what_id",               S, True,  "FK → any object"),
        ("OwnerId",               "owner_id",              S, True,  "FK → User.Id"),
        ("Subject",               "subject",               S, True,  ""),
        ("Status",                "status",                S, True,  "Not Started | In Progress | Completed"),
        ("Priority",              "priority",              S, True,  "Normal | High | Low"),
        ("ActivityDate",          "activity_date",         DT,True,  ""),
        ("Type",                  "type",                  S, True,  "Call | Email | Meeting"),
        ("IsClosed",              "is_closed",             B, False, ""),
        ("CallDurationInSeconds", "call_duration_seconds", L, True,  ""),
        ("CallType",              "call_type",             S, True,  "Inbound | Outbound | Internal"),
        ("Description",           "description",           S, True,  ""),
    ] + AUDIT),

    "event": ("event", "event", "Id", "SystemModstamp", "event_sk", [
        ("WhoId",            "who_id",           S, True,  "FK → Contact.Id or Lead.Id"),
        ("WhatId",           "what_id",          S, True,  "FK → any object"),
        ("OwnerId",          "owner_id",         S, True,  "FK → User.Id"),
        ("Subject",          "subject",          S, True,  ""),
        ("Location",         "location",         S, True,  ""),
        ("StartDateTime",    "start_date_time",  TS,True,  ""),
        ("EndDateTime",      "end_date_time",    TS,True,  ""),
        ("DurationInMinutes","duration_minutes", L, True,  ""),
        ("IsAllDayEvent",    "is_all_day_event", B, False, ""),
        ("IsPrivate",        "is_private",       B, False, ""),
        ("Type",             "type",             S, True,  ""),
        ("ShowAs",           "show_as",          S, True,  "Busy | Out of Office | Free"),
        ("Description",      "description",      S, True,  ""),
    ] + AUDIT),

    "campaign": ("campaign", "campaign", "Id", "SystemModstamp", "campaign_sk", [
        ("Name",                     "name",                        S, False, ""),
        ("Type",                     "type",                        S, True,  ""),
        ("Status",                   "status",                      S, True,  "Planned | Active | Completed | Aborted"),
        ("StartDate",                "start_date",                  DT,True,  ""),
        ("EndDate",                  "end_date",                    DT,True,  ""),
        ("BudgetedCost",             "budgeted_cost",               D, True,  ""),
        ("ActualCost",               "actual_cost",                 D, True,  ""),
        ("NumberSent",               "number_sent",                 L, True,  ""),
        ("NumberOfLeads",            "number_of_leads",             L, True,  ""),
        ("NumberOfConvertedLeads",   "number_of_converted_leads",   L, True,  ""),
        ("NumberOfContacts",         "number_of_contacts",          L, True,  ""),
        ("NumberOfOpportunities",    "number_of_opportunities",     L, True,  ""),
        ("NumberOfWonOpportunities", "number_of_won_opportunities", L, True,  ""),
        ("AmountWonOpportunities",   "amount_won_opportunities",    D, True,  ""),
        ("IsActive",                 "is_active",                   B, False, ""),
        ("OwnerId",                  "owner_id",                    S, True,  "FK → User.Id"),
        ("ParentId",                 "parent_id",                   S, True,  "FK → Campaign.Id"),
        ("Description",              "description",                 S, True,  ""),
    ] + AUDIT),

    "campaignmember": ("campaignmember", "campaign_member", "Id", "SystemModstamp", "campaign_member_sk", [
        ("CampaignId",        "campaign_id",          S, False, "FK → Campaign.Id"),
        ("LeadId",            "lead_id",              S, True,  "FK → Lead.Id"),
        ("ContactId",         "contact_id",           S, True,  "FK → Contact.Id"),
        ("Status",            "status",               S, True,  "Sent | Responded"),
        ("HasResponded",      "has_responded",        B, False, ""),
        ("FirstRespondedDate","first_responded_date", DT,True,  ""),
    ] + AUDIT),

    "user": ("user", "sf_user", "Id", "SystemModstamp", "sf_user_sk", [
        ("FirstName",  "first_name",  S, True,  ""),
        ("LastName",   "last_name",   S, False, ""),
        ("Email",      "email",       S, False, ""),
        ("Username",   "username",    S, False, ""),
        ("Alias",      "alias",       S, True,  ""),
        ("Title",      "title",       S, True,  ""),
        ("Department", "department",  S, True,  ""),
        ("ManagerId",  "manager_id",  S, True,  "FK → User.Id"),
        ("ProfileId",  "profile_id",  S, True,  "FK → Profile"),
        ("IsActive",   "is_active",   B, False, "FALSE = deprovisioned"),
        ("UserType",   "user_type",   S, True,  "Standard | PowerPartner | Guest"),
        ("Phone",      "phone",       S, True,  ""),
        ("MobilePhone","mobile_phone",S, True,  ""),
    ] + AUDIT),
}

# COMMAND ----------
# MAGIC %md ### Seed `_schema_registry`

# COMMAND ----------

rows = []
for table_key, (bronze_tbl, silver_tbl, pk, wm_col, sk_col, cols) in REGISTRY.items():
    for ordinal, (bronze_col, silver_col, dtype, nullable, comment) in enumerate(cols):
        rows.append((
            table_key, bronze_tbl, silver_tbl, pk, wm_col, sk_col,
            ordinal, bronze_col, silver_col, dtype, nullable, comment, True,
        ))

schema = """
    table_key STRING, bronze_table STRING, silver_table STRING,
    pk STRING, watermark_col STRING, surrogate_key_col STRING,
    col_ordinal INT, bronze_col STRING, silver_col STRING,
    data_type STRING, nullable BOOLEAN, col_comment STRING, is_active BOOLEAN
"""
seed_df = spark.createDataFrame(rows, schema)

(
    DeltaTable.forName(spark, log("_schema_registry")).alias("tgt")
    .merge(seed_df.alias("src"),
           "tgt.table_key = src.table_key AND tgt.bronze_col = src.bronze_col")
    .whenMatchedUpdate(set={
        "silver_table"     : "src.silver_table",
        "silver_col"       : "src.silver_col",
        "data_type"        : "src.data_type",
        "nullable"         : "src.nullable",
        "col_ordinal"      : "src.col_ordinal",
        "col_comment"      : "src.col_comment",
        "surrogate_key_col": "src.surrogate_key_col",
        "is_active"        : "src.is_active",
        "updated_at"       : "current_timestamp()",
    })
    .whenNotMatchedInsert(values={
        "table_key"        : "src.table_key",
        "bronze_table"     : "src.bronze_table",
        "silver_table"     : "src.silver_table",
        "pk"               : "src.pk",
        "watermark_col"    : "src.watermark_col",
        "surrogate_key_col": "src.surrogate_key_col",
        "col_ordinal"      : "src.col_ordinal",
        "bronze_col"       : "src.bronze_col",
        "silver_col"       : "src.silver_col",
        "data_type"        : "src.data_type",
        "nullable"         : "src.nullable",
        "col_comment"      : "src.col_comment",
        "is_active"        : "src.is_active",
        "updated_at"       : "current_timestamp()",
    })
    .execute()
)
print(f"Registry seeded: {len(REGISTRY)} tables, {len(rows)} column mappings")

# COMMAND ----------
# MAGIC %md ### Seed `_watermark`

# COMMAND ----------

EPOCH = "1970-01-01T00:00:00.000+0000"

for table_key, (bronze_tbl, _, _, wm_col, _, _) in REGISTRY.items():
    fq_source = bronze(bronze_tbl)
    spark.sql(f"""
        MERGE INTO {log('_watermark')} AS tgt
        USING (
            SELECT
                '{fq_source}'  AS source_table,
                '{wm_col}'     AS watermark_col,
                CAST('{EPOCH}' AS TIMESTAMP) AS last_watermark_ts,
                '{ENV}'        AS environment,
                current_timestamp() AS updated_at,
                NULL           AS updated_by_run_id
        ) AS src
        ON  tgt.source_table = src.source_table
        AND tgt.environment  = src.environment
        WHEN NOT MATCHED THEN INSERT *
    """)

print(f"Watermark seeded: {len(REGISTRY)} rows (existing positions untouched)")

# COMMAND ----------
# MAGIC %md ### Verify

# COMMAND ----------

display(spark.sql(f"""
    SELECT r.table_key, r.silver_table, r.surrogate_key_col,
           count(*) AS col_count,
           w.last_watermark_ts, w.updated_at AS watermark_updated
    FROM   {log('_schema_registry')} r
    JOIN   {log('_watermark')}       w
        ON w.source_table = concat('{CATALOG}.{BRONZE_SCHEMA}.', r.bronze_table)
       AND w.environment  = '{ENV}'
    WHERE  r.is_active = true
    GROUP  BY r.table_key, r.silver_table, r.surrogate_key_col,
              w.last_watermark_ts, w.updated_at
    ORDER  BY r.table_key
"""))
