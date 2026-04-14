# Databricks notebook source
# MAGIC %md
# MAGIC # 00b · Schema Registry — Setup & Seed
# MAGIC
# MAGIC Creates `pipeline_control._schema_registry` and populates it with
# MAGIC all 15 Salesforce source → Silver column mappings.
# MAGIC
# MAGIC **Re-runnable.** Uses MERGE on `(table_key, bronze_col)` so existing rows
# MAGIC are updated in place, new rows are inserted, and nothing is duplicated.

# COMMAND ----------

CATALOG        = "my_catalog"
LOGGING_SCHEMA = "pipeline_control"
REGISTRY_FQ    = f"{CATALOG}.{LOGGING_SCHEMA}._schema_registry"

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {REGISTRY_FQ} (
    table_key      STRING   NOT NULL  COMMENT 'Registry key, e.g. account',
    bronze_table   STRING   NOT NULL  COMMENT 'Unqualified Bronze table name',
    silver_table   STRING   NOT NULL  COMMENT 'Unqualified Silver table name',
    pk             STRING   NOT NULL  COMMENT 'PK column in Bronze (always Id for Salesforce)',
    watermark_col  STRING   NOT NULL  COMMENT 'Column driving incremental load',
    col_ordinal    INT      NOT NULL  COMMENT 'Column order for DDL and SELECT',
    bronze_col     STRING   NOT NULL  COMMENT 'Column name as it exists in Bronze',
    silver_col     STRING   NOT NULL  COMMENT 'Column name in Silver (renamed where needed)',
    data_type      STRING   NOT NULL  COMMENT 'Spark SQL type: STRING | BOOLEAN | BIGINT | DECIMAL(18,4) | DATE | TIMESTAMP',
    nullable       BOOLEAN  NOT NULL  COMMENT 'Whether the Silver column allows NULLs',
    col_comment    STRING            COMMENT 'Business description',
    is_active      BOOLEAN  NOT NULL  COMMENT 'FALSE = exclude column without deleting the row',
    updated_at     TIMESTAMP         COMMENT 'Row last-modified timestamp'
)
USING DELTA
COMMENT 'Single source of truth for all Bronze → Silver column mappings.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'pipeline.owner'             = 'data_engineering'
)
""")
print(f"Table ready: {REGISTRY_FQ}")

# COMMAND ----------
# MAGIC %md ### Column definitions

# COMMAND ----------

# Type shorthands
S  = "STRING"
B  = "BOOLEAN"
L  = "BIGINT"
D  = "DECIMAL(18,4)"
DT = "DATE"
TS = "TIMESTAMP"

# Audit columns appended to every Salesforce object
AUDIT = [
    ("Id",               "sf_id",               S,  False, "Salesforce 18-char record ID (PK)"),
    ("CreatedDate",      "created_date",         TS, False, "Record creation timestamp (UTC)"),
    ("LastModifiedDate", "last_modified_date",   TS, True,  "Last user-visible modification (UTC)"),
    ("SystemModstamp",   "system_modstamp",      TS, False, "System-level last-change stamp; watermark column"),
    ("IsDeleted",        "is_deleted",           B,  False, "Salesforce recycle-bin soft-delete flag"),
    ("CreatedById",      "created_by_id",        S,  True,  "FK → User.Id — record creator"),
    ("LastModifiedById", "last_modified_by_id",  S,  True,  "FK → User.Id — last modifier"),
]

# (table_key, bronze_table, silver_table, pk, watermark_col, [ (bronze_col, silver_col, type, nullable, comment) ])
REGISTRY = {
    "account": ("account", "account", "Id", "SystemModstamp", [
        ("Name",              "name",                S, False, "Account / company name"),
        ("Type",              "type",                S, True,  "Prospect | Customer | Partner"),
        ("Industry",          "industry",            S, True,  "Standard SF industry picklist"),
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
        ("ParentId",          "parent_id",           S, True,  "FK → Account.Id (hierarchy)"),
        ("AccountSource",     "account_source",      S, True,  "Lead source picklist"),
        ("Description",       "description",         S, True,  ""),
    ] + AUDIT),

    "contact": ("contact", "contact", "Id", "SystemModstamp", [
        ("AccountId",            "account_id",              S, True,  "FK → Account.Id"),
        ("FirstName",            "first_name",              S, True,  ""),
        ("LastName",             "last_name",               S, False, ""),
        ("Email",                "email",                   S, True,  "Primary email address"),
        ("Phone",                "phone",                   S, True,  ""),
        ("MobilePhone",          "mobile_phone",            S, True,  ""),
        ("Title",                "title",                   S, True,  "Job title"),
        ("Department",           "department",              S, True,  ""),
        ("LeadSource",           "lead_source",             S, True,  "How contact was acquired"),
        ("MailingCity",          "mailing_city",            S, True,  ""),
        ("MailingState",         "mailing_state",           S, True,  ""),
        ("MailingCountry",       "mailing_country",         S, True,  ""),
        ("OwnerId",              "owner_id",                S, True,  "FK → User.Id"),
        ("DoNotCall",            "do_not_call",             B, True,  "Opt-out flag"),
        ("HasOptedOutOfEmail",   "has_opted_out_of_email",  B, True,  "Email opt-out flag"),
        ("Description",          "description",             S, True,  ""),
    ] + AUDIT),

    "lead": ("lead", "lead", "Id", "SystemModstamp", [
        ("FirstName",             "first_name",              S, True,  ""),
        ("LastName",              "last_name",               S, False, ""),
        ("Email",                 "email",                   S, True,  ""),
        ("Phone",                 "phone",                   S, True,  ""),
        ("Company",               "company",                 S, True,  "Self-reported company name"),
        ("Title",                 "title",                   S, True,  ""),
        ("Industry",              "industry",                S, True,  ""),
        ("AnnualRevenue",         "annual_revenue",          D, True,  ""),
        ("NumberOfEmployees",     "number_of_employees",     L, True,  ""),
        ("LeadSource",            "lead_source",             S, True,  ""),
        ("Status",                "status",                  S, True,  "Open | Working | Converted | Unqualified"),
        ("Rating",                "rating",                  S, True,  "Hot | Warm | Cold"),
        ("IsConverted",           "is_converted",            B, False, "TRUE once MQL→SQL conversion happens"),
        ("ConvertedDate",         "converted_date",          DT,True,  "Date of conversion"),
        ("ConvertedAccountId",    "converted_account_id",    S, True,  "FK → Account.Id post-conversion"),
        ("ConvertedContactId",    "converted_contact_id",    S, True,  "FK → Contact.Id post-conversion"),
        ("ConvertedOpportunityId","converted_opportunity_id",S, True,  "FK → Opportunity.Id post-conversion"),
        ("OwnerId",               "owner_id",                S, True,  "FK → User.Id"),
        ("HasOptedOutOfEmail",    "has_opted_out_of_email",  B, True,  ""),
        ("DoNotCall",             "do_not_call",             B, True,  ""),
        ("City",                  "city",                    S, True,  ""),
        ("State",                 "state",                   S, True,  ""),
        ("Country",               "country",                 S, True,  ""),
        ("Description",           "description",             S, True,  ""),
    ] + AUDIT),

    "opportunity": ("opportunity", "opportunity", "Id", "SystemModstamp", [
        ("AccountId",        "account_id",       S, True,  "FK → Account.Id"),
        ("Name",             "name",             S, False, "Opportunity name"),
        ("StageName",        "stage_name",       S, False, "Prospecting | Qualification | … | Closed Won"),
        ("Amount",           "amount",           D, True,  "Expected deal value"),
        ("CloseDate",        "close_date",       DT,False, "Expected or actual close date"),
        ("Probability",      "probability",      D, True,  "0-100 close probability %"),
        ("ForecastCategory", "forecast_category",S, True,  "Pipeline | Best Case | Commit | Closed"),
        ("Type",             "type",             S, True,  "New Business | Renewal | Upsell | Cross-Sell"),
        ("LeadSource",       "lead_source",      S, True,  ""),
        ("IsClosed",         "is_closed",        B, False, "TRUE when stage is terminal"),
        ("IsWon",            "is_won",           B, False, "TRUE for Closed Won only"),
        ("OwnerId",          "owner_id",         S, True,  "FK → User.Id"),
        ("CampaignId",       "campaign_id",      S, True,  "FK → Campaign.Id"),
        ("Pricebook2Id",     "pricebook2_id",    S, True,  "FK → Pricebook2.Id"),
        ("ExpectedRevenue",  "expected_revenue", D, True,  "Amount × Probability / 100"),
        ("NextStep",         "next_step",        S, True,  ""),
        ("Description",      "description",      S, True,  ""),
    ] + AUDIT),

    "opportunitylineitem": ("opportunitylineitem", "opportunity_line_item", "Id", "SystemModstamp", [
        ("OpportunityId",    "opportunity_id",    S, False, "FK → Opportunity.Id"),
        ("Product2Id",       "product2_id",       S, True,  "FK → Product2.Id"),
        ("PricebookEntryId", "pricebook_entry_id",S, True,  "FK → PricebookEntry.Id"),
        ("Name",             "name",              S, True,  "Product name at line creation"),
        ("Quantity",         "quantity",          D, True,  ""),
        ("UnitPrice",        "unit_price",        D, True,  ""),
        ("ListPrice",        "list_price",        D, True,  ""),
        ("TotalPrice",       "total_price",       D, True,  "Quantity × UnitPrice"),
        ("Discount",         "discount",          D, True,  "Discount % applied"),
        ("ServiceDate",      "service_date",      DT,True,  ""),
        ("SortOrder",        "sort_order",        L, True,  ""),
        ("Description",      "description",       S, True,  ""),
    ] + AUDIT),

    "product2": ("product2", "product", "Id", "SystemModstamp", [
        ("Name",                    "name",                     S, False, "Product name"),
        ("ProductCode",             "product_code",             S, True,  "Internal SKU"),
        ("Description",             "description",              S, True,  ""),
        ("IsActive",                "is_active",                B, False, "FALSE = archived"),
        ("Family",                  "family",                   S, True,  "Product category picklist"),
        ("QuantityUnitOfMeasure",   "quantity_unit_of_measure", S, True,  ""),
        ("StockKeepingUnit",        "stock_keeping_unit",       S, True,  ""),
    ] + AUDIT),

    "pricebook2": ("pricebook2", "pricebook", "Id", "SystemModstamp", [
        ("Name",        "name",        S, False, ""),
        ("Description", "description", S, True,  ""),
        ("IsActive",    "is_active",   B, False, ""),
        ("IsStandard",  "is_standard", B, False, "TRUE = Standard Price Book"),
        ("IsArchived",  "is_archived", B, True,  ""),
    ] + AUDIT),

    "pricebookentry": ("pricebookentry", "pricebook_entry", "Id", "SystemModstamp", [
        ("Pricebook2Id",     "pricebook2_id",     S, False, "FK → Pricebook2.Id"),
        ("Product2Id",       "product2_id",       S, False, "FK → Product2.Id"),
        ("CurrencyIsoCode",  "currency_iso_code", S, True,  "ISO 4217 currency code"),
        ("UnitPrice",        "unit_price",        D, False, "Listed price in this pricebook"),
        ("IsActive",         "is_active",         B, False, ""),
        ("UseStandardPrice", "use_standard_price",B, False, "TRUE = inherit from Standard PB"),
    ] + AUDIT),

    "case": ("case", "support_case", "Id", "SystemModstamp", [
        ("CaseNumber",  "case_number",  S, False, "Auto-number displayed to customers"),
        ("AccountId",   "account_id",   S, True,  "FK → Account.Id"),
        ("ContactId",   "contact_id",   S, True,  "FK → Contact.Id"),
        ("Subject",     "subject",      S, True,  ""),
        ("Description", "description",  S, True,  ""),
        ("Status",      "status",       S, False, "New | Working | Escalated | Closed"),
        ("Priority",    "priority",     S, True,  "Low | Medium | High | Critical"),
        ("Origin",      "origin",       S, True,  "Email | Phone | Web | Chat"),
        ("Type",        "type",         S, True,  "Question | Problem | Feature Request"),
        ("IsEscalated", "is_escalated", B, False, ""),
        ("IsClosed",    "is_closed",    B, False, ""),
        ("ClosedDate",  "closed_date",  TS,True,  ""),
        ("OwnerId",     "owner_id",     S, True,  "FK → User.Id or Queue.Id"),
        ("ParentId",    "parent_id",    S, True,  "FK → Case.Id (hierarchy)"),
    ] + AUDIT),

    "casecomment": ("casecomment", "case_comment", "Id", "SystemModstamp", [
        ("ParentId",    "case_id",      S, False, "FK → Case.Id"),
        ("CommentBody", "comment_body", S, True,  "Comment text"),
        ("IsPublished", "is_published", B, False, "FALSE = internal note only"),
    ] + AUDIT),

    "task": ("task", "task", "Id", "SystemModstamp", [
        ("WhoId",                  "who_id",                S, True,  "FK → Contact.Id or Lead.Id"),
        ("WhatId",                 "what_id",               S, True,  "FK → Opportunity, Account, etc."),
        ("OwnerId",                "owner_id",              S, True,  "FK → User.Id"),
        ("Subject",                "subject",               S, True,  ""),
        ("Status",                 "status",                S, True,  "Not Started | In Progress | Completed | Deferred"),
        ("Priority",               "priority",              S, True,  "Normal | High | Low"),
        ("ActivityDate",           "activity_date",         DT,True,  "Due date"),
        ("Type",                   "type",                  S, True,  "Call | Email | Meeting"),
        ("IsClosed",               "is_closed",             B, False, ""),
        ("CallDurationInSeconds",  "call_duration_seconds", L, True,  ""),
        ("CallType",               "call_type",             S, True,  "Inbound | Outbound | Internal"),
        ("Description",            "description",           S, True,  ""),
    ] + AUDIT),

    "event": ("event", "event", "Id", "SystemModstamp", [
        ("WhoId",            "who_id",           S, True,  "FK → Contact.Id or Lead.Id"),
        ("WhatId",           "what_id",          S, True,  "FK → any object"),
        ("OwnerId",          "owner_id",         S, True,  "FK → User.Id"),
        ("Subject",          "subject",          S, True,  ""),
        ("Location",         "location",         S, True,  ""),
        ("StartDateTime",    "start_date_time",  TS,True,  "Event start (UTC)"),
        ("EndDateTime",      "end_date_time",    TS,True,  "Event end (UTC)"),
        ("DurationInMinutes","duration_minutes", L, True,  ""),
        ("IsAllDayEvent",    "is_all_day_event", B, False, ""),
        ("IsPrivate",        "is_private",       B, False, ""),
        ("Type",             "type",             S, True,  ""),
        ("ShowAs",           "show_as",          S, True,  "Busy | Out of Office | Free"),
        ("Description",      "description",      S, True,  ""),
    ] + AUDIT),

    "campaign": ("campaign", "campaign", "Id", "SystemModstamp", [
        ("Name",                        "name",                         S, False, ""),
        ("Type",                        "type",                         S, True,  "Email | Webinar | Tradeshow"),
        ("Status",                      "status",                       S, True,  "Planned | Active | Completed | Aborted"),
        ("StartDate",                   "start_date",                   DT,True,  ""),
        ("EndDate",                     "end_date",                     DT,True,  ""),
        ("BudgetedCost",                "budgeted_cost",                D, True,  ""),
        ("ActualCost",                  "actual_cost",                  D, True,  ""),
        ("NumberSent",                  "number_sent",                  L, True,  "Emails / items sent"),
        ("NumberOfLeads",               "number_of_leads",              L, True,  ""),
        ("NumberOfConvertedLeads",      "number_of_converted_leads",    L, True,  ""),
        ("NumberOfContacts",            "number_of_contacts",           L, True,  ""),
        ("NumberOfOpportunities",       "number_of_opportunities",      L, True,  ""),
        ("NumberOfWonOpportunities",    "number_of_won_opportunities",  L, True,  ""),
        ("AmountWonOpportunities",      "amount_won_opportunities",     D, True,  ""),
        ("IsActive",                    "is_active",                    B, False, ""),
        ("OwnerId",                     "owner_id",                     S, True,  "FK → User.Id"),
        ("ParentId",                    "parent_id",                    S, True,  "FK → Campaign.Id (hierarchy)"),
        ("Description",                 "description",                  S, True,  ""),
    ] + AUDIT),

    "campaignmember": ("campaignmember", "campaign_member", "Id", "SystemModstamp", [
        ("CampaignId",        "campaign_id",          S, False, "FK → Campaign.Id"),
        ("LeadId",            "lead_id",              S, True,  "FK → Lead.Id"),
        ("ContactId",         "contact_id",           S, True,  "FK → Contact.Id"),
        ("Status",            "status",               S, True,  "Sent | Responded"),
        ("HasResponded",      "has_responded",        B, False, ""),
        ("FirstRespondedDate","first_responded_date", DT,True,  ""),
    ] + AUDIT),

    "user": ("user", "sf_user", "Id", "SystemModstamp", [
        ("FirstName",  "first_name",  S, True,  ""),
        ("LastName",   "last_name",   S, False, ""),
        ("Email",      "email",       S, False, ""),
        ("Username",   "username",    S, False, "Unique login (org-scoped email format)"),
        ("Alias",      "alias",       S, True,  ""),
        ("Title",      "title",       S, True,  ""),
        ("Department", "department",  S, True,  ""),
        ("ManagerId",  "manager_id",  S, True,  "FK → User.Id"),
        ("ProfileId",  "profile_id",  S, True,  "FK → Profile object"),
        ("IsActive",   "is_active",   B, False, "FALSE = deprovisioned user"),
        ("UserType",   "user_type",   S, True,  "Standard | PowerPartner | Guest"),
        ("Phone",      "phone",       S, True,  ""),
        ("MobilePhone","mobile_phone",S, True,  ""),
    ] + AUDIT),
}

# COMMAND ----------
# MAGIC %md ### Seed — MERGE into registry table

# COMMAND ----------

rows = []
for table_key, (bronze_tbl, silver_tbl, pk, wm_col, cols) in REGISTRY.items():
    for ordinal, (bronze_col, silver_col, dtype, nullable, comment) in enumerate(cols):
        rows.append((
            table_key, bronze_tbl, silver_tbl, pk, wm_col,
            ordinal, bronze_col, silver_col, dtype, nullable, comment, True,
        ))

schema = """
    table_key STRING, bronze_table STRING, silver_table STRING,
    pk STRING, watermark_col STRING, col_ordinal INT,
    bronze_col STRING, silver_col STRING, data_type STRING,
    nullable BOOLEAN, col_comment STRING, is_active BOOLEAN
"""
seed_df = spark.createDataFrame(rows, schema)

from delta.tables import DeltaTable
registry_tbl = DeltaTable.forName(spark, REGISTRY_FQ)

(
    registry_tbl.alias("tgt")
    .merge(
        seed_df.alias("src"),
        "tgt.table_key = src.table_key AND tgt.bronze_col = src.bronze_col"
    )
    .whenMatchedUpdate(set={
        "silver_table"  : "src.silver_table",
        "silver_col"    : "src.silver_col",
        "data_type"     : "src.data_type",
        "nullable"      : "src.nullable",
        "col_ordinal"   : "src.col_ordinal",
        "col_comment"   : "src.col_comment",
        "is_active"     : "src.is_active",
        "updated_at"    : "current_timestamp()",
    })
    .whenNotMatchedInsert(values={
        "table_key"     : "src.table_key",
        "bronze_table"  : "src.bronze_table",
        "silver_table"  : "src.silver_table",
        "pk"            : "src.pk",
        "watermark_col" : "src.watermark_col",
        "col_ordinal"   : "src.col_ordinal",
        "bronze_col"    : "src.bronze_col",
        "silver_col"    : "src.silver_col",
        "data_type"     : "src.data_type",
        "nullable"      : "src.nullable",
        "col_comment"   : "src.col_comment",
        "is_active"     : "src.is_active",
        "updated_at"    : "current_timestamp()",
    })
    .execute()
)

total = spark.table(REGISTRY_FQ).count()
print(f"Registry seeded — {total} active column mappings across {len(REGISTRY)} tables.")

# COMMAND ----------
# MAGIC %md ### Verify

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT table_key, silver_table, count(*) AS col_count
        FROM   {REGISTRY_FQ}
        WHERE  is_active = true
        GROUP  BY table_key, silver_table
        ORDER  BY table_key
    """)
)
