# LHP Templates

This directory contains reusable LHP templates for common data pipeline patterns.

## Naming Conventions

**Templates:** `TMPL<number>_<source_type>_<function>.yaml`

- Example: `TMPL001_delta_scd2.yaml`, `TMPL002_cloudfiles_bronze.yaml`

**Flowgroups using templates:** `<domain>_<final_table>_TMPL<number>`
- Example: `billing_invoice_TMPL001`, `orders_customer_TMPL002`
- The TMPL number must match the template being used

## Available Templates

### TMPL001_delta_scd2.yaml

**Purpose:** Implements SCD Type 2 (Slowly Changing Dimension Type 2) pattern for tracking historical changes to records.

**Use Cases:**
- Tracking historical changes to dimension tables
- Maintaining version history of business entities
- Processing CDC (Change Data Capture) streams from databases like PostgreSQL

**How it Works:**
1. **Load**: Streams data from a source Delta table
2. **Transform**:
   - Excludes CDC metadata columns (e.g., `__START_AT`, `__END_AT` from PostgreSQL WAL)
   - Excludes audit columns (e.g., `created_by`, `modified_by`)
   - Creates a sequence timestamp using `COALESCE(modified_at, created_at)` for SCD2 ordering
3. **Write**: Writes to streaming table with SCD Type 2 configuration

**Required Parameters:**

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `source_table` | string | Fully qualified source table name | `"catalog.schema.invoice"` |
| `target_table` | string | Target table name (no catalog/schema) | `"invoice"` |
| `natural_keys` | array | Business identifier columns | `["invoice_number"]` or `["customer_id", "order_id"]` |
| `modified_at_column` | string | Modification timestamp column name | `"modified_at"` or `"updated_at"` |
| `created_at_column` | string | Creation timestamp column name (fallback) | `"created_at"` |

**Optional Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `exclude_columns` | array | `["__START_AT", "__END_AT", "created_at", "modified_at", "created_by", "modified_by"]` | Columns to exclude from output |
| `sequence_timestamp_name` | string | `"_sequence_timestamp"` | Name for generated sequence column |

**Example Usage:**

```yaml
pipeline: billing_bronze
flowgroup: billing_invoice_TMPL001

use_template: TMPL001_delta_scd2

template_parameters:
  source_table: "brickwell_health.billing_ingestion_schema.invoice"
  target_table: "invoice"
  natural_keys:
    - invoice_number
  modified_at_column: "modified_at"
  created_at_column: "created_at"
  exclude_columns:
    - "__START_AT"
    - "__END_AT"
    - "created_at"
    - "modified_at"
    - "created_by"
    - "modified_by"
```

**Multiple Natural Keys Example:**

For composite keys (e.g., a junction table):

```yaml
template_parameters:
  source_table: "catalog.schema.order_items"
  target_table: "order_items"
  natural_keys:
    - order_id
    - product_id
  modified_at_column: "updated_at"
  created_at_column: "created_at"
```

**Benefits:**
- ✅ Consistent SCD2 implementation across all tables
- ✅ Reduces boilerplate YAML configuration
- ✅ Makes it easy to onboard new CDC sources
- ✅ Handles PostgreSQL WAL metadata automatically
- ✅ Supports both single and composite natural keys

---

### TMPL002_append_only.yaml

**Purpose:** Implements append-only pattern for immutable event tables that don't change after creation.

**Use Cases:**
- Digital events and web sessions
- Communication records and campaign responses
- Survey responses (CSAT, NPS)
- Any event/log data that represents point-in-time snapshots

**How it Works:**
1. **Load**: Streams data from a source Delta table
2. **Transform**:
   - Converts BINARY UUID columns to hex strings (if specified)
   - Excludes CDC metadata columns (e.g., `__START_AT`, `__END_AT`)
   - Adds `_processing_timestamp` for audit trail
3. **Write**: Writes to streaming table in APPEND mode (no CDC, no history tracking)

**Required Parameters:**

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `source_table` | string | Fully qualified source table name | `"catalog.schema.digital_event"` |
| `target_table` | string | Target table name (no catalog/schema) | `"digital_event"` |

**Optional Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `exclude_columns` | array | `["__START_AT", "__END_AT", "modified_at", "created_by", "modified_by"]` | Columns to exclude from output |
| `uuid_columns` | array | `[]` | BINARY UUID columns to convert to hex strings |

**Example Usage:**

```yaml
pipeline: communication_bronze
flowgroup: communication_campaign_response_TMPL002

use_template: TMPL002_append_only

template_parameters:
  source_table: "brickwell_health.communication_ingestion_schema.campaign_response"
  target_table: "campaign_response"
  exclude_columns:
    - "__START_AT"
    - "__END_AT"
    - "modified_at"
```

**Benefits:**
- ✅ Simple, efficient pattern for immutable event data
- ✅ No unnecessary CDC overhead for data that never changes
- ✅ Maintains complete event history with processing timestamps
- ✅ Optimal for analytics and time-series queries

---

### TMPL003_scd_type1.yaml

**Purpose:** Implements SCD Type 1 pattern for reference/lookup tables - maintains only current state with no historical tracking.

**Use Cases:**
- Reference tables (benefit categories, case types, complaint categories)
- Lookup tables (communication templates, interaction types, survey types)
- Dimension tables where history is not required (products, providers, hospitals)

**How it Works:**
1. **Load**: Streams data from a source Delta table
2. **Transform**:
   - Converts BINARY UUID columns to hex strings (if specified)
   - Excludes CDC metadata columns
   - Creates a sequence column based on LSN (Log Sequence Number) for ordering
3. **Write**: Writes to streaming table with SCD Type 1 configuration (latest record wins)

**Required Parameters:**

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `source_table` | string | Fully qualified source table name | `"catalog.schema.benefit_category"` |
| `target_table` | string | Target table name (no catalog/schema) | `"benefit_category"` |
| `natural_keys` | array | Business identifier columns for matching | `["category_code"]` |

**Optional Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `lsn_column` | string | `"__LSN"` | Log Sequence Number column for ordering |
| `exclude_columns` | array | `["__START_AT", "__END_AT", "__LSN", "created_at", "modified_at", "created_by", "modified_by"]` | Columns to exclude from output |
| `sequence_timestamp_name` | string | `"_sequence_lsn"` | Name for generated sequence column |
| `uuid_columns` | array | `[]` | BINARY UUID columns to convert to hex strings |

**Example Usage:**

```yaml
pipeline: reference_bronze
flowgroup: reference_benefit_category_TMPL003

use_template: TMPL003_scd_type1

template_parameters:
  source_table: "brickwell_health.reference_ingestion_schema.benefit_category"
  target_table: "benefit_category"
  natural_keys:
    - category_code
  exclude_columns:
    - "__START_AT"
    - "__END_AT"
    - "__LSN"
    - "created_at"
    - "modified_at"
```

**Benefits:**
- ✅ Always maintains current state (no historical versions)
- ✅ Simpler than SCD Type 2 for tables where history isn't needed
- ✅ Uses LSN ordering for correct sequencing of changes
- ✅ Ideal for lookup tables that change infrequently

---

### TMPL004_cdc_log_scd2.yaml

**Purpose:** Implements SCD Type 2 pattern for CDC log tables with epoch timestamps and partial update support (`ignore_null_updates`).

**Use Cases:**
- CDC event streams where updates only contain changed columns + primary key (nulls for unchanged)
- Source tables with timestamps stored as bigint microsecond epoch
- Source tables with date columns stored as int epoch days

**How it Works:**
1. **Load**: Streams data from a source Delta table
2. **Transform**:
   - Converts epoch day columns (int) to DATE via `date_add('1970-01-01', col)`
   - Excludes CDC event metadata (`_event_id`, `_event_type`, `_event_timestamp`, `_event_worker_id`)
   - Excludes audit columns (`created_at`, `modified_at`, `created_by`, `modified_by`)
   - Creates sequence timestamp: `COALESCE(timestamp_micros(modified_at), timestamp_micros(created_at))`
3. **Write**: Writes to streaming table with SCD Type 2 and `ignore_null_updates: true`

**Required Parameters:**

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `source_table` | string | Fully qualified source table name | `"catalog.schema.claim"` |
| `target_table` | string | Target table name (no catalog/schema) | `"claim"` |
| `target_schema` | string | Target schema name | `"edw_bronze_claim"` |
| `natural_keys` | array | Business identifier columns | `["claim_id"]` |

**Optional Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `modified_at_column` | string | `"modified_at"` | Bigint microsecond epoch column for modification time |
| `created_at_column` | string | `"created_at"` | Bigint microsecond epoch column for creation time |
| `epoch_day_columns` | array | `[]` | Int columns to convert from epoch days to DATE |
| `exclude_columns` | array | `["_event_id", "_event_type", "_event_timestamp", "_event_worker_id", "created_at", "modified_at", "created_by", "modified_by"]` | Columns to exclude |
| `sequence_timestamp_name` | string | `"_sequence_timestamp"` | Name for generated sequence column |

**Example Usage:**

```yaml
pipeline: claim_bronze
flowgroup: claim_claim_TMPL004

use_template: TMPL004_cdc_log_scd2

template_parameters:
  source_table: "brickwell_health.ingest_schema_bwh.claim"
  target_table: "claim"
  natural_keys:
    - claim_id
  epoch_day_columns:
    - service_date
    - lodgement_date
    - assessment_date
    - payment_date
  target_schema: "edw_bronze_claim"
```

**Benefits:**
- Handles partial CDC updates where only changed columns are populated
- Converts epoch microsecond timestamps to proper TIMESTAMP for SCD2 sequencing
- Converts epoch day integers to proper DATE type
- No UUID conversion needed (IDs already stored as strings)

---

### TMPL005_cdc_log_append.yaml

**Purpose:** Implements append-only pattern for CDC log insert-only tables with epoch timestamp/date conversion.

**Use Cases:**
- CDC event streams that only contain insert events (no updates)
- Source tables with timestamps stored as bigint microsecond epoch
- Source tables with date columns stored as int epoch days

**How it Works:**
1. **Load**: Streams data from a source Delta table
2. **Transform**:
   - Converts epoch day columns (int) to DATE via `date_add('1970-01-01', col)`
   - Converts epoch microsecond columns (bigint) to TIMESTAMP via `timestamp_micros()`
   - Converts `created_at` from bigint microsecond epoch to TIMESTAMP (kept in output)
   - Excludes CDC event metadata and audit columns
3. **Write**: Writes to streaming table in APPEND mode

**Required Parameters:**

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `source_table` | string | Fully qualified source table name | `"catalog.schema.ambulance_claim"` |
| `target_table` | string | Target table name (no catalog/schema) | `"ambulance_claim"` |
| `target_schema` | string | Target schema name | `"edw_bronze_claim"` |

**Optional Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `epoch_day_columns` | array | `[]` | Int columns to convert from epoch days to DATE |
| `epoch_us_columns` | array | `[]` | Bigint columns to convert from microsecond epoch to TIMESTAMP |
| `created_at_column` | string | `"created_at"` | Creation timestamp column (converted and kept in output) |
| `exclude_columns` | array | `["_event_id", "_event_type", "_event_timestamp", "_event_worker_id", "created_by"]` | Columns to exclude |

**Example Usage:**

```yaml
pipeline: claim_bronze
flowgroup: claim_ambulance_claim_TMPL005

use_template: TMPL005_cdc_log_append

template_parameters:
  source_table: "brickwell_health.ingest_schema_bwh.ambulance_claim"
  target_table: "ambulance_claim"
  epoch_day_columns:
    - incident_date
  target_schema: "edw_bronze_claim"
```

**Benefits:**
- Simple, efficient pattern for immutable CDC event data
- Converts epoch timestamps and dates to proper types in bronze layer
- Retains `created_at` as proper TIMESTAMP for audit trail
- No unnecessary CDC overhead for data that never changes

---

## Using Templates

To use a template in your flowgroup YAML:

1. Reference the template with `use_template: <template_name>`
2. Provide required parameters under `template_parameters:`
3. Run validation: `lhp validate --env dev`
4. Generate Python: `lhp generate --env dev`

## Creating New Templates

See the [LHP templates-presets reference](../.claude/skills/lhp/references/templates-presets.md) for detailed guidance on creating templates.

**Template Structure:**
```yaml
name: template_name
version: "1.0"
description: "What this template does"

parameters:
  - name: param_name
    type: string|array|object|boolean|number
    required: true|false
    default: value  # if not required

actions:
  # Use {{ param_name }} for substitution
  - name: load_{{ table_name }}
    # ... action configuration
```
