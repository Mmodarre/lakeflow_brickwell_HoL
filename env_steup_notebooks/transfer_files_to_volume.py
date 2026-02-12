# Databricks notebook source
# MAGIC %md
# MAGIC # Brickwell Health - Incremental File Transfer Between Volumes
# MAGIC
# MAGIC Transfers extracted files from one Unity Catalog volume to another incrementally,
# MAGIC processing **one month at a time** and tracking progress in a Delta table.
# MAGIC
# MAGIC **Features:**
# MAGIC - Incremental processing (one month at a time across all tables)
# MAGIC - Parallel processing support (process multiple tables concurrently)
# MAGIC - Progress tracking via Delta table
# MAGIC - Detects `extraction_month=YYYY-MM` Hive-style partition folders
# MAGIC - Handles sparse months (tables without data for a given month are skipped)
# MAGIC - Restart capability to begin from scratch
# MAGIC - Supports cross-catalog and cross-schema transfers
# MAGIC
# MAGIC **Source Structure:**
# MAGIC ```
# MAGIC source_volume/
# MAGIC ├── schema_name/
# MAGIC │   ├── table_name/
# MAGIC │   │   ├── extraction_month=2020-01/
# MAGIC │   │   │   └── *.parquet / *.json / *.csv
# MAGIC │   │   ├── extraction_month=2020-02/
# MAGIC │   │   └── ...
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration Parameters

# COMMAND ----------

# DBTITLE 1,Create Widgets
dbutils.widgets.text("source_volume_path", "/Volumes/brickwell_health/edw_raw/extraction_volume", "Source Volume Path")
dbutils.widgets.text("target_volume_path", "/Volumes/brickwell_health/edw_raw/landing", "Target Volume Path")
dbutils.widgets.text("tracking_catalog", "brickwell_health", "Tracking Table Catalog")
dbutils.widgets.text("tracking_schema", "_meta", "Tracking Table Schema")
dbutils.widgets.dropdown("restart", "false", ["false", "true"], "Restart from Beginning")
dbutils.widgets.text("month_limit", "", "Optional: Process only this specific month (YYYY-MM)")
dbutils.widgets.dropdown("max_workers", "4", ["1", "2", "4", "8", "16"],
                         "Max Parallel Workers (1=sequential)")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry Run")

# COMMAND ----------

# DBTITLE 1,Get Parameters
source_volume_path = dbutils.widgets.get("source_volume_path")
target_volume_path = dbutils.widgets.get("target_volume_path")
tracking_catalog = dbutils.widgets.get("tracking_catalog")
tracking_schema = dbutils.widgets.get("tracking_schema")
restart = dbutils.widgets.get("restart").lower() == "true"
month_limit = dbutils.widgets.get("month_limit").strip()
max_workers = int(dbutils.widgets.get("max_workers"))
dry_run = dbutils.widgets.get("dry_run").lower() == "true"

print(f"Configuration:")
print(f"  Source Volume: {source_volume_path}")
print(f"  Target Volume: {target_volume_path}")
print(f"  Tracking Catalog: {tracking_catalog}")
print(f"  Tracking Schema: {tracking_schema}")
print(f"  Restart: {restart}")
print(f"  Month Limit: {month_limit if month_limit else 'None (process next available)'}")
print(f"  Max Workers: {max_workers} {'(sequential)' if max_workers == 1 else '(parallel)'}")
print(f"  Dry Run: {dry_run}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Import Libraries and Setup

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, IntegerType
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tracking Table Management

# COMMAND ----------

# DBTITLE 1,Define Tracking Table Schema
tracking_table = f"{tracking_catalog}.{tracking_schema}.file_transfer_tracker"

table_schema = StructType([
    StructField("simulation_month", StringType(), False),
    StructField("total_tables_found", IntegerType(), False),
    StructField("tables_with_files", IntegerType(), False),
    StructField("tables_skipped", IntegerType(), False),
    StructField("total_files_transferred", IntegerType(), False),
    StructField("total_bytes_transferred", LongType(), False),
    StructField("last_updated", TimestampType(), False),
    StructField("status", StringType(), False),
])

# COMMAND ----------

def cleanup_target_volume(target_path: str):
    """Delete all subdirectories and files from the target volume."""
    try:
        print(f"Cleaning up target volume: {target_path}")
        items = dbutils.fs.ls(target_path)

        if not items:
            print("  Target volume is already empty")
            return

        deleted_count = 0
        for item in items:
            if item.name.startswith("."):
                continue
            try:
                dbutils.fs.rm(item.path, recurse=True)
                print(f"  Deleted: {item.name}")
                deleted_count += 1
            except Exception as e:
                print(f"  Failed to delete {item.name}: {e}")

        print(f"Cleanup complete: {deleted_count} item(s) deleted")

    except Exception as e:
        if "FileNotFoundException" in str(e) or "does not exist" in str(e).lower():
            print(f"  Target volume does not exist yet or is empty")
        else:
            raise

# COMMAND ----------

# DBTITLE 1,Initialize Tracking Table
def initialize_tracking_table(recreate: bool = False):
    """Initialize or recreate the tracking table."""
    if recreate:
        print(f"Dropping existing tracking table: {tracking_table}")
        spark.sql(f"DROP TABLE IF EXISTS {tracking_table}")
        cleanup_target_volume(target_volume_path)

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {tracking_table} (
        simulation_month STRING NOT NULL,
        total_tables_found INT NOT NULL,
        tables_with_files INT NOT NULL,
        tables_skipped INT NOT NULL,
        total_files_transferred INT NOT NULL,
        total_bytes_transferred BIGINT NOT NULL,
        last_updated TIMESTAMP NOT NULL,
        status STRING NOT NULL,
        CONSTRAINT pk_simulation_month PRIMARY KEY (simulation_month)
    )
    USING DELTA
    COMMENT 'Tracks incremental file transfer progress by simulation month'
    """

    spark.sql(create_sql)
    print(f"Tracking table ready: {tracking_table}")


initialize_tracking_table(recreate=restart)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Table Discovery and Month Detection

# COMMAND ----------

# DBTITLE 1,Discover Schema/Table Paths
def get_table_paths(base_path: str) -> List[Dict[str, str]]:
    """
    Discover all schema/table directories in the source volume.

    Returns:
        List of dicts with 'schema', 'table', 'path' keys.
    """
    tables = []
    try:
        schemas = dbutils.fs.ls(base_path)
        for schema_dir in schemas:
            if not schema_dir.isDir() or schema_dir.name.startswith("."):
                continue
            schema_name = schema_dir.name.rstrip("/")
            try:
                table_dirs = dbutils.fs.ls(schema_dir.path)
                for table_dir in table_dirs:
                    if not table_dir.isDir() or table_dir.name.startswith("."):
                        continue
                    table_name = table_dir.name.rstrip("/")
                    tables.append({
                        "schema": schema_name,
                        "table": table_name,
                        "path": table_dir.path,
                    })
            except Exception as e:
                print(f"  Warning: Error listing tables in {schema_name}: {e}")
    except Exception as e:
        print(f"Error listing source volume: {e}")

    return sorted(tables, key=lambda t: f"{t['schema']}/{t['table']}")

# COMMAND ----------

# DBTITLE 1,Extract Months from Table Partitions
MONTH_PATTERN = re.compile(r"extraction_month=(\d{4}-\d{2})")


def extract_months_from_table(table_path: str) -> List[str]:
    """
    Extract all available months from partition folders in a table directory.

    Returns:
        Sorted list of month strings (YYYY-MM).
    """
    months = []
    try:
        items = dbutils.fs.ls(table_path)
        for item in items:
            match = MONTH_PATTERN.search(item.name)
            if match:
                months.append(match.group(1))
    except Exception as e:
        pass
    return sorted(months)

# COMMAND ----------

# DBTITLE 1,Get Files for Specific Month
def get_files_for_month(table_path: str, target_month: str) -> List[Tuple[str, int]]:
    """
    Get all files inside a specific extraction_month partition.

    Returns:
        List of (file_path, size_bytes) tuples.
    """
    files = []
    partition_path = f"{table_path}extraction_month={target_month}"

    try:
        items = dbutils.fs.ls(partition_path)
        for item in items:
            if item.isFile() and not item.name.startswith("_"):
                clean_path = item.path.replace("dbfs:", "") if item.path.startswith("dbfs:") else item.path
                files.append((clean_path, item.size))
    except Exception as e:
        pass

    return files

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Transfer Logic

# COMMAND ----------

# DBTITLE 1,Get Last Processed Month
def get_last_processed_month() -> Optional[str]:
    """Get the last processed simulation month from tracking table."""
    try:
        result = spark.sql(f"""
            SELECT MAX(simulation_month) as last_month
            FROM {tracking_table}
        """).collect()

        if result and result[0]["last_month"]:
            return result[0]["last_month"]
        return None
    except Exception:
        return None

# COMMAND ----------

# DBTITLE 1,Get Next Simulation Month
def get_next_simulation_month(last_processed: Optional[str]) -> Optional[str]:
    """
    Determine the next month to process across all tables.

    Scans all tables for available months, returns the next one after last_processed.
    """
    table_paths = get_table_paths(source_volume_path)

    if not table_paths:
        print("No tables found in source volume")
        return None

    all_months = set()
    for tp in table_paths:
        months = extract_months_from_table(tp["path"])
        all_months.update(months)

    sorted_months = sorted(all_months)

    if not sorted_months:
        print("No extraction_month partitions found in any table")
        return None

    if last_processed is None:
        return sorted_months[0]

    next_months = [m for m in sorted_months if m > last_processed]
    return next_months[0] if next_months else None

# COMMAND ----------

# DBTITLE 1,Transfer Files for a Table/Month
def transfer_files_for_table(
    schema_name: str,
    table_name: str,
    table_path: str,
    target_month: str,
    files: List[Tuple[str, int]],
) -> Dict:
    """Transfer all files for a specific month from one table."""
    stats = {"transferred": 0, "failed": 0, "bytes": 0, "errors": []}

    if not files:
        return stats

    target_base = f"{target_volume_path}/{schema_name}/{table_name}/extraction_month={target_month}"

    for file_path, file_size in files:
        try:
            file_name = file_path.split("/")[-1]
            target_path = f"{target_base}/{file_name}"
            dbutils.fs.cp(file_path, target_path, recurse=False)
            stats["transferred"] += 1
            stats["bytes"] += file_size
        except Exception as e:
            stats["failed"] += 1
            stats["errors"].append(f"{file_path}: {str(e)}")
            print(f"    Failed: {file_path} - {e}")

    return stats

# COMMAND ----------

# DBTITLE 1,Update Tracking Table
def update_tracking(simulation_month: str, month_stats: Dict):
    """Update tracking table with results for a simulation month."""
    new_record = spark.createDataFrame([
        (
            simulation_month,
            month_stats["total_tables_found"],
            month_stats["tables_with_files"],
            month_stats["tables_skipped"],
            month_stats["total_files_transferred"],
            month_stats["total_bytes_transferred"],
            datetime.now(),
            month_stats["status"],
        )
    ], table_schema)

    new_record.write.format("delta").mode("append").saveAsTable(tracking_table)

    print(f"\nTracking Updated:")
    print(f"   Simulation Month: {simulation_month}")
    print(f"   Tables with files: {month_stats['tables_with_files']}/{month_stats['total_tables_found']}")
    print(f"   Tables skipped: {month_stats['tables_skipped']}")
    print(f"   Total files: {month_stats['total_files_transferred']}")
    print(f"   Total bytes: {month_stats['total_bytes_transferred']:,}")
    print(f"   Status: {month_stats['status']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Main Processing Loop

# COMMAND ----------

# DBTITLE 1,Process Single Table for Month
def process_single_table_for_month(
    table_info: Dict[str, str],
    target_month: str,
) -> Dict:
    """
    Process a single table for a specific simulation month.
    Thread-safe for parallel execution.
    """
    schema_name = table_info["schema"]
    table_name = table_info["table"]
    table_path = table_info["path"]
    label = f"{schema_name}/{table_name}"

    result = {
        "label": label,
        "has_files": False,
        "files_transferred": 0,
        "bytes_transferred": 0,
        "errors": [],
    }

    try:
        available_months = extract_months_from_table(table_path)

        if target_month not in available_months:
            print(f"  SKIP {label}: no data for {target_month}")
            return result

        files = get_files_for_month(table_path, target_month)

        if not files:
            print(f"  SKIP {label}: no files found")
            return result

        result["has_files"] = True
        print(f"  {label}: {len(files)} file(s) found")

        transfer_stats = transfer_files_for_table(
            schema_name, table_name, table_path, target_month, files
        )

        result["files_transferred"] = transfer_stats["transferred"]
        result["bytes_transferred"] = transfer_stats["bytes"]
        result["errors"] = transfer_stats["errors"]

        print(f"  OK {label}: {transfer_stats['transferred']} file(s) transferred ({transfer_stats['bytes']:,} bytes)")

    except Exception as e:
        error_msg = f"{label}: Unexpected error - {str(e)}"
        result["errors"].append(error_msg)
        print(f"  ERROR {error_msg}")

    return result

# COMMAND ----------

# DBTITLE 1,Process All Tables for Simulation Month
def process_all_tables(specific_month: Optional[str] = None):
    """
    Main processing function — processes all tables for the same simulation month.

    All tables are synchronized to process the same month on each run.
    Tables without data for that month are skipped.
    """
    print("=" * 80)
    print("Starting Incremental File Transfer (Global Simulation Month)")
    print("=" * 80)

    # 1. Discover all schema/table paths
    table_paths = get_table_paths(source_volume_path)

    if not table_paths:
        print("No tables found in source volume!")
        return

    print(f"\nFound {len(table_paths)} tables across schemas")

    # 2. Determine which month to process
    if specific_month:
        target_month = specific_month
        print(f"User-specified simulation month: {target_month}")
    else:
        last_processed = get_last_processed_month()

        if last_processed:
            print(f"Last processed simulation month: {last_processed}")
        else:
            print(f"First run - no previous months processed")

        target_month = get_next_simulation_month(last_processed)

    if target_month is None:
        print("\nAll simulation months have been processed!")
        print("=" * 80)
        return

    print(f"\n{'='*80}")
    print(f"Processing Simulation Month: {target_month}")
    print(f"{'='*80}\n")

    # 3. Initialize statistics
    month_stats = {
        "total_tables_found": len(table_paths),
        "tables_with_files": 0,
        "tables_skipped": 0,
        "total_files_transferred": 0,
        "total_bytes_transferred": 0,
        "errors": [],
    }

    # 4. Process tables (sequential or parallel)
    if max_workers == 1:
        print(f"  Processing tables sequentially...")
        for table_info in table_paths:
            result = process_single_table_for_month(table_info, target_month)
            if result["has_files"]:
                month_stats["tables_with_files"] += 1
                month_stats["total_files_transferred"] += result["files_transferred"]
                month_stats["total_bytes_transferred"] += result["bytes_transferred"]
            else:
                month_stats["tables_skipped"] += 1
            if result["errors"]:
                month_stats["errors"].extend(result["errors"])
    else:
        print(f"  Processing tables in parallel (max {max_workers} workers)...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_table = {
                executor.submit(process_single_table_for_month, tp, target_month): tp["schema"] + "/" + tp["table"]
                for tp in table_paths
            }
            for future in as_completed(future_to_table):
                label = future_to_table[future]
                try:
                    result = future.result()
                    if result["has_files"]:
                        month_stats["tables_with_files"] += 1
                        month_stats["total_files_transferred"] += result["files_transferred"]
                        month_stats["total_bytes_transferred"] += result["bytes_transferred"]
                    else:
                        month_stats["tables_skipped"] += 1
                    if result["errors"]:
                        month_stats["errors"].extend(result["errors"])
                except Exception as e:
                    print(f"  Exception processing {label}: {e}")
                    month_stats["tables_skipped"] += 1
                    month_stats["errors"].append(f"{label}: {str(e)}")

    # 5. Determine status
    if month_stats["errors"]:
        status = "partial" if month_stats["total_files_transferred"] > 0 else "failed"
    else:
        status = "success"

    month_stats["status"] = status

    # 6. Update tracking
    update_tracking(target_month, month_stats)

    # 7. Summary
    print("\n" + "=" * 80)
    print("Simulation Month Summary")
    print("=" * 80)
    print(f"Simulation Month: {target_month}")
    print(f"Tables with files: {month_stats['tables_with_files']}")
    print(f"Tables skipped: {month_stats['tables_skipped']}")
    print(f"Total files transferred: {month_stats['total_files_transferred']}")
    print(f"Total bytes transferred: {month_stats['total_bytes_transferred']:,}")
    print(f"Status: {status}")

    if month_stats["errors"]:
        print(f"\nErrors encountered: {len(month_stats['errors'])}")
        for error in month_stats["errors"][:10]:
            print(f"  - {error}")
        if len(month_stats["errors"]) > 10:
            print(f"  ... and {len(month_stats['errors']) - 10} more")
    else:
        print("\nAll transfers completed successfully!")

    print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Execute Transfer

# COMMAND ----------

# DBTITLE 1,Run Transfer
if not dry_run:
    process_all_tables(specific_month=month_limit if month_limit else None)
else:
    print("Dry run - no files will be transferred")
    print(f"\nSource volume contents:")
    table_paths = get_table_paths(source_volume_path)
    for tp in table_paths:
        months = extract_months_from_table(tp["path"])
        print(f"  {tp['schema']}/{tp['table']}: {len(months)} months ({months[0]}..{months[-1]})" if months else f"  {tp['schema']}/{tp['table']}: empty")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. View Tracking Table

# COMMAND ----------

# DBTITLE 1,Show Recent Transfer History
display(spark.sql(f"""
    SELECT
        simulation_month,
        total_tables_found,
        tables_with_files,
        tables_skipped,
        total_files_transferred,
        ROUND(total_bytes_transferred / 1024 / 1024, 2) as mb_transferred,
        last_updated,
        status
    FROM {tracking_table}
    ORDER BY simulation_month DESC
    LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Utility Queries

# COMMAND ----------

# DBTITLE 1,Progress Overview
display(
    spark.sql(f"""
    SELECT
        COUNT(*) as months_processed,
        MIN(simulation_month) as first_month,
        MAX(simulation_month) as latest_month,
        SUM(total_files_transferred) as total_files,
        ROUND(SUM(total_bytes_transferred) / 1024 / 1024, 2) as total_mb,
        AVG(tables_with_files) as avg_tables_with_files,
        AVG(tables_skipped) as avg_tables_skipped,
        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
        SUM(CASE WHEN status = 'partial' THEN 1 ELSE 0 END) as partial_runs,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs
    FROM {tracking_table}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Manual Reset (Optional)
# MAGIC
# MAGIC Uncomment and run to reset tracking for a specific month:

# COMMAND ----------

# DBTITLE 1,Reset Specific Month (Commented Out)
# month_to_reset = "2024-01"
# spark.sql(f"DELETE FROM {tracking_table} WHERE simulation_month = '{month_to_reset}'")
# print(f"Reset tracking for {month_to_reset}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Usage Notes
# MAGIC
# MAGIC ### Global Simulation Month Synchronization
# MAGIC All tables process the **same simulation month** on each run:
# MAGIC - **Run 1**: All tables process 2020-01
# MAGIC - **Run 2**: All tables process 2020-02
# MAGIC - Tables without data for a given month are skipped (not an error)
# MAGIC
# MAGIC ### First Run
# MAGIC 1. Set `source_volume_path` and `target_volume_path`
# MAGIC 2. Set `restart=true` to create tracking table and clean target volume
# MAGIC 3. Set `dry_run=false`
# MAGIC 4. Set `max_workers` (4 recommended)
# MAGIC 5. Run notebook — processes first month across all tables
# MAGIC
# MAGIC ### Subsequent Runs
# MAGIC 1. Set `restart=false` (default)
# MAGIC 2. Run notebook — processes next simulation month
# MAGIC 3. Repeat until all months are processed
# MAGIC
# MAGIC ### Process Specific Month
# MAGIC 1. Set `month_limit` to a specific month (e.g., `2024-01`)
# MAGIC 2. Run notebook — processes only that month
# MAGIC
# MAGIC ### Parallel Processing
# MAGIC - **max_workers=1**: Sequential (safest, for debugging)
# MAGIC - **max_workers=4**: Recommended for most cases
# MAGIC - **max_workers=8+**: Aggressive (use with many tables)
