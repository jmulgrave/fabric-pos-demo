# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a6310158-d270-4ca9-9de4-ec6285ceccb4",
# META       "default_lakehouse_name": "bronze_lakehouse",
# META       "default_lakehouse_workspace_id": "3deade31-2f59-49b7-88b1-9b104ce28d0a",
# META       "known_lakehouses": [
# META         {
# META           "id": "a6310158-d270-4ca9-9de4-ec6285ceccb4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ══════════════════════════════════════════════════════════════════════════
# CONFIGURATION — Update these values
# ══════════════════════════════════════════════════════════════════════════
# OneLake SAS (reuse from your working code)
WORKSPACE_ID = "3deade31-2f59-49b7-88b1-9b104ce28d0a"  # GUID of the workspace
LAKEHOUSE_ID = "a6310158-d270-4ca9-9de4-ec6285ceccb4"       # GUID of the lakehouse
PARQUET_FILES_PATH = "Files/parquet-samples/"
CSV_FILES_PATH = "Files/csv-samples/"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ══════════════════════════════════════════════════════════════════════════
# List parquet files
# ══════════════════════════════════════════════════════════════════════════

files = notebookutils.fs.ls(
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/{PARQUET_FILES_PATH}"
)
parquet_files = [f for f in files if f.name.endswith(".parquet")]
print(f"Found {len(parquet_files)} parquet files\n")

for f in parquet_files:
    # Derive table name from file name (strip .parquet extension)
   print(f.name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ══════════════════════════════════════════════════════════════════════════
# Load parquet files into Delta tables
#   - Table name is the file name without the .parquet extension
#   - Added defensive handling for unsupported Parquet types (e.g. TIMESTAMP(NANOS))
#     so that one bad file does not fail the whole loop.
# ══════════════════════════════════════════════════════════════════════════

failed_files = []  # track files that cannot be read by Spark

for f in parquet_files:
    # Derive table name from file name (strip .parquet extension)
    table_name = f.name.rsplit(".", 1)[0]

    # Build full OneLake path for the parquet file
    file_path = (
        f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
        f"{LAKEHOUSE_ID}/{PARQUET_FILES_PATH}{f.name}"
    )

    try:
        # Read parquet file
        df = spark.read.parquet(file_path)
    except Exception as e:
        # Common cause here is unsupported Parquet logical type
        print(f"[SKIP] Could not read '{file_path}': {e}")
        failed_files.append({"file": file_path, "error": str(e)})
        continue  # move on to the next file

    # Write as Delta table into the default Lakehouse, overwriting if it exists
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

    print(f"Loaded '{file_path}' into Delta table '{table_name}' .....")

if failed_files:
    print("\nThe following parquet files were skipped due to read errors:")
    for info in failed_files:
        print(f" - {info['file']} | error: {info['error']}")
else:
    print("\nAll parquet files were loaded successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ══════════════════════════════════════════════════════════════════════════
# List csv files
# ══════════════════════════════════════════════════════════════════════════

files = notebookutils.fs.ls(
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}/{CSV_FILES_PATH}"
)
csv_files = [f for f in files if f.name.endswith(".csv")]
print(f"Found {len(csv_files)} csv files\n")

for f in csv_files:
    # Derive table name from file name (strip .csv extension)
   print(f.name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ══════════════════════════════════════════════════════════════════════════
# Load CSV files into Delta tables
#   - Table name is the file name without the .csv extension
#   - Assumes the first row of each CSV file contains column headers
# ══════════════════════════════════════════════════════════════════════════

failed_files = []  # track files that cannot be read by Spark

for f in csv_files:
    # Derive table name from file name (strip .csv extension)
    table_name = f.name.rsplit(".", 1)[0]

    # Build full OneLake path for the CSV file
    file_path = (
        f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/"
        f"{LAKEHOUSE_ID}/{CSV_FILES_PATH}{f.name}"
    )

    try:
        # Read CSV file, treating the first row as header and inferring schema
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(file_path)
        )
    except Exception as e:
        print(f"[SKIP] Could not read '{file_path}': {e}")
        failed_files.append({"file": file_path, "error": str(e)})
        continue  # move on to the next file

    # Write as Delta table into the default Lakehouse, overwriting if it exists
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

    print(f"Loaded '{file_path}' into Delta table '{table_name}' .....")

if failed_files:
    print("\nThe following csv files were skipped due to read errors:")
    for info in failed_files:
        print(f" - {info['file']} | error: {info['error']}")
else:
    print("\nAll csv files were loaded successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
