# Databricks notebook source
# MAGIC %md
# MAGIC # File Size Diagnostics

# COMMAND ----------

from dbx_toolkit.diagnostics import TableDiagnostics, analyze_multiple_tables, analyze_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter Setup

# COMMAND ----------

# MAGIC %md Set the table to analyse (catalog.schema.table or hive_metastore.schema.table).

# COMMAND ----------

catalog_name = "hive_metastore"
schema_name = "schema_name"
table_name = "table_name"

full_table_name = f"{catalog_name}.{schema_name}.__apply_changes_storage_table_{table_name}"

print(f"Analysing table: {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Analysis Helper

# COMMAND ----------

single_result = analyze_table(full_table_name, print_results=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detailed Class-Based Diagnostics

# COMMAND ----------

diagnostics = TableDiagnostics(table_name)

spark_stats = diagnostics.get_file_size_statistics()
print("File size statistics:")
for key, value in spark_stats.items():
    if key == "descriptive_stats":
        continue
    print(f"  {key}: {value}")

descriptive_stats = spark_stats.get("descriptive_stats", {})
if descriptive_stats:
    print("\nDescriptive statistics:")
    for metric, value in descriptive_stats.items():
        print(f"  {metric}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## File Size Distribution

# COMMAND ----------

distribution_df = diagnostics.get_file_size_distribution()
distribution_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyse Multiple Tables at Once

# COMMAND ----------

example_tables = [
    table_name,
    "PRD_ALDM.ALDM_STAGING.TABLE_ACT_ENTRY",
]

multi_results = analyze_multiple_tables(example_tables, print_results=False)
for tbl, result in multi_results.items():
    print(f"\n{tbl}")
    for key, value in result.items():
        print(f"  {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect Delta Log Location

# COMMAND ----------

print(f"Table location: {diagnostics.table_location}")
