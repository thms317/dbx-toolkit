# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion of hive_metastore tables

# COMMAND ----------

from dbx_toolkit.dlt_flows import (
    bootstrap_bronze_flow,
    bootstrap_scd1_flow,
    format_cluster_columns,
)
from dbx_toolkit.ingestion import BatchIngestion, IngestionConfig

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Setup

# COMMAND ----------

TARGET_SCHEMA = "target_schema"
CLUSTER_BY = "part_col"

cluster_by_columns = [col.strip() for col in CLUSTER_BY.split(",") if col.strip()]

config = IngestionConfig(
    schema_name=TARGET_SCHEMA,
    cluster_by=cluster_by_columns or None,
)

print(f"Source name: {config.source_name}")
print(f"Schema name: {config.schema_name}")
print(f"Cluster by: {config.cluster_by}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Configuration Data

# COMMAND ----------

data_collect = config.get_config_data()

print(f"Found {len(data_collect)} tables to process:")
for row in data_collect:
    table_name = row.table_name
    table_inc_name = getattr(row, "inc_table_name", table_name)
    print(f"  - {table_name} -> {table_inc_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 1: Use Modularized Functions Directly

# COMMAND ----------

for row in data_collect:
    table_name = row.table_name
    table_inc_name = getattr(row, "inc_table_name", table_name)

    print(f"Setting up ingestion for: {table_name}")

    # Bootstrap bronze table and append flow
    bootstrap_bronze_flow(
        schema_name=config.schema_name,
        table_name=table_inc_name,
        cluster_by=config.cluster_by,
    )

    # If you need silver layer processing, uncomment below:
    # Example keys - adjust based on your table structure
    # primary_keys = ["id"]  # Replace with actual primary key columns
    # bootstrap_scd1_flow(
    #     schema_name=config.schema_name,
    #     table_name=table_name,
    #     keys=primary_keys,
    #     sequence_by="updated_date",  # Replace with actual sequence column
    #     create_target=False,
    # )

print("DLT flows created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 2: Use the Batch Ingestion Class

# COMMAND ----------

BatchIngestion.create_parameterized_ingestion_pipeline(
    config=config,
    table_filter=None,  # Process all tables from config
    use_historic_bronze=True,
    use_historic_silver=False,  # Set to True if you need silver layer
    silver_keys=None,  # Specify keys if using silver layer
    sequence_by_col="sort_struct",
)

print("Batch ingestion pipeline created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions Available

# COMMAND ----------

# Show available utility functions
print("Available utility functions:")
print(f"- format_cluster_columns: {format_cluster_columns.__doc__}")
print(f"- bootstrap_bronze_flow: {bootstrap_bronze_flow.__doc__}")
print(f"- bootstrap_scd1_flow: {bootstrap_scd1_flow.__doc__}")

# Example of using format_cluster_columns
cluster_columns = format_cluster_columns(["col1", "col2", "col3"])
print(f"\nFormatted cluster columns: {cluster_columns}")
