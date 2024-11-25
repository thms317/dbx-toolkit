# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Flows

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load libraries and functions
# MAGIC

# COMMAND ----------

import dlt
from databricks.sdk.runtime import spark
from pyspark.sql import Column

# COMMAND ----------


def ingest_scd1_table(
    schema_name: str,
    table_name: str,
    keys: list[str] | list[Column],
    sequence_by: str | Column,
) -> None:
    """Ingest SCD1 table from the hive_metastore."""
    # Create a target table for the historic SCD1 table and the auto-loader stream
    dlt.create_streaming_table(
        name=table_name,
        comment="Clean, merged data",
        spark_conf={"pipelines.trigger.interval": "60 seconds"},
        table_properties={"quality": "silver"},
    )

    # Create a flow to ingest the historic SCD1 table from the hive_metastore
    # @dlt.append_flow(
    #     name=f"hive_metastore_{table_name}",
    #     target=table_name,
    #     # once=True,  # best practice, but it crashes in current configuration
    # )
    @dlt.table(name=f"hive_metastore_{table_name}")
    def hive_metastore_table() -> dlt.DataFrame:
        """hive_metastore SCD1 table."""
        return spark.table(f"hive_metastore.{schema_name}.{table_name}")

    dlt.apply_changes(
        target=table_name,
        source=f"hive_metastore_{table_name}",
        keys=keys,
        sequence_by=sequence_by,
        flow_name=f"hive_metastore_{table_name}_apply_changes",
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### List tables that will be created

# COMMAND ----------

schema_name = "aldm_staging"

df_tables = spark.table("aldm_oper.landing_load_config").where(
    "source_system == 'ENSAPP2' AND skip_table == 'N' AND table_name == 'PAYMENT_ACTIVITY'"
)

table_names = df_tables.select("table_name").collect()
table_names = [x.table_name for x in table_names]

table_inc_names = df_tables.select("inc_table_name").collect()
table_inc_names = [x.inc_table_name for x in table_inc_names]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add flows

# COMMAND ----------

for table_name, table_inc_name in zip(table_names, table_inc_names, strict=False):
    ingest_historic_table(schema_name, table_inc_name)
    ingest_scd1_table(schema_name, table_name, ["payment_id"], "effective_date")


# COMMAND ----------

# Ingest historic table from the hive_metastore, and write to target table in UC
ingest_historic_table(schema_name, tableIncName)


# Write auto-loader data to the target table
# @dlt.append_flow(name=f"auto-loader_{tableIncName}", target=tableIncName)
@dlt.table(
    name=tableIncName,
    spark_conf={"pipelines.trigger.interval": "60 seconds"},
    table_properties={"quality": "bronze"},
)
def delta_live_test_inc():
    df = (
        spark.readStream.format("cloudFiles")
        # .option("cloudFiles.format", "csv")
        # .option("cloudFiles.maxFileAge", "14 days")  # Track files only for the last 7 days
        # .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # .option("cloudFiles.schemaHints", schema_definition)
        # .option("header", header)
        # .option("rescuedDataColumn", "_rescued_data")
        # .option("nullValue", "NULL")
        # .option("delimiter", p_delimiter)
        # .option("pathGlobfilter", file_extension)
        # .load(file_dir)
    )


if merge_ind == "Y":
    #     # dlt.create_streaming_table(
    #     #     name=f"{tableName}",
    #     #     comment="Clean, merged data",
    #     #     table_properties={"quality": "silver"},
    #     #     spark_conf={"pipelines.trigger.interval": "60 seconds"},
    #     # )

    # Ingest SCD1 table from the hive_metastore, and write to target table in UC to minimize reprocessing
    ingest_scd1_table(schema_name, tableName, parlist, F.col("sort_struct"))

    # # Apply changes from auto-loader data to the target table
    # dlt.apply_changes(
    #     target=tableName,
    #     source=tableIncName,
    #     keys=parlist,
    #     sequence_by=F.col("sort_struct"),
    #     flow_name=f"auto-loader_scd1_{table_name}",
    # )
