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

# COMMAND ----------


def add_flow(table_name: str) -> None:
    """Dynamiccally create flows."""
    dlt.create_streaming_table(
        name=table_name, comment="Clean, merged data", table_properties={"quality": "silver"}
    )

    @dlt.append_flow(name=f"hive_{table_name}", target=table_name)
    def bronze_table() -> dlt.DataFrame:
        """hive_metastore table."""
        return spark.readStream.table(f"hive_metastore.aldm_staging.{table_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### List tables that will be created

# COMMAND ----------


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
    add_flow(table_name)
    add_flow(table_inc_name)
