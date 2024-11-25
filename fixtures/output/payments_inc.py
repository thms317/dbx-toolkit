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

dlt.create_streaming_table(
    name="payments_inc", comment="Clean, merged data", table_properties={"quality": "silver"}
)


@dlt.table(name="payments_inc")
def dynamic_flow_payments_inc() -> dlt.DataFrame:
    """Dynamic flow for the payments_inc table."""
    return spark.readStream.table("hive_metastore.aldm_staging.payments_inc")
