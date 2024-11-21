# Databricks notebook source
# MAGIC %pip install --no-deps --force-reinstall {spark.conf.get("wheel_path")}

# COMMAND ----------

# MAGIC %md
# MAGIC # Data ingestion with Delta Live Tables
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load libraries and functions
# MAGIC

# COMMAND ----------

import dlt
from databricks.sdk.runtime import spark
from pyspark.sql import functions as F

from dbx_toolkit.main import main

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set parameters
# MAGIC

# COMMAND ----------

table = spark.conf.get("table")
autoloader_path = spark.conf.get("autoloader_path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest data with the Auto Loader
# MAGIC Only for demonstration purposes, we made the ingestion dependent on the `main` function of our wheel.

# COMMAND ----------

if main():

    @dlt.table(
        name=f"raw_{table}",
        temporary=False,
        table_properties={"quality": "raw"},
        comment="Data loaded from Landing Zone",
    )
    def ingest() -> dlt.DataFrame:
        """Ingest raw data using the Auto Loader."""
        return (
            spark.readStream.format("delta")
            .load(autoloader_path)
            .withColumn("load_date", F.current_timestamp())
            .select("*", "_metadata")
        )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Set [expectations](https://learn.microsoft.com/en-us/azure/databricks/delta-live-tables/expectations) to ensure bronze data quality
# MAGIC

# COMMAND ----------

expectations: dict[str, str] = {
    "valid_trip_distance": "trip_distance > 0",
    "valid_fare_amount": "fare_amount > 0",
}

# COMMAND ----------


@dlt.table(
    name=f"bronze_{table}",
    table_properties={"quality": "bronze"},
    comment="Data transformed from raw",
)
@dlt.expect_all_or_drop(expectations)
def set_expectations() -> dlt.DataFrame:
    """Set expectations on raw data for bronze quality."""
    return dlt.read_stream(f"raw_{table}")
