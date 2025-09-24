"""Utilities for configuring and orchestrating Delta Live Tables ingestion."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import dlt
import pyspark.sql.functions as F
from databricks.sdk.runtime import spark

from dbx_toolkit.dlt_flows import (
    bootstrap_bronze_flow,
    bootstrap_incremental_flow,
    bootstrap_scd1_flow,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from pyspark.sql import DataFrame


def _normalize_cluster_by(cluster_by: str | Sequence[str] | None) -> list[str]:
    """Normalise cluster-by input to a list of column names."""
    if cluster_by is None:
        return []
    if isinstance(cluster_by, str):
        return [cluster_by]
    return list(cluster_by)


class IngestionConfig:
    """Configuration helper for batch ingestion orchestration.

    Captures ingestion defaults sourced from Spark configuration when available.

    Parameters
    ----------
    source_name : str | None, optional
        Source system name, by default None
    schema_name : str, optional
        Target schema name, by default "aldm_staging"
    cluster_by : Sequence[str] | None, optional
        Column names to cluster by, by default None
    config_table : str, optional
        Configuration table name, by default "aldm_oper.landing_load_config"
    """

    def __init__(
        self,
        source_name: str | None = None,
        schema_name: str = "aldm_staging",
        cluster_by: Sequence[str] | None = None,
        config_table: str = "aldm_oper.landing_load_config",
    ) -> None:
        inferred_source = source_name
        if inferred_source is None and spark is not None:
            try:
                inferred_source = spark.conf.get("source_name")
            except Exception:  # pragma: no cover - defensive
                inferred_source = None

        self.source_name = inferred_source
        self.schema_name = schema_name
        self.cluster_by = _normalize_cluster_by(cluster_by) or ["part_col"]
        self.config_table = config_table

    def get_config_data(self, table_filter: Sequence[str] | None = None) -> list[Any]:
        """Collect configuration rows from the landing load config table."""
        query = spark.table(self.config_table).where(F.col("skip_table") == "N")

        if self.source_name:
            query = query.where(F.col("source_system") == self.source_name)

        if table_filter:
            query = query.where(F.col("table_name").isin(list(table_filter)))

        return list(query.collect())


class AutoLoaderIngestion:
    """Auto Loader helpers for Delta Live Tables pipelines."""

    @staticmethod
    def create_auto_loader_bronze_table(  # noqa: PLR0913
        table_inc_name: str,
        schema_definition: str,
        file_dir: str,
        p_delimiter: str = ",",
        header: bool = True,
        file_extension: str = "*.csv",
        sort_struct_list: Sequence[str] | None = None,
        cluster_by: str | Sequence[str] | None = "part_col",
        max_file_age: str = "14 days",
    ) -> None:
        """Create an Auto Loader bronze table using DLT."""
        cluster_columns = _normalize_cluster_by(cluster_by)
        table_properties = {
            "quality": "bronze",
            "delta.enableDeletionVectors": "true",
        }
        if cluster_columns:
            table_properties["delta.dataSkippingStatsColumns"] = ",".join(cluster_columns)

        @dlt.table(
            name=table_inc_name,
            cluster_by=cluster_columns or None,
            table_properties=table_properties,
        )
        def auto_loader_bronze_table() -> DataFrame:
            """Auto Loader bronze table."""
            df = (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("cloudFiles.maxFileAge", max_file_age)
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option("cloudFiles.schemaHints", schema_definition)
                .option("header", header)
                .option("rescuedDataColumn", "_rescued_data")
                .option("nullValue", "NULL")
                .option("delimiter", p_delimiter)
                .option("pathGlobfilter", file_extension)
                .load(file_dir)
            )

            df_bronze = (
                df.withColumn("insert_ts", F.current_timestamp())
                .withColumn("part_col", F.current_date())
                .select("*", F.col("_metadata.file_path").alias("file_nm"))
            )

            if sort_struct_list:
                return df_bronze.withColumn(
                    "sort_struct", F.struct([F.col(col) for col in sort_struct_list])
                )

            return df_bronze.withColumn("sort_struct", F.current_date())

    @staticmethod
    def create_auto_loader_append_flow(  # noqa: PLR0913
        table_inc_name: str,
        schema_definition: str,
        file_dir: str,
        p_delimiter: str = ",",
        header: bool = True,
        file_extension: str = "*.csv",
        sort_struct_list: Sequence[str] | None = None,
        max_file_age: str = "14 days",
    ) -> None:
        """Create an Auto Loader append flow using DLT."""

        @dlt.append_flow(
            name=f"auto-loader_{table_inc_name}",
            target=table_inc_name,
        )
        def auto_loader_append_flow() -> DataFrame:
            """Auto Loader append flow."""
            df = (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", "csv")
                .option("cloudFiles.maxFileAge", max_file_age)
                .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
                .option("cloudFiles.schemaHints", schema_definition)
                .option("header", header)
                .option("rescuedDataColumn", "_rescued_data")
                .option("nullValue", "NULL")
                .option("delimiter", p_delimiter)
                .option("pathGlobfilter", file_extension)
                .load(file_dir)
            )

            df_bronze = (
                df.withColumn("insert_ts", F.current_timestamp())
                .withColumn("part_col", F.current_date())
                .select("*", F.col("_metadata.file_path").alias("file_nm"))
            )

            if sort_struct_list:
                return df_bronze.withColumn(
                    "sort_struct", F.struct([F.col(col) for col in sort_struct_list])
                )

            return df_bronze.withColumn("sort_struct", F.current_date())


class BatchIngestion:
    """Helper for orchestrating batch ingestion patterns across many tables."""

    @staticmethod
    def create_parameterized_ingestion_pipeline(  # noqa: PLR0913
        config: IngestionConfig,
        table_filter: Sequence[str] | None = None,
        use_historic_bronze: bool = True,
        use_historic_silver: bool = True,
        silver_keys: Sequence[str] | None = None,
        sequence_by_col: str = "sort_struct",
    ) -> None:
        """Create a parameterised ingestion pipeline driven by configuration tables."""
        data_collect = config.get_config_data(table_filter)

        for row in data_collect:
            table_name = row.table_name
            table_inc_name = getattr(row, "inc_table_name", table_name)

            if use_historic_bronze:
                bootstrap_bronze_flow(
                    schema_name=config.schema_name,
                    table_name=table_inc_name,
                    cluster_by=config.cluster_by,
                )

            if use_historic_silver and silver_keys:
                bootstrap_scd1_flow(
                    schema_name=config.schema_name,
                    table_name=table_name,
                    keys=list(silver_keys),
                    sequence_by=sequence_by_col,
                    create_target=False,
                )

    @staticmethod
    def create_incremental_append_pipeline(
        config: IngestionConfig,
        table_filter: Sequence[str] | None = None,
        skip_change_commits: bool = True,
    ) -> None:
        """Create append-only incremental ingestion flows without bronze bootstrap."""
        data_collect = config.get_config_data(table_filter)

        for row in data_collect:
            table_name = row.table_name
            table_inc_name = getattr(row, "inc_table_name", table_name)
            bootstrap_incremental_flow(
                schema_name=config.schema_name,
                table_name=table_inc_name,
                skip_change_commits=skip_change_commits,
            )


def create_environment_specific_pipeline(
    environment: str,
    table_filter: Sequence[str] | None = None,
    source_name: str | None = None,
) -> None:
    """Create an environment-specific pipeline (DEV/PRD)."""
    env_table_filters = {
        "DEV": ["TBAP_ITEM"],
        "PRD": ["MEMO"],
    }

    effective_filter = (
        list(table_filter) if table_filter else env_table_filters.get(environment.upper())
    )

    config = IngestionConfig(source_name=source_name)

    BatchIngestion.create_parameterized_ingestion_pipeline(
        config=config,
        table_filter=effective_filter,
        use_historic_bronze=True,
        use_historic_silver=False,
    )


def get_table_list_from_config(
    config_table: str = "aldm_oper.landing_load_config",
    source_name: str | None = None,
    skip_table: str = "N",
) -> tuple[list[str], list[str]]:
    """Return table names and incremental table names from configuration."""
    query = spark.table(config_table).where(F.col("skip_table") == skip_table)

    if source_name:
        query = query.where(F.col("source_system") == source_name)

    df_tables = query.collect()

    table_names = [row.table_name for row in df_tables]
    table_inc_names = [getattr(row, "inc_table_name", row.table_name) for row in df_tables]

    return table_names, table_inc_names
