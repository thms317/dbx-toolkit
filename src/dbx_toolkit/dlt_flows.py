"""High-level helpers for building Delta Live Tables flows."""

from __future__ import annotations

from typing import TYPE_CHECKING

import dlt
from databricks.sdk.runtime import spark

if TYPE_CHECKING:
    from collections.abc import Mapping, MutableMapping, Sequence

    from pyspark.sql import Column


def _normalize_cluster_by(cluster_by: str | Sequence[str] | None) -> list[str]:
    if cluster_by is None:
        return []
    if isinstance(cluster_by, str):
        return [cluster_by]
    return list(cluster_by)


def format_cluster_columns(cluster_by: str | Sequence[str] | None) -> str:
    """Return a comma-separated string of cluster-by columns."""
    columns = _normalize_cluster_by(cluster_by)
    return ",".join(columns)


def _build_table_properties(
    quality: str,
    cluster_by: str | Sequence[str] | None,
    *,
    extra_properties: Mapping[str, str] | None = None,
) -> MutableMapping[str, str]:
    properties: MutableMapping[str, str] = {
        "quality": quality,
        "delta.enableDeletionVectors": "true",
    }

    cluster_columns = _normalize_cluster_by(cluster_by)
    if cluster_columns:
        properties["delta.dataSkippingStatsColumns"] = format_cluster_columns(cluster_columns)

    if extra_properties:
        properties.update(extra_properties)

    return properties


def bootstrap_bronze_flow(  # noqa: PLR0913
    schema_name: str,
    table_name: str,
    target_table: str | None = None,
    cluster_by: str | Sequence[str] | None = None,
    skip_change_commits: bool = True,
    table_properties: Mapping[str, str] | None = None,
) -> None:
    """Create a bronze DLT table and append flow fed from the hive_metastore."""
    target = target_table or table_name
    cluster_columns = _normalize_cluster_by(cluster_by) or ["part_col"]
    properties = _build_table_properties(
        "bronze", cluster_columns, extra_properties=table_properties
    )

    dlt.create_streaming_table(
        name=target,
        cluster_by=cluster_columns,
        table_properties=properties,
    )

    flow_name = f"hive_metastore_{target}"

    @dlt.append_flow(name=flow_name, target=target)
    def hive_metastore_bronze() -> dlt.DataFrame:
        reader = spark.readStream
        if skip_change_commits:
            reader = reader.option("skipChangeCommits", "true")
        return reader.table(f"hive_metastore.{schema_name}.{table_name}")


def bootstrap_incremental_flow(
    schema_name: str,
    table_name: str,
    target_table: str | None = None,
    skip_change_commits: bool = False,
) -> None:
    """Register an append flow that keeps an incremental table in sync with hive_metastore."""
    target = target_table or table_name
    flow_name = f"hive_metastore_{target}"

    @dlt.append_flow(name=flow_name, target=target)
    def hive_metastore_incremental() -> dlt.DataFrame:
        reader = spark.readStream
        if skip_change_commits:
            reader = reader.option("skipChangeCommits", "true")
        return reader.table(f"hive_metastore.{schema_name}.{table_name}")


def bootstrap_scd1_flow(  # noqa: PLR0913
    schema_name: str,
    table_name: str,
    keys: Sequence[str] | Sequence[Column],
    sequence_by: str | Column,
    create_target: bool = False,
    cluster_by: str | Sequence[str] | None = None,
    skip_change_commits: bool = True,
    comment: str | None = "Clean, merged data",
) -> None:
    """Create the temporary source and apply-changes flow for SCD Type 1 processing."""
    source_alias = f"hive_metastore_{table_name}"

    @dlt.table(name=source_alias, temporary=True)
    def hive_metastore_silver() -> dlt.DataFrame:
        reader = spark.readStream
        if skip_change_commits:
            reader = reader.option("skipChangeCommits", "true")
        return reader.table(f"hive_metastore.{schema_name}.{table_name}")

    if create_target:
        cluster_columns = _normalize_cluster_by(cluster_by)
        properties = _build_table_properties("silver", cluster_columns)
        dlt.create_streaming_table(
            name=table_name,
            comment=comment,
            cluster_by=cluster_columns or None,
            spark_conf={"pipelines.trigger.interval": "60 seconds"},
            table_properties=properties,
        )

    dlt.apply_changes(
        target=table_name,
        source=source_alias,
        keys=list(keys),
        sequence_by=sequence_by,
        flow_name=f"{source_alias}_apply_changes",
    )


__all__ = [
    "bootstrap_bronze_flow",
    "bootstrap_incremental_flow",
    "bootstrap_scd1_flow",
    "format_cluster_columns",
]
