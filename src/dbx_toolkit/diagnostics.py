"""Module for Databricks diagnostic tools and analysis."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as F
from databricks.sdk.runtime import spark

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class TableDiagnostics:
    """Class for analyzing Databricks table diagnostics.

    Parameters
    ----------
    table_name : str
        Full table name (e.g., "catalog.schema.table" or "hive_metastore.schema.table")
    """

    def __init__(self, table_name: str) -> None:
        self.table_name = table_name
        self._table_location: str | None = None
        self._table_details: dict[str, Any] | None = None

    @property
    def table_location(self) -> str:
        """Get the table location from DESCRIBE DETAIL."""
        if self._table_location is None:
            result = spark.sql(f"DESCRIBE DETAIL {self.table_name}").select("location").collect()
            if result:
                self._table_location = result[0]["location"]
            else:
                msg = f"Could not get location for table {self.table_name}"
                raise ValueError(msg)
        return self._table_location

    @property
    def table_details(self) -> dict[str, Any]:
        """Get full table details from DESCRIBE DETAIL."""
        if self._table_details is None:
            self._table_details = (
                spark.sql(f"DESCRIBE DETAIL {self.table_name}").collect()[0].asDict()
            )
        return self._table_details

    def get_delta_log_files(self) -> DataFrame:
        """Get DataFrame with file information from Delta log.

        Returns
        -------
        DataFrame
            DataFrame with columns: relative_path, size_in_bytes, full_path
        """
        # Read all JSON files in the _delta_log directory
        log_df = spark.read.json(f"{self.table_location}/_delta_log/*.json")

        # Filter out the 'add' actions which represent new data files
        add_files_df = log_df.filter(F.col("add").isNotNull()).select(
            F.col("add.path").alias("relative_path"), F.col("add.size").alias("size_in_bytes")
        )

        # Build the full file paths
        return add_files_df.withColumn(
            "full_path",
            F.concat(F.lit(self.table_location.rstrip("/")), F.lit("/"), F.col("relative_path")),
        )

    def get_file_size_statistics(self, use_delta_log: bool = True) -> dict[str, Any]:
        """Get comprehensive file size statistics.

        Parameters
        ----------
        use_delta_log : bool, optional
            If True, use Delta log for detailed analysis. If False, use DESCRIBE DETAIL.
            Default is True.

        Returns
        -------
        dict[str, Any]
            Dictionary containing size statistics
        """
        if use_delta_log:
            return self._get_delta_log_statistics()
        return self._get_table_detail_statistics()

    def _get_delta_log_statistics(self) -> dict[str, Any]:
        """Get statistics from Delta log analysis."""
        add_files_df = self.get_delta_log_files()

        num_files = add_files_df.count()
        if num_files == 0:
            return {
                "total_size_bytes": 0,
                "total_size_mb": 0,
                "num_files": 0,
                "average_file_size_bytes": 0,
                "average_file_size_mb": 0,
                "descriptive_stats": {},
            }

        # Calculate statistics
        total_size_row = add_files_df.agg(F.sum("size_in_bytes")).collect()[0]
        total_size_bytes = total_size_row[0] or 0
        average_file_size_bytes = total_size_bytes / num_files if num_files else 0

        # Convert to megabytes
        total_size_mb = total_size_bytes / (1024 * 1024)
        average_file_size_mb = average_file_size_bytes / (1024 * 1024)

        # Get descriptive statistics
        desc_stats = add_files_df.select("size_in_bytes").describe().collect()
        desc_dict = {}
        for row in desc_stats:
            value = row["size_in_bytes"]
            if value is not None:
                desc_dict[row["summary"]] = float(value)

        return {
            "total_size_bytes": total_size_bytes,
            "total_size_mb": total_size_mb,
            "num_files": num_files,
            "average_file_size_bytes": average_file_size_bytes,
            "average_file_size_mb": average_file_size_mb,
            "descriptive_stats": desc_dict,
        }

    def _get_table_detail_statistics(self) -> dict[str, Any]:
        """Get statistics from DESCRIBE DETAIL."""
        details = self.table_details
        total_size_bytes = details.get("sizeInBytes", 0)
        num_files = details.get("numFiles", 0)
        average_file_size_bytes = total_size_bytes / num_files if num_files else 0

        # Convert to megabytes
        total_size_mb = total_size_bytes / (1024 * 1024)
        average_file_size_mb = average_file_size_bytes / (1024 * 1024)

        return {
            "total_size_bytes": total_size_bytes,
            "total_size_mb": total_size_mb,
            "num_files": num_files,
            "average_file_size_bytes": average_file_size_bytes,
            "average_file_size_mb": average_file_size_mb,
        }

    def get_file_size_distribution(self) -> DataFrame:
        """Get file size distribution in predefined bins.

        Returns
        -------
        DataFrame
            DataFrame with columns: size_bin, count
        """
        add_files_df = self.get_delta_log_files()

        # Define size bins (e.g., <1MB, 1-10MB, 10-100MB, >100MB)
        bins = [0, 1 * 1024 * 1024, 10 * 1024 * 1024, 100 * 1024 * 1024]

        # Bucketize the file sizes
        add_files_with_bins = add_files_df.withColumn(
            "size_bin",
            F.when(F.col("size_in_bytes") < bins[1], "<1MB")
            .when(F.col("size_in_bytes") < bins[2], "1MB-10MB")
            .when(F.col("size_in_bytes") < bins[3], "10MB-100MB")
            .otherwise(">100MB"),
        )

        return add_files_with_bins.groupBy("size_bin").count()

    def print_diagnostics(self) -> None:
        """Print comprehensive diagnostics to console."""
        print(f"=== Table Diagnostics for {self.table_name} ===")

        # Basic statistics
        stats = self.get_file_size_statistics()
        print(f"Total Size: {stats['total_size_bytes']:,} bytes ({stats['total_size_mb']:.2f} MB)")
        print(f"Number of Files: {stats['num_files']:,}")
        print(
            f"Average File Size: {stats['average_file_size_bytes']:.2f} bytes "
            f"({stats['average_file_size_mb']:.2f} MB)"
        )

        # Descriptive statistics if available
        if "descriptive_stats" in stats:
            print("\n--- Descriptive Statistics ---")
            desc_stats = stats["descriptive_stats"]
            for stat, value in desc_stats.items():
                if stat in ["min", "max", "mean", "stddev"]:
                    print(f"{stat.capitalize()}: {value:,.2f} bytes")

        # File size distribution
        print("\n--- File Size Distribution ---")
        distribution = self.get_file_size_distribution()
        distribution.show()


def analyze_table(table_name: str, print_results: bool = True) -> dict[str, Any]:
    """Analyze a table and optionally print results.

    Parameters
    ----------
    table_name : str
        Full table name to analyze
    print_results : bool, optional
        Whether to print results to console, by default True

    Returns
    -------
    dict[str, Any]
        Dictionary containing analysis results
    """
    diagnostics = TableDiagnostics(table_name)

    if print_results:
        diagnostics.print_diagnostics()

    return diagnostics.get_file_size_statistics()


def analyze_multiple_tables(
    table_names: list[str], print_results: bool = True
) -> dict[str, dict[str, Any]]:
    """Analyze multiple tables.

    Parameters
    ----------
    table_names : list[str]
        List of table names to analyze
    print_results : bool, optional
        Whether to print results to console, by default True

    Returns
    -------
    dict[str, dict[str, Any]]
        Dictionary mapping table names to their analysis results
    """
    results = {}

    for table_name in table_names:
        if print_results:
            print(f"\n{'=' * 80}")

        try:
            results[table_name] = analyze_table(table_name, print_results)
        except Exception as e:
            if print_results:
                print(f"Error analyzing {table_name}: {e}")
            results[table_name] = {"error": str(e)}

    return results
