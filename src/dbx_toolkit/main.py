"""Main CLI entry point for the dbx_toolkit package."""

import argparse
import sys

from dbx_toolkit.diagnostics import analyze_multiple_tables, analyze_table
from dbx_toolkit.generate_hierarchy import generate_azure_hierarchy
from dbx_toolkit.ingestion import get_table_list_from_config


def diagnose_table(args: argparse.Namespace) -> None:
    """Diagnose a single table."""
    try:
        analyze_table(args.table_name, print_results=True)
    except Exception as e:
        print(f"Error analyzing table {args.table_name}: {e}")
        sys.exit(1)


def diagnose_multiple_tables(args: argparse.Namespace) -> None:
    """Diagnose multiple tables."""
    if args.table_names:
        table_names = args.table_names.split(",")
    elif args.config_table:
        # Get table names from configuration
        table_names, _ = get_table_list_from_config(
            config_table=args.config_table, source_name=args.source_name
        )
    else:
        print("Error: Must provide either --table-names or --config-table")
        sys.exit(1)

    try:
        analyze_multiple_tables(table_names, print_results=True)
    except Exception as e:
        print(f"Error analyzing tables: {e}")
        sys.exit(1)


def generate_hierarchy_diagram(args: argparse.Namespace) -> None:
    """Generate Azure Databricks hierarchy diagram."""
    try:
        diagram = generate_azure_hierarchy(
            json_file_path=args.input_file,
            output_file_path=args.output_file,
            horizontal=args.horizontal,
        )

        if not args.output_file:
            print(diagram)
        else:
            print(f"Hierarchy diagram saved to: {args.output_file}")

    except Exception as e:
        print(f"Error generating hierarchy diagram: {e}")
        sys.exit(1)


def list_config_tables(args: argparse.Namespace) -> None:
    """List tables from configuration."""
    try:
        table_names, table_inc_names = get_table_list_from_config(
            config_table=args.config_table, source_name=args.source_name
        )

        print(f"Found {len(table_names)} tables in configuration:")
        print("-" * 50)
        for i, (table_name, inc_name) in enumerate(
            zip(table_names, table_inc_names, strict=False), 1
        ):
            print(f"{i:3}. {table_name}")
            if inc_name != table_name:
                print(f"     -> {inc_name} (incremental)")

    except Exception as e:
        print(f"Error listing tables: {e}")
        sys.exit(1)


def create_cli_parser() -> argparse.ArgumentParser:
    """Create the command line argument parser."""
    parser = argparse.ArgumentParser(
        description="Databricks Toolkit - Diagnostic and analysis tools",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze a single table
  dbx-toolkit diagnose-table hive_metastore.schema.table_name

  # Analyze multiple tables
  dbx-toolkit diagnose-tables --table-names "table1,table2,table3"

  # Analyze tables from configuration
  dbx-toolkit diagnose-tables --config-table "schema.config_table" --source-name "SYSTEM1"

  # Generate hierarchy diagram
  dbx-toolkit generate-hierarchy --input workspaces.json --output hierarchy.md

  # List tables from configuration
  dbx-toolkit list-tables --config-table "schema.config_table"
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Diagnose single table command
    diagnose_parser = subparsers.add_parser(
        "diagnose-table", help="Analyze a single table for file size and performance diagnostics"
    )
    diagnose_parser.add_argument("table_name", help="Full table name to analyze")
    diagnose_parser.set_defaults(func=diagnose_table)

    # Diagnose multiple tables command
    diagnose_multi_parser = subparsers.add_parser("diagnose-tables", help="Analyze multiple tables")
    diagnose_multi_group = diagnose_multi_parser.add_mutually_exclusive_group(required=True)
    diagnose_multi_group.add_argument("--table-names", help="Comma-separated list of table names")
    diagnose_multi_group.add_argument(
        "--config-table", help="Configuration table to get list of tables from"
    )
    diagnose_multi_parser.add_argument("--source-name", help="Source system name filter")
    diagnose_multi_parser.set_defaults(func=diagnose_multiple_tables)

    # Generate hierarchy command
    hierarchy_parser = subparsers.add_parser(
        "generate-hierarchy", help="Generate Azure Databricks hierarchy Mermaid diagram"
    )
    hierarchy_parser.add_argument(
        "--input",
        "-i",
        dest="input_file",
        default="databricks-workspaces.json",
        help="Input JSON file with workspace data",
    )
    hierarchy_parser.add_argument(
        "--output",
        "-o",
        dest="output_file",
        help="Output file for the diagram (optional, prints to console if not specified)",
    )
    hierarchy_parser.add_argument(
        "--horizontal", action="store_true", help="Generate horizontal diagram instead of vertical"
    )
    hierarchy_parser.set_defaults(func=generate_hierarchy_diagram)

    # List tables command
    list_parser = subparsers.add_parser("list-tables", help="List tables from configuration")
    list_parser.add_argument(
        "--config-table", default="aldm_oper.landing_load_config", help="Configuration table name"
    )
    list_parser.add_argument("--source-name", help="Source system name filter")
    list_parser.set_defaults(func=list_config_tables)

    return parser


def main() -> bool:
    """Entry point for the dbx_toolkit package."""
    parser = create_cli_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return False

    try:
        args.func(args)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False
    else:
        return True


if __name__ == "__main__":  # pragma: no cover
    success = main()
    sys.exit(0 if success else 1)
