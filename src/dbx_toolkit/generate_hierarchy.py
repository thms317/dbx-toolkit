"""Generate a Mermaid diagram showing the hierarchy of Azure Databricks resources."""

import json
import re
from pathlib import Path
from typing import Any


def generate_azure_hierarchy(
    json_file_path: str = "databricks-workspaces.json",
    output_file_path: str | None = None,
    horizontal: bool = False,
) -> str:
    """Generate a Mermaid diagram showing the hierarchy of Azure Databricks resources.

    Parameters
    ----------
    json_file_path : str, optional
        Path to the JSON file containing workspace data
    output_file_path : str | None, optional
        Path where to save the Mermaid diagram (optional)
    horizontal : bool, optional
        Whether to generate a horizontal (LR) or vertical (TD) diagram

    Returns
    -------
    str
        Mermaid diagram as a string
    """
    # Load workspaces
    with Path(json_file_path).open() as f:
        workspaces = json.load(f)

    # Create hierarchy structure
    tenant_id = workspaces[0]["tenant_id"]
    subscriptions: dict[str, list[str]] = {}
    resource_groups: dict[str, dict[str, Any]] = {}

    for ws in workspaces:
        sub_id = ws["subscription_id"]
        rg_name = ws["resource_group"]
        ws_name = ws["workspace_name"]
        ws_id = ws["workspace_id"]

        if sub_id not in subscriptions:
            subscriptions[sub_id] = []

        if rg_name not in resource_groups:
            resource_groups[rg_name] = {"subscription": sub_id, "workspaces": []}
            subscriptions[sub_id].append(rg_name)

        resource_groups[rg_name]["workspaces"].append({"name": ws_name, "id": ws_id})

    # Generate Mermaid - using LR (left to right) or TD (top down) based on parameter
    direction = "LR" if horizontal else "TD"
    mermaid = ["```mermaid", f"flowchart {direction}"]

    # Add tenant node
    mermaid.append(f'    Tenant["Tenant: {tenant_id}"]')
    mermaid.append("")

    # Add subscription nodes
    for _, sub_id in enumerate(subscriptions.keys(), 1):
        safe_sub_id = re.sub(r"[^a-zA-Z0-9]", "_", sub_id)
        mermaid.append(f'    Sub_{safe_sub_id}["Subscription: {sub_id}"]')
        mermaid.append(f"    Tenant --> Sub_{safe_sub_id}")

    mermaid.append("")

    # Add resource group nodes
    for rg_name, rg_data in resource_groups.items():
        safe_rg_name = re.sub(r"[^a-zA-Z0-9]", "_", rg_name)
        safe_sub_id = re.sub(r"[^a-zA-Z0-9]", "_", rg_data["subscription"])
        mermaid.append(f'    RG_{safe_rg_name}["Resource Group: {rg_name}"]')
        mermaid.append(f"    Sub_{safe_sub_id} --> RG_{safe_rg_name}")

        # Add workspace nodes
        for ws in rg_data["workspaces"]:
            safe_ws_name = re.sub(r"[^a-zA-Z0-9]", "_", ws["name"])
            mermaid.append(f'    WS_{safe_ws_name}["Workspace: {ws["name"]} ({ws["id"]})"]')
            mermaid.append(f"    RG_{safe_rg_name} --> WS_{safe_ws_name}")

        mermaid.append("")

    mermaid.append("```")
    mermaid_str = "\n".join(mermaid)

    # Write to file if output path is provided
    if output_file_path:
        with Path(output_file_path).open("w") as f:
            f.write(mermaid_str)

    return mermaid_str


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate a Mermaid diagram of Azure Databricks hierarchy"
    )
    parser.add_argument(
        "--input",
        "-i",
        default="databricks-workspaces.json",
        help="Path to the JSON file with workspace data",
    )
    parser.add_argument(
        "--output", "-o", default=None, help="Path where to save the output Mermaid diagram"
    )
    parser.add_argument(
        "--horizontal",
        "-lr",
        action="store_true",
        help="Generate a horizontal (left to right) diagram instead of vertical",
    )

    args = parser.parse_args()

    # Generate the diagram
    diagram = generate_azure_hierarchy(
        json_file_path=args.input,
        output_file_path=args.output
        or (
            "azure-databricks-hierarchy-horizontal.md"
            if args.horizontal
            else "azure-databricks-hierarchy.md"
        ),
        horizontal=args.horizontal,
    )

    # Print to console
    print(diagram)
