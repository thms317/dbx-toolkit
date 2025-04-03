# Databricks notebook source
# MAGIC %pip install --upgrade databricks-sdk

# COMMAND ----------

from typing import Any

from databricks.sdk import WorkspaceClient

# COMMAND ----------

client = WorkspaceClient()

# COMMAND ----------


def has_serverless_compute() -> bool:
    """Check if serverless compute is enabled."""
    try:
        return any(w.enable_serverless_compute for w in client.warehouses.list())
    except (AttributeError, ValueError, ConnectionError):
        return False


# Count the number of available catalogs if they exists then unity catalog in enabled
def check_unity_catalog() -> bool:
    """Check if Unity Catalog is enabled."""
    try:
        catalogs = list(client.catalogs.list())
        return len(catalogs) > 0
    except (AttributeError, ValueError, ConnectionError):
        return False


# Delta Sharing check
def check_delta_sharing() -> bool:
    """Check if Delta Sharing is enabled.

    Checks using two methods:
    1. Direct shares listing
    2. Examining catalog secure kinds for CATALOG_DELTASHARING
    """
    try:
        # Method 1: Check shares directly
        shares = list(client.shares.list())
        if len(shares) > 0:
            return True
        # Method 2: Check via catalog secure kinds
        for catalog in client.catalogs.list():
            if hasattr(catalog, "securable_kind") and "CATALOG_DELTASHARING" in str(catalog.securable_kind):
                return True
        return False  # noqa: TRY300
    except (AttributeError, ValueError, ConnectionError):
        return False


# MLflow check to check for experiment count
def check_mlflow() -> bool:
    """Check if MLflow is enabled."""
    try:
        experiments = list(client.experiments.list_experiments())
        return len(experiments) > 0
    except (AttributeError, ValueError, ConnectionError):
        return False


# Hive Metastore object count
def count_hive_metastore_objects() -> int:
    """Count the number of Hive Metastore objects."""
    databases = []
    try:
        catalogs = [c.full_name for c in client.catalogs.list() if c.full_name not in ["__databricks_internal", "system"]]
        for c in catalogs:
            if c is not None:
                schemas_list = list(client.schemas.list(c))
                databases.extend(schemas_list)
        return len(databases)
    except (AttributeError, ValueError, ConnectionError):
        return 0


# Unity Catalog count
def count_unity_catalogs() -> int:
    """Count the number of Unity Catalogs."""
    try:
        catalogs = list(client.catalogs.list())
        return len(catalogs)
    except (AttributeError, ValueError, ConnectionError):
        return 0


# Count users, groups, and service principals
def count_workspace_entities() -> tuple[int, int, int] | bool:
    """Count users, groups, and service principals."""
    try:
        users = list(client.users.list())
        groups = list(client.groups.list())
        service_principals = list(client.service_principals.list())
        return len(users), len(groups), len(service_principals)
    except (AttributeError, ValueError, ConnectionError):
        return False


def check_user_privileges(group_name: str = "admins") -> bool:
    """Check if the current user is an admin."""
    try:
        current_user = client.current_user.me().display_name
        # Find the admin group directly
        admin_groups = list(client.groups.list(filter=f"display_name eq '{group_name}'"))
        if not admin_groups:
            return False
        # Get the group members
        admin_group_id = admin_groups[0].id
        # The SDK expects get_members rather than list_members
        members = list(client.groups.get_members(admin_group_id))  # type: ignore[attr-defined]
        if not members:
            return False
        # Check if current user is in the members list
        return any(member.display == current_user for member in members if hasattr(member, "display") and member.display)
    except (AttributeError, ValueError, ConnectionError):
        return False


# Based on the example in databricks sdk documentation https://databricks-sdk-py.readthedocs.io/en/latest/account/billing/billable_usage.html

# def get_workspace_usage():
#     a = AccountClient()
#     resp = a.billable_usage.download(start_month="2024-08", end_month="2024-09")
#     return resp

# output['Workspace Spend'] = get_workspace_usage()


# COMMAND ----------


def get_workspace_info() -> dict[str, Any]:
    """Get workspace information."""
    output: dict[str, Any] = {}

    output["Is Admin"] = check_user_privileges()
    output["Account ID"] = client.config.account_id or "Not Available"
    # Alternative for account_id
    # b= BudgetConfiguration()
    # output['Account ID'] = b.account_id or "Not Available"
    # Using workspace_id property instead of get_workspace_id method
    output["Workspace ID"] = getattr(client, "workspace_id", "Not Available")
    output["Platform"] = (
        client.config.host.split(".")[0] if hasattr(client.config, "host") else "Unknown"
    )
    output["Uses Unity Catalog"] = check_unity_catalog()
    output["Uses Delta Sharing"] = check_delta_sharing()
    output["Uses MlFlow"] = check_mlflow()
    output["Hive Metastore Objects"] = count_hive_metastore_objects()
    output["Unity Catalogs"] = count_unity_catalogs()

    entities_result = count_workspace_entities()
    if isinstance(entities_result, tuple):
        users_count, groups_count, service_principals_count = entities_result
        output["Nr of Users"] = users_count
        output["Nr of Groups"] = groups_count
        output["Nr of Service Principals"] = service_principals_count
    else:
        output["Nr of Users"] = "Error retrieving user count"
        output["Nr of Groups"] = "Error retrieving group count"
        output["Nr of Service Principals"] = "Error retrieving service principal count"

    return output


# COMMAND ----------

get_workspace_info()
