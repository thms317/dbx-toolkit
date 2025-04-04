#!/bin/bash

# Get Databricks workspace credentials using Azure Resource Graph
# Make script executable and run script:
# chmod +x scripts/get-databricks-creds.sh && ./scripts/get-databricks-creds.sh


# Login to Azure
echo "Checking Azure authentication..."
if ! az account show > /dev/null 2>&1; then
    echo "Not logged in to Azure. Attempting login..."
    az login
fi

# Install Azure Resource Graph extension if not already installed
echo "Checking for Azure Resource Graph extension..."
az extension show --name resource-graph &> /dev/null || az extension add --name resource-graph

# Prompt for workspace name or URL if not provided
WORKSPACE_IDENTIFIER=${1:-}
OUTPUT_FILE=${2:-}
EXPORT_ALL=false
DEFAULT_OUTPUT_FILE="databricks-workspaces.json"

if [ -z "$WORKSPACE_IDENTIFIER" ]; then
    EXPORT_ALL=true
    # Use default output file if none provided
    if [ -z "$OUTPUT_FILE" ]; then
        OUTPUT_FILE="$DEFAULT_OUTPUT_FILE"
    fi
fi

echo "Searching for Databricks workspaces across all subscriptions..."

# Query all Databricks workspaces across all subscriptions
QUERY="Resources | where type =~ 'Microsoft.Databricks/workspaces' | project name, id, location, workspaceUrl=properties.workspaceUrl, subscriptionId, resourceGroup"
WORKSPACES=$(az graph query -q "$QUERY" --query "data" -o json)

# Check if any workspaces were found
if [ "$(echo $WORKSPACES | jq length)" -eq 0 ]; then
    echo "No Databricks workspaces found in any subscription."
    exit 1
fi

echo "Found $(echo $WORKSPACES | jq length) Databricks workspaces."

# Get tenant ID
TENANT_ID=$(az account show --query "tenantId" -o tsv)

if [ "$EXPORT_ALL" = true ]; then
    # Process all workspaces and create a combined JSON
    ALL_CREDS=$(echo $WORKSPACES | jq --arg tenantId "$TENANT_ID" '[.[] | {
        subscription_id: .subscriptionId,
        resource_group: .resourceGroup,
        workspace_name: .name,
        azure_resource_id: .id,
        workspace_url: .workspaceUrl,
        tenant_id: $tenantId,
        workspace_id: (.workspaceUrl | capture("adb-(?<id>[0-9]+)").id)
    }]')

    # Output to file
    echo "$ALL_CREDS" > "$OUTPUT_FILE"
    echo "Exported credentials for all workspaces to $OUTPUT_FILE"
    exit 0
fi

# Find matching workspace - match only on name to avoid issues
MATCHES=$(echo $WORKSPACES | jq -c ".[] | select(.name | contains(\"$WORKSPACE_IDENTIFIER\"))")

if [ -z "$MATCHES" ]; then
    echo "No workspace matching '$WORKSPACE_IDENTIFIER' found."
    echo "Available workspaces:"
    echo $WORKSPACES | jq -r '.[] | "  - \(.name)"'
    exit 1
fi

# Count matches
MATCH_COUNT=$(echo "$MATCHES" | jq -s '. | length')
if [ "$MATCH_COUNT" -gt 1 ]; then
    echo "Multiple workspaces found matching '$WORKSPACE_IDENTIFIER':"
    echo "$MATCHES" | jq -r '.name'
    read -p "Please enter the exact workspace name from the list above: " EXACT_NAME
    WORKSPACE=$(echo "$MATCHES" | jq -c "select(.name == \"$EXACT_NAME\")")

    # If no exact match is found
    if [ -z "$WORKSPACE" ]; then
        echo "No exact match found. Please try again."
        exit 1
    fi
else
    WORKSPACE=$MATCHES
fi

# Extract and display information
SUBSCRIPTION_ID=$(echo $WORKSPACE | jq -r '.subscriptionId')
RESOURCE_GROUP=$(echo $WORKSPACE | jq -r '.resourceGroup')
WORKSPACE_NAME=$(echo $WORKSPACE | jq -r '.name')
AZURE_RESOURCE_ID=$(echo $WORKSPACE | jq -r '.id')
WORKSPACE_URL=$(echo $WORKSPACE | jq -r '.workspaceUrl')
# Extract the Databricks workspace ID from the URL (the number after adb- and before the first period)
WORKSPACE_ID=$(echo "$WORKSPACE_URL" | sed -E 's/adb-([0-9]+)\..*/\1/')

# Check if we should output JSON or text
if [ -n "$OUTPUT_FILE" ]; then
    # Output as JSON
    echo "{
  \"subscription_id\": \"$SUBSCRIPTION_ID\",
  \"resource_group\": \"$RESOURCE_GROUP\",
  \"workspace_name\": \"$WORKSPACE_NAME\",
  \"azure_resource_id\": \"$AZURE_RESOURCE_ID\",
  \"workspace_url\": \"$WORKSPACE_URL\",
  \"workspace_id\": \"$WORKSPACE_ID\",
  \"tenant_id\": \"$TENANT_ID\"
}" > "$OUTPUT_FILE"
    echo "Exported credentials to $OUTPUT_FILE"
else
    # Output as text
    echo "-----------------------------------------"
    echo "DATABRICKS WORKSPACE CREDENTIALS"
    echo "-----------------------------------------"
    echo "Public Cloud ID / Subscription ID: $SUBSCRIPTION_ID"
    echo "Account ID: $RESOURCE_GROUP"
    echo "Workspace Name: $WORKSPACE_NAME"
    echo "Azure Resource ID: $AZURE_RESOURCE_ID"
    echo "Workspace URL: $WORKSPACE_URL"
    echo "Workspace ID: $WORKSPACE_ID"
    echo "Tenant ID: $TENANT_ID"
fi
