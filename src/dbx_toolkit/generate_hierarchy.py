import json
import re

# Load workspaces
with open('databricks-workspaces.json', 'r') as f:
    workspaces = json.load(f)

# Create hierarchy structure
tenant_id = workspaces[0]['tenant_id']
subscriptions = {}
resource_groups = {}

for ws in workspaces:
    sub_id = ws['subscription_id']
    rg_name = ws['resource_group']
    ws_name = ws['workspace_name']
    ws_id = ws['workspace_id']
    
    if sub_id not in subscriptions:
        subscriptions[sub_id] = []
    
    if rg_name not in resource_groups:
        resource_groups[rg_name] = {
            'subscription': sub_id,
            'workspaces': []
        }
        subscriptions[sub_id].append(rg_name)
    
    resource_groups[rg_name]['workspaces'].append({
        'name': ws_name,
        'id': ws_id
    })

# Generate Mermaid
mermaid = ['```mermaid', 'flowchart TD']

# Add tenant node
mermaid.append(f'    Tenant["Tenant: {tenant_id}"]')
mermaid.append('')

# Add subscription nodes
for i, sub_id in enumerate(subscriptions.keys(), 1):
    safe_sub_id = re.sub(r'[^a-zA-Z0-9]', '_', sub_id)
    mermaid.append(f'    Sub_{safe_sub_id}["Subscription: {sub_id}"]')
    mermaid.append(f'    Tenant --> Sub_{safe_sub_id}')

mermaid.append('')

# Add resource group nodes
for rg_name, rg_data in resource_groups.items():
    safe_rg_name = re.sub(r'[^a-zA-Z0-9]', '_', rg_name)
    safe_sub_id = re.sub(r'[^a-zA-Z0-9]', '_', rg_data['subscription'])
    mermaid.append(f'    RG_{safe_rg_name}["Resource Group: {rg_name}"]')
    mermaid.append(f'    Sub_{safe_sub_id} --> RG_{safe_rg_name}')
    
    # Add workspace nodes
    for ws in rg_data['workspaces']:
        safe_ws_name = re.sub(r'[^a-zA-Z0-9]', '_', ws['name'])
        mermaid.append(f'    WS_{safe_ws_name}["Workspace: {ws["name"]} ({ws["id"]})"]')
        mermaid.append(f'    RG_{safe_rg_name} --> WS_{safe_ws_name}')
    
    mermaid.append('')

mermaid.append('```')

# Write to file
with open('azure-databricks-hierarchy.md', 'w') as f:
    f.write('\n'.join(mermaid))

# Print to console
print('\n'.join(mermaid)) 