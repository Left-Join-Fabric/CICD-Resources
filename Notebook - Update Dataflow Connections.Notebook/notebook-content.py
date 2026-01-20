# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Fabric CI/CD:  Update Dataflow connections/destinations for new environment deployments
# 
# Background: When Dataflows (Gen2) are migrated to a new workspace, if the query destination is a Lakehouse in the original workspace then it will not automatically update and use the new Lakehouse in the new workspace.
# 
# This script solves that problem by looking for old connection information and replacing it the new version, using the Fabric REST API.
# It will update both the Default Destination and any individual query destinations,
#     **as long as the current destination matches the source workspace/lakehouse provided below.**
# 
# Inputs: 
# 
# - target_workspace:   The workspace containing the Dataflows to update
# - target_lakehouse:   A Lakehouse name, in the target workspace, The name of the new Lakehouse, which will be used to set the new connection
# - source_lakehouse:   The name of the Lakehouse currently used as the query destination
# - source_workspace:   The name of workspace containing the source_lakehouse
# 
# **All Dataflows in the target workspace are examined, all references to the source workspace or lakehouse are updated to the target info.**
# 
# Connections in Fabric require both the Workspace and Lakehouse ID.
# 
# Assumption:  The new destination is a Lakehouse in the same workspace as the migrated Dataflow.
# 
# 
# 
# 


# CELL ********************

# Input parameters.  Modify any of the variables in this cell prior to running

target_lakehouse='Lakehouse_Silver'
source_lakehouse=target_lakehouse

source_workspace= 'Data Integration'
target_workspace= 'Data Integration (Test)'
description_comment = 'Destination set to ' + target_workspace

FABRIC_API = "https://api.fabric.microsoft.com/v1"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Import libraries

import sempy.fabric as fabric
import pandas as pd
import base64
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Define functions

# CELL ********************

def get_workspace_items(workspace_name):

    workspace_id = fabric.resolve_workspace_id(workspace_name)

    if len(workspace_id) < 10:
        print(len(df_selected_workspace))
        raise RuntimeError("Invalid Workspace Name")

    else:

        url = f"{FABRIC_API}/workspaces/{workspace_id}/items"
        response = client.get(url)
    
        return workspace_id,pd.json_normalize(response.json()['value'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def update_item_property(item_type,item_id,property_name,new_value):

    payload={f'{property_name}': f'{new_value}'}
    url = f"{FABRIC_API}/workspaces/{target_workspace_id}/{item_type}/{item_id}"
  
    response = client.patch(url, json=payload)
    
    return response.status_code

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def update_dataflow_destination(item_to_update,workspace_id,source_workspace_id,target_workspace_id,source_lakehouse_id,target_lakehouse_id):

    url = f"{FABRIC_API}/workspaces/{workspace_id}/dataflows/{item_to_update}/getDefinition"
    response = client.post(url)

    if response.status_code != 200:
        raise RuntimeError(f"Unable to retrieve item.  Response Status Code: {response.status_code}")

    previous_definition=response.json()

    # Index 1 contains the query Steps and Destination of a Dataflow
    payload_64 = previous_definition["definition"]["parts"][1]["payload"]

    # Decode the payload to plain text
    decoded_bytes=base64.b64decode(payload_64)
    old_payload_text=decoded_bytes.decode("utf-8")
    
    # The query definition is now plain text and can be modified as a string
    #  Replace the previous lakehouse connection values with the new values
    new_payload_text=old_payload_text.replace(source_workspace_id,target_workspace_id)
    new_payload_text=new_payload_text.replace(source_lakehouse_id,target_lakehouse_id)

    if new_payload_text==old_payload_text:
        return 0  # No update is necessary, exit the function
    
    # Convert the payload string back to Base64
    encoded_bytes=base64.b64encode(new_payload_text.encode('utf-8'))
    encoded_string=encoded_bytes.decode('utf-8')

    # Replace the old payload with the new payload, keeping the rest of the definition as is
    updated_definition=previous_definition
    updated_definition["definition"]["parts"][1]["payload"]=encoded_string

    # The updated definition is now complete, use the API to update the definition
    url = f"{FABRIC_API}/workspaces/{workspace_id}/dataflows/{item_to_update}/updateDefinition"
    response=client.post(url,json=updated_definition)
  
    return response.status_code


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def append_item_description(item_type,item_id,description_comment):

    # Get the old description
    url = f"{FABRIC_API}/workspaces/{target_workspace_id}/{item_type}/{item_id}"
    response = client.get(url)

    if response.status_code != 200:
        raise RuntimeError(f"Unable to retrieve description for item id: {item_id}.  Response Status Code: {response.status_code}")
    else:
        old_description=response.json()["description"]
        #print(old_description)

        # Appened the new description comment to the old comment
        # If the dataflow has been updated previously, make sure and account for this without duplicating the comment
        temp = description_comment.replace(target_workspace,'')

        pos = old_description.find(temp)
        if pos>=0:
            old_description = old_description[:pos]

        new_description = old_description + """

""" + description_comment

        return new_description

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Code Section: Update the Connection properties in the Dataflows

# MARKDOWN ********************

# **Process Overview**
# 
# 1. Based on the inputs provided, use the Fabric REST API to lookup up ID values of the workspace/lakehouse
# 2. Retrieve a list of all dataflows in the target workspace and loop through it
# 3. If the data flow references the source connection, replace it with the target connection
# 4. Record the update by updating the dataflow description

# MARKDOWN ********************

# ### Find the items to update along with the new and old connection info

# CELL ********************

#Instantiate the client
client = fabric.FabricRestClient()

# Get a list of available workspaces
#url = f"{FABRIC_API}/workspaces"
#response = client.get(url)
#df_workspaces = pd.json_normalize(response. json()['value'])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The custom get_workspace_items returns a workspace id and a dataframe of all items within that workspace

source_workspace_id,df_source_items = get_workspace_items(source_workspace)
target_workspace_id,df_target_items = get_workspace_items(target_workspace)

# Get the ID of the Source Lakehouse

df_source_lakehouse = df_source_items[(df_source_items['displayName']==source_lakehouse) & (df_source_items['type']=='Lakehouse')]
source_lakehouse_id =df_source_lakehouse['id'].item()

# Get the ID of the Target Lakehouse

df_target_lakehouse = df_target_items[(df_target_items['displayName']==target_lakehouse) & (df_target_items['type']=='Lakehouse')]
target_lakehouse_id =df_target_lakehouse['id'].item()

if len(source_lakehouse_id)<1 or len(target_lakehouse_id)<1:
    display(source_lakehouse_id)
    display(target_lakehouse_id)
    raise RuntimeError("Unable to retrieve Lakehouse IDs")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Get the list of dataflows to update, using a loop to update each item

# CELL ********************

# Get the list of Dataflows in the target workspace

#display(target_workspace_items)
df_update_items = df_target_items[df_target_items['type']=='Dataflow']

display(df_update_items)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

items_updated = 0
items_skipped = 0

for item_to_update in df_update_items.itertuples():

    display(item_to_update.id)
    return_status = update_dataflow_destination(
        item_to_update.id,
        target_workspace_id # location of the items to update
        ,source_workspace_id,target_workspace_id
        ,source_lakehouse_id,target_lakehouse_id
        )
    if return_status==200:
        # Update the description to confirm update
        new_description=append_item_description('dataflows',df_update_items["id"].item(),description_comment)
        update_item_property('dataflows',df_update_items["id"].item(),'description',new_description)
        items_updated += 1
    else:
        items_skipped += 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Process Complete.  Print a summary of the status and how many items were updated.

# CELL ********************

 # Summary of process

print(f"Last return status: {return_status}")
print(f"Items updated: {items_updated}")
print(f"Items skipped: {items_skipped}")
 
mssparkutils.notebook.exit("Success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
