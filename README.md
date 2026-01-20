# CICD-Resources
### _Resources for implementing CI/CD on Microsoft Fabric_

For background, see **Microsoft Fabric - CICD Introduction.pdf** (root folder of this repo)
https://github.com/Left-Join-Fabric/CICD-Resources/blob/8d27d62fd8d234cdbcbae3047c85df3a5f79995b/Microsoft%20Fabric%20-%20CICD%20Introduction.pdf


## 1. Fabric Notebook to update Dataflow connections/destinations

Background: When Dataflows (Gen2) are migrated to a new workspace, if the query destination is a Lakehouse in the original workspace then it will not automatically update and use the new Lakehouse in the new workspace.  This Python notebook solves that problem.

### To Deploy and Run

1. Add the notebook to any Fabric workspace in your tenant
   - Option 1:  Download the .iqynb file and import it into a workspace
   - Option 2:  Using Fabric Git Integration:  Clone this repo or download the content, and add to a Azure DevOps or GitHub repo linked to a Fabric Workspace
2. After deploying your Workspace to a new environment, open the Notebook in Fabric and edit the parameters in the first cell (see Inputs, below)
3. Execute (Run All) the notebook

### Script Overview

This script updates connections in a Dataflow by looking up the old connection information and replacing it the equivalent connnection in the new Workspace, using the Fabric REST API.
It will update both the Default Destination and any individual query destinations,
    **as long as the current destination matches the source workspace/lakehouse parameters.**

#### Inputs (Parameters): 

- target_workspace:   The workspace containing the Dataflows to update
- target_lakehouse:   A Lakehouse name, in the target workspace, The name of the new Lakehouse, which will be used to set the new connection
- source_lakehouse:   The name of the Lakehouse currently used as the query destination
- source_workspace:   The name of workspace containing the source_lakehouse

**All Dataflows in the target workspace are examined, all references to the source workspace or lakehouse are updated to the target info.**

Connections in Fabric require both the Workspace and Lakehouse ID.

Assumptions:
* The new destination is a Lakehouse in the same workspace as the migrated Dataflow.
* The user running the notebooks has Contributor access to both workspaces




