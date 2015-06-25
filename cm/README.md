### Cloudera Manager API

This README will cover the usage of the Cloudera Manager API and extend a few of the examples on the documentation page. 

Importing the CM module and understanding the basics.
```python
from cm_api.api_client import ApiResource

cm_host = "cm-host"
api = ApiResource(cm_host, username="admin", password="admin")
# Grab the Cloudera Manager instance object to perform the actions at the CM level
cm = api.get_cloudera_manager()

```
The above example imports the module, The api object creates a top level api for performing a few actions, such as iterating through clusters and creating a host. 
To look into this further, you can call `dir(api)` or `help(api)` to understand what methods are available on the API object.  

The more complicated workflows require the Cloudera Manager object, such as decommission/recommission actions on a host or running any Cloudera Manager wizards / workflows.  

### Performing actions with the API

```python
# Get a list of all clusters
for c in api.get_all_clusters():
  print c.name

# Grab the first cluster
cluster = api.get_all_clusters()[0]

# Iterate through the services
for s in cluster.get_all_services():
  print s
```

If the CM instance is managing multiple clusters, you can either interate or fetch the cluster by name.  
The following is the outline of how items are structured.
```
CM / API -> Host List    -> Host Name -> Host ID
Cluster  -> Service List -> Role Name -> Role ID
```
For example, if you want to stop the datanode role for a given host, the following workflow would need to occur.
1. Define the cluster and service object, i.e. "CDH5 Cluster" and "HDFS" service.
2. Given the hostname as an input arg, search for the host ID through the list of hosts that are registered to the CM instance. 
3. Interate through the HDFS roles and find the role types that match _DATANODE_.
4. For each _DATANODE_ match, check the host ID from #2 to the current host ID. Once a matching role is found, save the role.
5. Given the role name, shutdown the role using the service object .

If using an interpreter to aide in understanding the structure and APIs available, you can use the `locals()` or `globals()` commands to print any local/global variables that have been defined. I find this useful when trying to build a workflow. 

### Understanding Configurations 
