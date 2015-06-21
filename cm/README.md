### Cloudera Manager API

This README will cover the usage of the Cloudera Manager API and extend a few of the examples on the documentation page. 

Importing the CM module and understanding the basics.
```python
from cm_api.api_client import ApiResource

cm_host = "cm-host"
api = ApiResource(cm_host, username="admin", password="admin")
# Grab the Cloudera Manager instance object to perform the actions at the CM level
cm = api.get_cloudera_manager()

# Get a list of all clusters
for c in api.get_all_clusters():
  print c.name

# Grab the first cluster
cluster = api.get_all_clusters()[0]

# Iterate through the services
for s in cluster.get_all_services():
  print s
```

The above example imports the module, The api object creates a top level api for performing a few actions, such as iterating through clusters and creating a host. 
To look into this further, you can call `dir(api)` or `help(api)` to understand what methods are available on the API object.  

To drill down further, you can either work on the cluster object or the cloudera manager object as seen above.

