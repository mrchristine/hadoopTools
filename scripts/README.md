## CDH Tools

`impalaStats.py` is a utility to grab diagnostic information for Impala queries run on the platform.  
Users connect to a particular impala daemon to run queries, and this tool uses the impala daemons debug UI page to collect relevant diagnostic information into a tarball.  
The script uses the json exposed backends to parse impala daemon metrics and specific queries. Users can connect to any impala daemon, and load balancer can complicate this process if users don't know which daemon they used to run the query. 

**Usage**:
```bash
./impalaStats.py IMPALA_HOSTNAME
```
**Options**:
```
-n : integer argument (default 10) that collects the number of query profiles from the selected impala daemon. daemons store up to 25 profiles.  
-v : verbose mode for debugging  
-d : don't cleanup the original directory where metrics are collected. Useful for users who want to analyze the information themselves  
-l : grab all the logfiles from all impala daemon backends. This iterates through all impala daemons and collects the log fragments exposed by the debug UI.  
-p : specify daemon port if the port is not the default (25000)
```


`./solrReplicaCountDebugger.sh` is a utility to query a shard with multiple replicas, and verifies the document count for a shard between the shards replicas. Depending on the indexing speed, the goal was to identify when there were gaps between the doc counts across replicas.  

Within the script, there are a few arguments that need to be set:
```
colName : Collection Name  
shard : Shard Name. This can be found in the clusterstate.json from zookeeper or the solr web UI  
query : This specifies the query to run. Make sure the rows argument is larger than the # of docs per shard.  
  - Example:"select?q=*%3A*&wt=json&indent=true&rows=5000&distrib=false"  
zkServer : location of a zookeeper server to collect the clusterstate.json object  
```

Example of the table here comparing the replicas against each other.  

| Name | sorted_id_host-1.log | sorted_id_host-2.log | sorted_id_host-3.log | 
| ---- | -------------------- | ---------------------| -------------------- |
| sorted_id_host-1.log | 0 | 0 | 0 | 
| sorted_id_host-2.log | 0 | 0 | 0 | 
| sorted_id_host-3.log | 0 | 0 | 0 | 


