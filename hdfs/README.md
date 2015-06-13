## HDFS Tools

There was a time where a lease could not be covered and operators had no method to recover the file lease from the command line.  
The `HDFSClient.java` was used for releases that didn't have this available.
Upstream jira: [HDFS-6917](https://issues.apache.org/jira/browse/HDFS-6917)  
`$ hdfs debug recoverLease [-path <path>] [-retries <num-retries>]`

There are a few cases where DN block reports would not get processed from the NN, and operators needed a way to trigger a block report from the command line.  
Upstream jira: [HDFS-7278](https://issues.apache.org/jira/browse/HDFS-7278)  
`$ hdfs dfsadmin [-triggerBlockReport [-incremental] <datanode_host:ipc_port>]`


