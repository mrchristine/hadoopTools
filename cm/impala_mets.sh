#!/bin/bash
# To enable debugging. Change debug to 1. This will not delete the temporary hosts file
# Grabs all the hostnames for the impala daemons and collect the metrics pages for each instance.
debug=0

## User defined arguments
user="admin"
pass="admin"
## Hostname of the CM instance here:
scm="http://localhost:7180/api/v5"
## Cluster name here. Replace spaces w/ %20 to comply w/ HTTP rules
cname="Cluster%201%20-%20CDH4"

if [ $debug -eq 1 ]
then
    verb="-v"
fi

## Get the role name for a specific cluster
role_name=`curl ${verb} -su ${user}:${pass} ${scm}/clusters/${cname}/services | grep -B 1 "type.....IMPALA"  | grep name | awk '{print $3}' | sed "s/[\",]//g"`
if [ $debug -eq 1 ]
then
	echo "Role name: $role_name"
fi

## Collect the role name for each host
roles=`curl -s -u ${user}:${pass} ${scm}/clusters/${cname}/services/${role_name}/roles | grep -A 6 "type.....IMPALAD"  | grep hostId | awk '{print $3}' | sed "s/[\",]//g" > tmp_imp_$$.log`

mkdir -p output

while read line
do
	curl -s http://${line}:25000/backends?raw=true > output/impalad_${line}.log
	curl -s http://${line}:25000//metrics?raw=true > output/impalad_metrics_${line}.log
done < "tmp_imp_$$.log"

if [ $debug -eq 0 ]
then
	rm -rf tmp_imp_$$.log
fi

echo "DONE"
