#!/bin/bash
# To enable debugging. Change debug to 1. This will not delete the temporary hosts file
debug=1
# Config
user="admin"
pass="admin"
scm="http://localhost:7180/api/v4"
rack_script="find_rack.sh"
if [ $debug -eq 1 ]
then
	verb="--verbose"
fi

# Get the current lists of hosts within the cluster. 
cluster_name="Cluster%201%20-%20CDH4"
curl $verb -su ${user}:${pass} -o hosts_$$.log "${scm}/clusters/${cluster_name}/hosts"
# Collect hosts into a temporary variable
if [ -e hosts_$$.log ]
then
	hosts=$(cat hosts_$$.log | grep "hostId" | sed "s/\"//g" | awk '{print $3}')
fi

# Delete the temporary hosts file
if [ -e hosts_$$.log ] && [ $debug -eq 0 ] 
then
	rm -f hosts_$$.log
fi 

# Loop through each host and run the next script to determine the rack location
for h in $hosts
do
	rack=$(./${rack_script} $h)
	if [ $debug -eq 1 ]
	then
		echo "Updating host: $h to rack $rack"
	fi
	if [[ ! -z "$rack" ]]
	then
		# Update the rack location for that json object
		out=$(curl ${verb} -su ${user}:${pass} -X PUT -H "Content-Type:application/json" -d '{"rackId":"'"$rack"'"}' "http://localhost:7180/api/v4/hosts/$h")
	fi 
done

