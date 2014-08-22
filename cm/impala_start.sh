#!/bin/bash
# To enable debugging. Change debug to 1. This will not delete the temporary hosts file
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
roles=`curl -s -u ${user}:${pass} ${scm}/clusters/${cname}/services/${role_name}/roles | grep -B 1 "type.....IMPALAD"  | grep name | awk '{print $3}' | sed "s/[\",]//g" > tmp_imp_$$.log`

## Initialize counter and array of role names
INDX=1
## Batch size variable
SIZE=2
roles="["
while read line
do
	roles="$roles \"$line\","
	if [ $INDX -eq $SIZE ]
	then
		roles="$roles \"$line\"]"
		if [ $debug -eq 1 ]
		then
			echo "Role list: $roles"
		fi
 		out=$(curl -X POST -H "Content-Type:application/json" -d '{ "items":'"$roles"' }' -u ${user}:${pass} "${scm}/clusters/${cname}/services/${role_name}/roleCommands/restart")
		## Reset to begin the next batch.
		sleep 600
		roles="["
		INDX=0;

	fi
	INDX=$[INDX + 1]
done < "tmp_imp_$$.log"

# There may not be an even # of nodes, so we need to restart the last batch here.
roles="$roles \"$line\"]"
if [ $debug -eq 1 ]
then
	echo "Role list: $roles"
fi
out=$(curl -X POST -H "Content-Type:application/json" -d '{ "items":'"$roles"' }' -u ${user}:${pass} "${scm}/clusters/${cname}/services/${role_name}/roleCommands/restart")

if [ $debug -eq 0 ]
then
	rm -rf tmp_imp_$$.log
fi

echo "DONE"
