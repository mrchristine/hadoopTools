#!/bin/bash
DEBUG=0

# Collection Name / Shard # and Query to debug fluctuating doc counts
colName="test"
shard="shard1"
query="select?q=*%3A*&wt=json&indent=true&rows=5000&distrib=false"
zkServer="localhost"

rm -rf clusterstate.json shard_data.txt core_names.txt cores.txt

echo "get /solr/clusterstate.json" | zookeeper-client -server $zkServer:2181 > clusterstate.json 2>/dev/null
grep -A1 "$colName\_$shard" clusterstate.json > shard_data.txt

if [ -s shard_data.txt ]; 
then
	# Need to parse node hostname and core name to automate
	echo "Clusterstate.json success!"

	# Grab the core / hostname for each shared
	grep "core\|node_name" shard_data.txt | awk -F ':' '{print $2}' | sed "s/[\",]//g" > core_names.txt
	
	xargs -L2 echo < core_names.txt > cores.txt

	# Loop through shards to find unique docs 	
	while read line; do
		host=$(echo $line | awk '{print $2}')
		sh=$(echo $line | awk '{print $1}')
		echo "shard: $sh, hostname: $host"
		curl -s "http://$host:8983/solr/$sh/$query" > id_${host%%.*}.log
		# Grab the id files
		grep "\"id\"\:" id_${host%%.*}.log | sort > sorted_id_${host%%.*}.log
	done < cores.txt

	# Need to compare the sorted ids
	files=`ls sort*`
	echo 
	echo -e "Table calculating the # of different doc ids between shards\n"
	columns="Name `echo $files | sed "s/\n/ /g"`"
	dfile=diff_table.txt
	echo $columns > $dfile

	for i in $files; do
		echo -n "$i " >> $dfile
		for j in $files; do
			count=`sdiff -s $i $j | wc -l`
			echo -n "$count" >> $dfile
			echo -n " " >> $dfile
		done
		echo -en "\n" >> $dfile
	done
	column -t -s ' ' $dfile

	if [ $DEBUG -eq 1 ]
	then
		echo "Sorted filenames: $files"
	fi

	if [ $DEBUG -eq 0 ]
	then
		rm -rf clusterstate.json shard_data.txt core_names.txt cores.txt
	fi
else
	echo "Cluster state failed."
	rm -rf clusterstate.json shard_data.txt
fi
echo -e "\n\nDONE!"
