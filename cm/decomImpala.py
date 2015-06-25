#!/usr/bin/python
from cm_api.api_client import ApiResource
import sys, argparse
import json, pprint
from urllib2 import urlopen, URLError, HTTPError
# Followed the guide located here:
# https://cloudera.github.io/cm_api/docs/python-client/
# Goal: Monitor the impala daemon debug UI for running queries before decommissioning the host
# Queries in flight is the metric we use to determine if a fragment is running on the host
# There is a configurable timeout to force a decommission after waiting a certain amount of time.
# This can be okay on systems that are not heavily loaded.


def build_parser(input_args):
    parser = argparse.ArgumentParser(
        description='Cloudera Manager Configuration APIs')

    parser.add_argument('-t', dest='wait_time', default=60,
                        help='Time to wait before forcefully decommissioning the node running impalad')
    parser.add_argument('impala_host', default="localhost", help='Host to requires decommissioning')
    # CM API args, i.e. user / pass
    parser.add_argument('-C', '--host', dest='cm_host', default='localhost', help='Cloudera Manager Hostname')
    parser.add_argument('-P', '--port', dest='port', default=7180, type=int, help='CM Port')
    parser.add_argument('-u', '--user', dest='user', default='admin', help='CM User')
    parser.add_argument('-p', '--pass', dest='password', default='admin', help='CM Password')
    parser.add_argument('-c', '--cluster', dest='cluster', default='Cluster 1', help='Cluster name')
    parser.add_argument('-v', dest='verbose', action='store_true', default=False, help='Enable verbose logging')
    return parser.parse_args(input_args)


def pick_cluster(cluster_list):
    # Print all cluster names and allow user to choose
    for i in xrange(len(cluster_list)):
        print str(i) + " : " + cluster_list[i].name + " / " + cluster_list[i].fullVersion
    while True:
        try:
            n = int(raw_input("Pick the cluster number from above: "))
        except ValueError:
            print "Please provide a valid number from above"
            continue
        if n not in range(len(cluster_list)):
            print "Please provide a valid number from above"
            continue
        else:
            if debug:
                print "Chosen cluster: " + cluster_list[n].name
                print "Cluster version: " + cluster_list[n].version
            return cluster_list[n]


# Get all services for cluster
def get_impala_service(cluster):
    """
    Gets an array of the Impala services
    :param cluster_name
    :return: array of service data types
    """
    services = []
    for s in cluster_name.get_all_services():
        if debug:
            print s
        if s.type == "IMPALA":
            services.append(s)
    return services


def check_for_inflight_queries(impala_hostname, impala_rolename, impala_service, wait_time=300):
    for i in impala_service.get_all_role_config_groups():
        if i.base and i.roleType == "IMPALAD":
            impala_conf = i.get_config(view="full")

    if impala_conf is None:
        exit(1)
    if impala_conf["impalad_webserver_port"].value is None:
        port = impala_conf["impalad_webserver_port"].default
    else:
        port = impala_conf["impalad_webserver_port"].value

    if debug:
        pprint.pprint(impala_conf)
        print "Impala host: " + impala_hostname
        print "Impala roleName: " + impala_rolename

    url = "http://" + impala_hostname + ":" + str(port)
    print "Connecting to http://" + impala_hostname + ":" + str(port)
    # We sleep 5 seconds between polling the impala daemon for queries in flight
    num_in_flight = -1
    poll_interval = 5
    num_iterations = wait_time / poll_interval
    while num_in_flight != 0 and num_iterations >= 0:
        try:
            html = urlopen(url + "/queries?json").read()
        except HTTPError, e:
            print "Impala service already down"
            print e.code
            return
        except URLError, e:
            print "Impala service already down"
            print e.args
            return
        jhtml = json.loads(html)
        n = int(jhtml["num_in_flight_queries"])
        if debug:
            print "In flight count: " + str(n)
        if n == 0:
            # Stop the impala daemon
            impala_service.stop_roles(impala_rolename)
            break
        else:
            num_iterations -= 1

if __name__ == "__main__":
    debug = False
    args = build_parser(sys.argv[1:])
    if args.verbose:
        debug = True

    if args.cm_host is None:
        print "Must provide source and destination hostnames for CM"
        exit(1)

    api = ApiResource(args.cm_host, args.port, args.user, args.password)
    # Get CM object to decommission the host
    cm = api.get_cloudera_manager()
    if args.cluster is None:
        clusters = []
        for c in api.get_all_clusters():
            clusters.append(c)
        if len(clusters) > 1:
            cluster = pick_cluster(clusters)
        else:
            cluster = clusters[0]
    else:
        cluster = api.get_cluster(args.cluster)
    host_list = api.get_all_hosts()

    # Find host object given the hostname as input arg
    for h in host_list:
        if h.hostname == args.impala_host:
            host = h

    impala = get_impala_service(cluster)

    for r in impala.get_all_roles():
        if r.type == "IMPALAD":
            if r.hostRef.hostId == host.hostId:
                impala_host = r

    # Function to check for running impala queries, then shutdown role, then proceed w/ host decom
    check_for_inflight_queries(host.hostname, impala_host.name, impala, args.wait_time)
    decom_status = cm.hosts_decommission([host.hostname])
    # You can wait for the command to finish or poll periodically, start a timer and see how long you've been waiting
    print decom_status
    decom_status.wait()

    print("Completed!")
