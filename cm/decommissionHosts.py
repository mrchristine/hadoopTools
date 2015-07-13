#!/usr/bin/python
from cm_api.api_client import ApiResource
import sys, argparse
import json, pprint

# Script to decommission hosts from a cluster


def build_parser(input_args):
    parser = argparse.ArgumentParser(
        description='Cloudera Manager Configuration APIs')

    parser.add_argument('decom_hosts', default="localhost", help='Host(s) to decommission', nargs='+')
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


if __name__ == "__main__":
    debug = False
    args = build_parser(sys.argv[1:])
    if args.verbose:
        debug = True

    if args.cm_host is None:
        print "Must provide hostname for CM"
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
    # Get a list of hosts registered with the Cloudera Manager instance
    host_list = api.get_all_hosts()

    # Find host object given the hostname as input arg
    decom_host_list = []
    for h in host_list:
        if h.hostname in args.decom_hosts:
            decom_host_list.append(h.hostname)

    print "Decommissioning following hosts: " + " ".join(decom_host_list)
    decom_status = cm.hosts_decommission(decom_host_list)
    print decom_status
    decom_status.wait()

    print "Successfully decommissioned: " + " ".join(decom_host_list)
