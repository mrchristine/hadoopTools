from cm_api.api_client import ApiResource
import re, math
import pprint, json
import sys, argparse
from subprocess import call
# Followed the guide located here:
# https://cloudera.github.io/cm_api/docs/python-client/
# This guide assumes the major version of CM are the same on both clusters
# Assumes there is 1 type of service per cluster, i.e. 1 hive service on each cluster.
# Assumes the source service role config groups should be created on the destination service
# Not tested on Kerberos enabled cluster
# Apply configuration from config file not yet working

# Cluster specific configurations are not applied
# Current list of non-configured settings:
# [_heapsize$, *database*, _service, namenode.*handler_count, hue_webhdfs]

debug = False

def buildParser(inputArgs):
    parser = argparse.ArgumentParser(
        description='Cloudera Manager Configuration APIs')

    parser.add_argument('-D', dest='dumpConf', action='store_true', default=False,
                        help='Dump the configuration to local disk. Specify cluster using --src parameter')
    parser.add_argument('--configFile', dest='srcConf', help='Configuration filename')
    # hostnames and port
    parser.add_argument('-s', '--src', dest='src', help='Source CM hostname')
    parser.add_argument('-d', '--dst', dest='dst', help='Destination CM hostname')
    parser.add_argument('-P', '--port', dest='port', default=7180, type=int, help='CM Port')
    # user / pass
    parser.add_argument('-u', '--user', dest='user', default='admin', help='CM User')
    parser.add_argument('-p', '--pass', dest='password', default='admin', help='CM Password')
    parser.add_argument('--srcCluster', dest='fromCluster', help='Source Cluster Name')
    parser.add_argument('--dstCluster', dest='toCluster', help='Destination Cluster Name')
    parser.add_argument('-a', '--applyConf', dest='readConf', help='NOT WORKING: Read and apply configuration from '
                                                                   'file defined by --configFile.')
    parser.add_argument('-v', dest='verbose', action='store_true', default=False,
                        help='Enable verbose logging')
    parser.add_argument('-b', dest='bestEffort', action='store_true', default=False,
                        help='Remove incompatible config properties. (version mismatch configs)')
    return parser.parse_args(inputArgs)

def pickCluster(cList):
    # Print all cluster names and allow user to choose
    for i in xrange(len(cList)):
        print str(i) + " : " + cList[i].name + " / " + cList[i].fullVersion
    while True:
        try:
            cNum = int(raw_input("Pick the cluster number from above: "))
        except ValueError:
            print "Please provide a valid number from above"
            continue
        if cNum not in range(len(cList)):
            print "Please provide a valid number from above"
            continue
        else:
            if debug:
                print "Chosen cluster: " + cList[cNum].name
                print "Cluster version: " + cList[cNum].version
            return cList[cNum]

# Get all services for cluster
def getServices(cluster):
    """
    Gets an array of the configured services
    This assumes only 1 type of service per cluster.
    :param cluster
    :return: array of service datatypes
    Datastructure: service.name / .type
    If multiple services exists, add logic to copy a particular services to particular destination service
    """
    services = []
    for s in cluster.get_all_services():
        if debug:
            print s
        services.append(s)
    return services

def dumpConfig(cluster, fname):
    """
    :param cList: cluster list
    :param fname: output filename
    This will dump the clusters configuration to a file. This is mainly used as a backup for cluster configuration.
    """
    outFile = open(fname, "w")
    services = getServices(cluster)
    # Iterate over services and print to file
    for s in services:
        sConf = s.get_config()[0]
        sEntry = {s.type : s.name,
                  "config" : sConf}
        if debug:
            print s.name
            pprint.pprint(sConf)
        json.dump(sEntry, outFile, sort_keys = True, indent=4, separators=(',', ": "))
        rcg = []
        for group in s.get_all_role_config_groups():
            rcg.append(group)
        for g in rcg:
            if debug:
                print "roleConfigGroup: " + g.name
            rConf = g.get_config()
            rEntry = {g.roleType : g.name,
                      "config" : rConf}
            json.dump(rEntry, outFile, sort_keys = True, indent=4, separators=(',', ": "))
    outFile.close()

def filterConfigs(conf):
    """
    This will remove the configs listed here
    _service: Filters service dependency configurations
    database: Filters any database related configurations
    _heapsize: Filters all heap size configurations.
    handler:
    :param conf: configuration dictionary
    :return:
    """
    configRegex = [".*_service$", ".*database.*", ".*_heapsize$",
                   ".*namenode.*handler_count$", "hue_webhdfs"]
    for s in configRegex:
        reObj = re.compile(s)
        rmKeys = filter(reObj.match, conf.keys())
        if rmKeys and debug:
            print "Filtered configuration keys: " + str(rmKeys)
        for k in rmKeys:
            del conf[k]

def copyServiceRoleGroup(sServ, dServ):
    """
    Copy over the roleConfigGroup configurations
    Grab all the base config groups to copy over, check the .base property to do so
    If multiple RCGs exist, user needs to add logic to copy over the configuration
    :param sServ: source service
    :param dServ: destination service
    :return:
    """
    sRCG = {}
    for sGroup in sServ.get_all_role_config_groups():
        if sGroup.roleType in sRCG:
            sRCG[sGroup.roleType].append(sGroup)
        else:
            sRCG[sGroup.roleType] = [sGroup]

    dRCG = {}
    for d in dServ.get_all_role_config_groups():
        # Build reverse index of roleTypes -> roleConfigGroup objects
        # Grab the destination clusters base role config groups, we will create new role config groups if they exist
        # on the source service
        if d.base:
            dRCG[d.roleType] = d

    for roleType, listOfGroupConfigs in sRCG.iteritems():
        if roleType in dRCG:
            for roleGroup in listOfGroupConfigs:
                roleConf = roleGroup.get_config()
                filterConfigs(roleConf)
                if debug:
                    print "Role type found in destination: " + roleType
                    print roleGroup.name
                    print "Updating w/ filtered conf: " + dRCG[roleType].name
                    pprint.pprint(roleConf)
                    print "\n"

                if roleGroup.base:
                    targetRoleGroup = dRCG[roleType]
                else:
                    try:
                        targetRoleGroup = dServ.create_role_config_group(roleGroup.name, roleGroup.displayName,
                                                                        roleGroup.roleType)
                    except Exception as e:
                        print e
                        if args.bestEffort:
                            print "Best effort flag set to true. Will skip role group."
                            continue
                        else:
                            raise e

                # Try to update the roleConfigGroup entries at least 5 times, removing entries if necessary
                # Impala needs special handling because of version changes
                for i in xrange(5):
                    try:
                        if roleConf:
                            targetRoleGroup.update_config(roleConf)
                    except Exception as e:
                        print e
                        prop = e.message.split("'")[1]

                        if args.bestEffort:
                           del roleConf[prop]
                        else:
                            print "Re-run the tool w/ the following -b option to remove incompatible properties"
                            raise

def copyServiceConf(sC, dC):
    """
    Given the cluster objects, find services and copy to destination
    :param sC: source cluster
    :param dC: destination cluster
    :return:
    """
    # Get list of services given cluster object
    sServices = getServices(sC)
    dServices = getServices(dC)
    # Build a dictionary w/ type as a key, and array of service objects as value
    dTypes = {}
    for d in dServices:
        # if the key exists, append service to value list
        if d.type in dTypes:
            dTypes[d.type].append(d)
        else:
            dTypes[d.type] = [d]

    for s in sServices:
        if s.type not in dTypes:
            continue
        sConf = s.get_config()[0]
        if debug:
            print "Src service: " + s.name
            pprint.pprint(sConf)
        # This loops through the services if multiple services are defined per type, i.e. multiple Hive services
        for dstService in dTypes[s.type]:
            if debug:
                print "Dst service: " + dstService.name
            # Ensure the config is non-empty
            if sConf:
                # Filter out dependency configs because service names vary between clusters
                filterConfigs(sConf)
                # Check if sConfig is empty or not before attempting to update
                if sConf:
                    dstService.update_config(sConf)

            # Now copy the roleConfigGroups for the service
            copyServiceRoleGroup(s, dstService)

def setUniqueConf(dC, dstApi):
    """
    Example of how you would set the unique configuration properties per service
    Namenode heap, NN handler counts
    :param dC: destination cluster
    :return:
    """
    dServices = getServices(dC)
    # Build a dictionary w/ type as a key, and array of service objects as value
    dTypes = {}
    for d in dServices:
        # if the key exists, append service to value list
        if d.type in dTypes:
            dTypes[d.type].append(d)
        else:
            dTypes[d.type] = [d]
    HDFS = dTypes["HDFS"][0]
    for r in HDFS.get_all_roles():
        if r.type == 'NAMENODE':
            nn = r

    for r in HDFS.get_all_role_config_groups():
        # Configure NAMENODE-BASE only
        # Skips SECONDARYNAMENODE configs
        if r.name.endswith("-NAMENODE-BASE"):
            if debug:
                print "Found NAMENODE config: " + r.name
            nnRole = r
            break

    nnHost = dstApi.get_host(nn.hostRef.hostId)
    # Configure 70% of the RAM for the NN
    # Could prompt the user here for size input
    nnHeap = long(math.ceil(nnHost.totalPhysMemBytes*0.7))

    # Calculate # of handler threads, ln(# of dn)*20
    # Get # of datanode roles per service
    num_dn = len(HDFS.get_roles_by_type("DATANODE"))
    handlerCount = int(math.ceil(math.log(num_dn)*20))
    nnConf = {"dfs_namenode_handler_count" : handlerCount,
              "dfs_namenode_service_handler_count" : handlerCount,
              "namenode_java_heapsize" : nnHeap}
    if debug:
        print "Updating NN config: "
        pprint.pprint(nnConf)
    nnRole.update_config(nnConf)

if __name__ == "__main__":
    args = buildParser(sys.argv[1:])
    if args.verbose:
        debug = True

    if (args.src is None) or (args.dst is None):
        print "Must provide source and destination hostnames for CM"
        exit(1)

    sapi = ApiResource(args.src, args.port, args.user, args.password)
    dapi = ApiResource(args.dst, args.port, args.user, args.password)

    if (args.fromCluster is None) or (args.toCluster is None):
        # Get all cluster names
        sClusters = []
        for c in sapi.get_all_clusters():
            sClusters.append(c)

        dClusters = []
        for c in dapi.get_all_clusters():
            dClusters.append(c)

        # Choose source and destination clusters
        print "Source cluster: " + args.src
        sCluster = pickCluster(sClusters)
        #cdh5 = sCluster
        print "Destination cluster: " + args.dst
        dCluster = pickCluster(dClusters)
    else:
        sCluster = sapi.get_cluster(args.fromCluster)
        dCluster = dapi.get_cluster(args.toCluster)

    if args.dumpConf:
        if args.srcConf:
            sName = args.srcConf
        else:
            sName = args.src + "_" + sCluster.name.replace(" ", "_") + "_config.txt"
        dumpConfig(sCluster, sName)
    elif args.readConf:
        # Read configuration from file and appy to destination cluster
        print "Boo"
    else:
        # Copy all service configurations to destination if that type of service exists
        origConf = args.dst + "_" + dCluster.name.replace(" ", "_") + "_original_config.txt"
        dumpConfig(dCluster, origConf)
        copyServiceConf(sCluster, dCluster)
        endConf = args.dst + "_" + dCluster.name.replace(" ", "_") + "_final_config.txt"
        dumpConfig(dCluster, endConf)
        call(["diff", "-u", origConf, endConf])

    print("Completed!")
