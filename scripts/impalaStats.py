#!/usr/bin/python
import os, time
import sys, argparse
import tarfile
from contextlib import closing
from shutil import rmtree
import json, pprint
from urllib2 import urlopen


def make_tarfile(output_filename, source_dir):
    with closing(tarfile.open(output_filename, "w:gz")) as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))


# Function to grab the raw html file
def get_stats(site):
    fname = site.split('/')[-1]
    if fname == '':
        fname = "index"
    if debug:
        print(site)
    if (fname == "metrics" or fname == "logs" or fname == "varz"):
        html = urlopen(site + "?raw=true").read()
    elif (fname == "memz"):
        html = urlopen(site + "?detailed&raw=true").read()
    else:
        html = urlopen(site + "?json").read()
    out = open(outputDir + "/" + fname, 'w')
    out.write(html)
    out.close()

def logQueryJson(query, output):
    output.write("User: " + query['effective_user'])
    output.write("\nDB: " + query['default_db'])
    output.write("\nQuery: " + query['stmt'])
    output.write("\nState: " + query['state'])
    qID = query['query_id']
    # Grab the query profile url
    if debug:
        # Format is :25000/query_profile?query_id=<ID>&raw=true
        print("qURL is " + BASE_URL + "/query_profile?query_id=" + qID + "&raw=true")
    qHtml = urlopen(BASE_URL + "/query_profile?query_id=" + qID + "&raw=true").read()
    output.write("\nQuery Profile: \n")
    output.write(qHtml)

def get_queries_json(site):
    # Parse and collect query and profiles. Query locations not collected
    html = urlopen(site + "?json").read()
    jhtml = json.loads(html)
    if debug:
        pprint.pprint(jhtml)
    completed_q = jhtml['completed_queries']
    # Count the # of query profiles to return
    c = 1
    # Iterate over the rows, and extract the profile
    outFile = open(outputDir + "/" + "queryProfiles", "w")
    for r in completed_q:
        if debug:
            print("Log: " + r['stmt'])
        logQueryJson(r, outFile)
        if c == min(results.count,25):
            break
        c = c+1
    outFile.close()

def getAllLogs(p):
    json_data = open(outputDir + "/" + "backends")
    jdata = json.load(json_data)
    bends = jdata['backends']
    logDir = outputDir + "/imp_logs/"
    os.makedirs(logDir)
    if debug:
        print(bends)
    for be in bends:
        delim = be.index(":")
        b = be[:delim]
        logUrl = "http://" + b + ":" + str(p) + "/logs?raw=true"
        if debug:
            print "Grab log: " + logUrl
        html = urlopen(logUrl).read()
        out = open(logDir + b, "w")
        out.write(html)
        out.close()


if __name__ == "__main__":
    # True / False debug variable
    debug = False
    parser = argparse.ArgumentParser(description='Gather Impala Stats')

    parser.add_argument('-n', dest='count', type=int, default=10, help='Number of query profiles to collect')
    parser.add_argument('-v', dest='verbose', action='store_true', default=False, help='Enable verbose logging')
    parser.add_argument('-d', dest='cleanup', action='store_false', default=True,
                    help="Don't delete original contents directory")
    parser.add_argument('-l', dest='grabLogs', action='store_true', default=False,
                    help="Collect all the impala daemon logs for the known backends tab. WARNING: Slow operation")
    parser.add_argument('-p', dest='port', type=int, default=25000, help="Port for impala web UI")
    parser.add_argument('impalad', help='Required: Hostname or IP address of impala daemon')

    results = parser.parse_args(sys.argv[1:])
    if results.verbose:
        debug = True

    BASE_URL = "http://" + results.impalad + ":" + str(results.port)

    outputDir = "impala-" + time.strftime("%Y%m%d-%H%M%S")

    if not os.path.exists(outputDir):
        os.makedirs(outputDir)

    # Loop through the index page and find the tabs available
    get_stats(BASE_URL + "/")
    index_tabs = json.load(open(outputDir + "/" + "index"))['__common__']['navbar']
    tabs = []
    for i in index_tabs:
        if i['link'] == "/":
            continue
        # Append to tabs and strip off beginning /
        tabs.append(i['link'][1:])

    if debug:
        print "TABS: "
        print(tabs)

    # Iterate through the tabs and grab the data
    for s in tabs:
        if (s != "queries"):
            get_stats(BASE_URL + "/" + s)
        else:
            get_queries_json(BASE_URL + "/" + s)

    if results.grabLogs:
        if debug:
            print "Collecting all logs in serial manner..."
        getAllLogs(results.port)
    # Tar gzip the directory contents
    make_tarfile(outputDir + ".tar.gz", outputDir)

    if results.cleanup == True:
        rmtree(outputDir)
