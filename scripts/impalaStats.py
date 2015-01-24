import os, time
import sys, argparse
import tarfile
from shutil import rmtree
from bs4 import BeautifulSoup
from urllib2 import urlopen

# True / False debug variable
debug = False

parser = argparse.ArgumentParser(description='Gather Impala Stats')

parser.add_argument('-n', dest='count', type=int, default=5, help='Number of query profiles to collect')
parser.add_argument('-v', dest='verbose', action='store_true', default=False, help='Enable verbose logging')
parser.add_argument('-d', dest='cleanup', action='store_false', default=True,
                    help="Don't delete original contents directory")
parser.add_argument('impalad', help='Required: Hostname or IP address of impala daemon')

results = parser.parse_args(sys.argv[1:])
if results.verbose:
    debug = True

BASE_URL = "http://" + results.impalad + ":25000"

outputDir = "impala-" + time.strftime("%Y%m%d-%H%M%S")

if not os.path.exists(outputDir):
    os.makedirs(outputDir)

# Check if all backends are needed for debugging
# sessions/threadz don't support raw=true
tabs = ['/',
        'backends',
        'catalog',
        'logs',
        'memz',
        'metrics',
        'queries',
        'sessions',
        'threadz',
        'varz']

def make_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))

# Function to grab the raw html file
def get_stats(site):
    fname = site.split('/')[-1]
    if fname == '':
        fname = "index"
    if debug:
        print(site)
    if (fname == "sessions" or fname == "threadz"):
        html = urlopen(site).read()
    elif (fname == "memz"):
        html = urlopen(site + "?detailed&raw=true").read()
    else:
        html = urlopen(site + "?raw=true").read()
    out = open(outputDir + "/" + fname, 'w')
    out.write(html)
    out.close()

# Extract the query profile link and grab the query details
def logQuery(query, output):
    # user 0 | DB 1 | Statement 2 | Type 3 | Start 4 | End 5 | BE Progress 6 | State 7 | # Rows 8 | Profile 9
    output.write("User: " + query[0].string)
    output.write("\nDB: " + query[1].string)
    output.write("\nQuery: " + query[2].string)
    output.write("\nState: " + query[7].string)
    # Grab the query profile url
    qUrl = query[9].find("a")["href"]
    if debug:
        print("qURL is " + qUrl)
    qHtml = urlopen(BASE_URL + qUrl + "&raw=true").read()
    output.write("\nQuery Profile: \n")
    output.write(qHtml)
    output.write("\n")

def get_queries(site):
    # Parse and collect query and profiles. Query locations not collected
    html = urlopen(site).read()
    soup = BeautifulSoup(html, "lxml")
    # query profile table is the 3rd table
    qpTable = soup.find_all("table")[2]
    # Ignore the first table header row
    qpRows = qpTable.find_all("tr")[1:]
    outFile = open(outputDir + "/" + "queryProfiles", "w")
    # Count the # of query profiles to return
    c = 1
    # Iterate over the rows, and extract the profile
    for r in qpRows:
        if debug:
            print(r)
        logQuery(r.find_all("td"), outFile)
        if c == min(results.count,25):
            break
        c = c+1
    outFile.close()

# Loop to grab all the web endpoints
for s in tabs:
    if (s != "queries"):
        get_stats(BASE_URL + "/" + s)
    else:
        get_queries(BASE_URL + "/" + s)

# If this is CDH5.2 / Impala 2.0, grab the /rpcz endpoint
if "impalad version 2" in open(outputDir + "/" + "index", "r").read():
    get_stats(BASE_URL + "/" + "rpcz")


# Tar gzip the directory contents
make_tarfile(outputDir + ".tar.gz", outputDir)

if results.cleanup == True:
    rmtree(outputDir)
