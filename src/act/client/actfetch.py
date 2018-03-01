#!/usr/bin/env python

#TODO: consider adding state argument because of refetch operation

"""
Fetch failed jobs from ARC.

Returns:
    1: No proxy found.
    2: One of the elements in job list is not a range.
    3: One of the elements in job list is not a valid ID.
"""

import argparse
import sys

# parse arguments
parser = argparse.ArgumentParser(description='Fetch failed jobs')
parser.add_argument('-j', '--jobs', default='',
        help='comma separated list of IDs or ranges')
parser.add_argument('-f', '--find',
        help='get only jobs with matching (sub)string in their name')
parser.add_argument('-v', '--verbose', action='store_true',
        help='show more information')
parser.add_argument('-p', '--proxy', default=None,
        help='custom path to proxy certificate')
parser.add_argument('-r', '--refetch', action='store_true',
        help='refetch packages')
args = parser.parse_args()

# logging
import logging
logFormat = "[%(asctime)s] [%(filename)s:%(lineno)d] [%(levelname)s] - %(message)s"
if args.verbose:
    logging.basicConfig(format=logFormat, level=logging.DEBUG, stream=sys.stdout)
else:
    import os
    logging.basicConfig(format=logFormat, level=logging.DEBUG, filename=os.devnull)

import act.client.jobmgr as jobmgr
import act.client.proxymgr as proxymgr
from act.client.errors import *

jobs = []
if args.jobs:
    try:
        jobs = jobmgr.getIDsFromList(args.jobs)
    except InvalidJobRangeError as e:
        print "error: range '{}' is not a valid range".format(e.jobRange)
        sys.exit(2)
    except InvalidJobIDError as e:
        print "error: ID '{}' is not a valid ID".format(e.jobid)
        sys.exit(3)

# get proxy ID given proxy
proxyManager = proxymgr.ProxyManager()
try:
    proxyid = proxyManager.getProxyIdForProxyFile(args.proxy)
except NoSuchProxyError as e:
    print 'error: no proxy found, run actproxy'
    sys.exit(1)

# fetch jobs
manager = jobmgr.JobManager()
if args.refetch:
    numFetching = manager.refetchJobs(proxyid, args.find)
    print 'Will refetch {} jobs'.format(numFetching)
else:
    numFetching = manager.fetchJobs(proxyid, jobs, args.find)
    print 'Will fetch {} jobs'.format(numFetching)


