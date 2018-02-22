#!/usr/bin/env python

"""
Clean jobs from aCT.

Returns:
    1: No proxy found.
    2: One of the elements in job list is not a range.
    3: One of the elements in job list is not a valid ID.
"""

import argparse
import sys

# parse arguments
parser = argparse.ArgumentParser(description='Clean jobs from aCT')
parser.add_argument('-j', '--jobs', default='',
        help='comma separated list of job IDs or ranges')
parser.add_argument('-f', '--find', default='',
        help='get only jobs with matching (sub)string in their name')
parser.add_argument('-s', '--state', default='',
        help='get only jobs with certain state')
parser.add_argument('-v', '--verbose', action='store_true',
        help='show more information')
parser.add_argument('-p', '--proxy', default=None,
        help='custom path to proxy certificate')
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

# create a list of jobs to work on
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

# clean jobs
manager = jobmgr.JobManager()
numDeleted = manager.cleanJobs(proxyid, jobs, args.state, args.find)
print 'Jobs deleted: {}'.format(numDeleted)


