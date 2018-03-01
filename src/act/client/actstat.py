#!/usr/bin/env python

"""
Get job information from aCT.

Returns:
    1: No proxy found.
    2: One of the elements in job list is not a range.
    3: One of the elements in job list is not a valid ID.
    9: Error getting job stats (likely because of wrong column names).
"""

import argparse
import sys

# parse arguments
parser = argparse.ArgumentParser(description='Get job info from aCT')
parser.add_argument('-j', '--jobs', default='',
        help='ID/range(id1-id2;id1<id2)/comma separated list of IDs/ranges')
parser.add_argument('-f', '--find', default='',
        help='get only jobs with matching (sub)string in their name')
parser.add_argument('-s', '--state', default='',
        help='get only jobs with certain state')
parser.add_argument('-v', '--verbose', action='store_true',
        help='show more information')
parser.add_argument('-p', '--proxy', default=None,
        help='custom path to proxy certificate')
parser.add_argument('--arc-cols', default='JobID,State,arcstate',
        help='columns from ARC table that should be fetched')
parser.add_argument('--client-cols', default='jobname',
        help='columns from client table that should be fetched')
parser.add_argument('--get-cols', action='store_true',
        help='print all available column names')
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

# get column names from database
manager = jobmgr.JobManager()
if args.get_cols:
    clientCols = manager.getClientColumns()
    arcCols = manager.getArcColumns()
    print 'client cols:',
    for col in clientCols:
        print col,
    print
    print 'arc cols:',
    for col in arcCols:
        print col,
    print
    sys.exit(0)

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

# create column lists
if not args.client_cols:
    clicols = []
else:
    clicols = args.client_cols.split(',')
if not args.arc_cols:
    arccols = []
else:
    arccols = args.arc_cols.split(',')

# get proxy ID given proxy
proxyManager = proxymgr.ProxyManager()
try:
    proxyid = proxyManager.getProxyIdForProxyFile(args.proxy)
except NoSuchProxyError as e:
    print 'error: no proxy found, run actproxy'
    sys.exit(1)

# get information
try:
    jobdicts = manager.getJobStats(
            proxyid,
            jobs,
            args.state,
            args.find,
            clicols=clicols,
            arccols=arccols)
except Exception as e:
    print 'error: {}'.format(str(e))
    sys.exit(9)

if not jobdicts: # no jobs so just exit
    sys.exit(0)

# For each column, determine biggest sized value so that output can
# be nicely formatted.
colsizes = {}
for job in jobdicts:
    for key, value in job.items():
        # All keys have a letter and underscore prepended, which is not
        # used when printing
        colsize = max(len(str(key[2:])), len(str(value)))
        try:
            if colsize > colsizes[key]:
                colsizes[key] = colsize
        except KeyError:
            colsizes[key] = colsize

# Print table header
for col in clicols:
    print '{:<{width}}'.format(col, width=colsizes['c_' + col]),
for col in arccols:
    print '{:<{width}}'.format(col, width=colsizes['a_' + col]),
print
line = ''
for value in colsizes.values():
    line += '-' * value
line += '-' * (len(colsizes.values()) - 1)
print line

# Print jobs
for job in jobdicts:
    for col in clicols:
        fullKey = 'c_' + col
        try:
            if job[fullKey].strip() == '':
                txt = "''"
            else:
                txt = job[fullKey]
        except:
            txt = job[fullKey]
        print '{:<{width}}'.format(txt, width=colsizes[fullKey]),
    for col in arccols:
        fullKey = 'a_' + col
        try:
            if job[fullKey].strip() == '':
                txt = "''"
            else:
                txt = job[fullKey]
        except:
            txt = job[fullKey]
        print '{:<{width}}'.format(txt, width=colsizes[fullKey]),
    print


