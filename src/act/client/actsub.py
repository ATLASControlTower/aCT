#!/usr/bin/env python

"""
Submit xRSL job to aCT.

Returns:
    1: No proxy found.
    4: Site is not configured.
    6: Invalid job description.
    7: Error with xRSL file.
"""


def readXRSL(filepath):
    """Return contents of given file."""
    with open(filepath, 'r') as xrsl:
        return xrsl.read()


import os
import argparse
import sys

# parse arguments
parser = argparse.ArgumentParser(description='Submit xRSL job to aCT')
parser.add_argument('-p', '--proxy', default=None,
        help='custom path to proxy certificate')
parser.add_argument('-s', '--site', default='default',
        help='specific site to submit job to')
parser.add_argument('-v', '--verbose', action='store_true',
        help='show more information')
parser.add_argument('xRSL', help='path to xRSL file')
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
import act.client.clientdb as clientdb
import act.client.proxymgr as proxymgr
import act.common.aCTConfig as aCTConfig
from act.client.errors import *

# get ID given proxy
proxyManager = proxymgr.ProxyManager()
try:
    proxyid = proxyManager.getProxyIdForProxyFile(args.proxy)
except NoSuchProxyError as e:
    print 'error: no proxy found, run actproxy'
    sys.exit(1)

# get and check job description, check sites
try:
    jobdesc = readXRSL(args.xRSL)
    jobmgr.checkJobDesc(jobdesc)
    jobmgr.checkSite(args.site) # use default path for sites.json
except jobmgr.InvalidJobDescriptionError:
    print 'error: invalid job description'
    sys.exit(6)
except jobmgr.NoSuchSiteError as e:
    print 'error: site \'{}\' is not configured'.format(args.site)
    sys.exit(4)
except IOError:
    print 'error: could not read xRSL file'
    sys.exit(7)

# insert job
arcconf = aCTConfig.aCTConfigARC()
clidb = clientdb.ClientDB(dbname=arcconf.get(['db', 'name']))
jobid = clidb.insertJob(jobdesc, proxyid, args.site)
print 'Successfully inserted job with id {}'.format(jobid)


