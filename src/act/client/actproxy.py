#!/usr/bin/env python

"""
Insert proxy certificate into aCT.

Returns:
    8: Error inserting or updating proxy.
"""

def printProxyInfo(proxyInfo):
    """Print proxy info from aCT table."""
    for key, value in proxyInfo.items():
        print '{:<12}: {}'.format(key, value)


import argparse
import os
import sys

# parse arguments
parser = argparse.ArgumentParser(description = 'aCT proxies utility')
parser.add_argument('-p', '--proxy', default = None, help = 'custom path to proxy')
parser.add_argument('-v', '--verbose', action='store_true',
        help='show more information')
args = parser.parse_args()

# logging
import logging
logFormat = "[%(asctime)s] [%(filename)s:%(lineno)d] [%(levelname)s] - %(message)s"
if args.verbose:
    logging.basicConfig(format=logFormat, level=logging.DEBUG, stream=sys.stdout)
else:
    logging.basicConfig(format=logFormat, level=logging.DEBUG, filename=os.devnull)

import act.client.proxymgr as proxymgr
from act.client.errors import *

# determine proxy file path from args
if not args.proxy: # default proxy path is /tmp/x509_u<user id>
    proxyPath = '/tmp/x509up_u' + str(os.getuid())
else:
    proxyPath = args.proxy

manager = proxymgr.ProxyManager()
try:
    manager.updateProxy(proxyPath)
except Exception as e:
    print 'error: {}'.format(str(e))
    sys.exit(8)
