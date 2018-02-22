#!/usr/bin/env python

import argparse
import sys
import requests

import config


conf_dict = {}

# get config from arguments
parser = argparse.ArgumentParser(description='Submit job to aCT server')
parser.add_argument('--proxy', default=None,
        help='path to proxy file')
parser.add_argument('--server', default=None,
        help='URL to aCT server')
parser.add_argument('--port', default=None,
        help='port on aCT server')
parser.add_argument('--cadir', default=None,
        help='path to directory with CA certificates')
parser.add_argument('--conf', default=None,
        help='path to configuration file')
parser.add_argument('--site', default='default',
        help='site that jobs should be submitted to')
parser.add_argument('xRSL', help='path to job description file')
args = parser.parse_args()

conf_dict['proxy']  = args.proxy
conf_dict['server'] = args.server
conf_dict['port']   = args.port
conf_dict['cadir']  = args.cadir

config.parse_non_param_conf(conf_dict, args.conf)

request_url = conf_dict['server'] + ':' + str(conf_dict['port']) + '/jobs'
try:
    files = {'xrsl': open(args.xRSL, 'r')}
except Exception as e:
    print 'error opening xRSL file: {}'.format(str(e))
    sys.exit(2)
form = {'site': args.site}

try:
    r = requests.post(request_url, files=files, data=form, cert=conf_dict['proxy'], verify=conf_dict['cadir'])
except Exception as e:
    print 'requests error: {}'.format(str(e))
    sys.exit(5)

if r.status_code == 200:
    print '{} - succesfully submited job with id {}'.format(r.status_code, r.text)
else:
    print '{} - {}'.format(r.status_code, r.text)
    sys.exit(4)
