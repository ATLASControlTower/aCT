#!/usr/bin/env python

import argparse
import requests
import sys

import config


conf_dict = {}

# get config from arguments
parser = argparse.ArgumentParser(description='Delete proxy from aCT server')
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
parser.add_argument('--id', default=None,
        help='a list of proxy IDs of proxies that should be deleted')
args = parser.parse_args()

if not args.id:
    print 'error: no proxy id given'
    sys.exit(1)

conf_dict['proxy']  = args.proxy
conf_dict['server'] = args.server
conf_dict['port']   = args.port
conf_dict['cadir']  = args.cadir

config.parse_non_param_conf(conf_dict, args.conf)

request_url = conf_dict['server'] + ':' + str(conf_dict['port']) + '/proxies'
request_url += '?id=' + args.id

try:
    r = requests.delete(request_url, cert=conf_dict['proxy'], verify=conf_dict['cadir'])
except Exception as e:
    print 'requests error: {}'.format(str(e))
    sys.exit(5)

if r.status_code == 200:
    print 'Deleted {} proxies'.format(r.text)
else:
    print '{} - {}'.format(r.status_code, r.text)
    sys.exit(4)


