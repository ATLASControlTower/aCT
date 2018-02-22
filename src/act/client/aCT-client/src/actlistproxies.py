#!/usr/bin/env python

import argparse
import sys
import requests
import json

import config


conf_dict = {}

# get config from arguments
parser = argparse.ArgumentParser(description='List proxies on aCT server')
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
args = parser.parse_args()

conf_dict['proxy']  = args.proxy
conf_dict['server'] = args.server
conf_dict['port']   = args.port
conf_dict['cadir']  = args.cadir

config.parse_non_param_conf(conf_dict, args.conf)

request_url = conf_dict['server'] + ':' + str(conf_dict['port']) + '/proxies'

try:
    r = requests.get(request_url, cert=conf_dict['proxy'], verify=conf_dict['cadir'])
except Exception as e:
    print 'requests error: {}'.format(str(e))
    sys.exit(5)

if r.status_code == 200:
    json_r = json.loads(r.text)
    for proxy in json_r:
        for key, value in proxy.items():
            print '{}:{} '.format(key, value),
        print
else:
    print '{} - {}'.format(r.status_code, r.text)
    sys.exit(4)
