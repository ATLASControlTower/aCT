#!/usr/bin/env python

import argparse
import sys
import requests

import config


conf_dict = {}

# get config from arguments
parser = argparse.ArgumentParser(description='Submit proxy to aCT server')
parser.add_argument('--proxy', default=None,
        help='path to proxy file')
parser.add_argument('--server', default=None,
        help='URL to aCT server')
parser.add_argument('--port', default=None,
        help='port on aCT server')
parser.add_argument('--cert', default=None,
        help="path to user's certificate file")
parser.add_argument('--key', default=None,
        help="path to user's key file")
parser.add_argument('--cadir', default=None,
        help='path to directory with CA certificates')
parser.add_argument('--conf', default=None,
        help='path to configuration file')
args = parser.parse_args()

conf_dict['proxy']  = args.proxy
conf_dict['server'] = args.server
conf_dict['port']   = args.port
conf_dict['cert']   = args.cert
conf_dict['key']    = args.key
conf_dict['cadir']  = args.cadir

config.parse_non_param_conf(conf_dict, args.conf)

# Since ARC doesn't seem to complain about non certificate files, should we
# check if given file is actual certificate here?
try:
    proxy_file = open(conf_dict['proxy'], 'r')
except Exception as e:
    print 'error opening proxy file: {}'.format(str(e))
    sys.exit(2)
else:
    proxy_str = proxy_file.read()
    proxy_file.close()

request_url = conf_dict['server'] + ':' + str(conf_dict['port']) + '/proxies'

try:
    r = requests.put(request_url, data=proxy_str, cert=(conf_dict['cert'], conf_dict['key']), verify=conf_dict['cadir'])
except Exception as e:
    print 'requests error: {}'.format(str(e))
    sys.exit(5)

if r.status_code == 200:
    print '{} - successfully inserted proxy with id {}'.format(r.status_code, r.text)
    sys.exit(0)
else:
    print '{} - {}'.format(r.status_code, r.text)
    sys.exit(4)


