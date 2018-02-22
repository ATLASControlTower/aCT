#!/usr/bin/env python

import argparse
import sys
import requests

import config


conf_dict = {}

# get config from arguments
parser = argparse.ArgumentParser(description="Fetch failed jobs")
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
        help='a list of IDs of jobs that should be queried')
parser.add_argument('--name', default=None,
        help='substring that jobs should have in name')
args = parser.parse_args()

conf_dict['proxy']  = args.proxy
conf_dict['server'] = args.server
conf_dict['port']   = args.port
conf_dict['cadir']  = args.cadir

config.parse_non_param_conf(conf_dict, args.conf)

post_data = {'arcstate': 'tofetch'}

request_url = conf_dict['server'] + ':' + str(conf_dict['port']) + '/jobs'

# add parameters
if args.id or args.name:
    request_url += '?'
    if args.id:
        request_url += 'id=' + args.id + '&'
    if args.name:
        request_url += 'name=' + args.name
    request_url = request_url.rstrip('&')

try:
    r = requests.patch(request_url, cert=conf_dict['proxy'], verify=conf_dict['cadir'], json=post_data)
except Exception as e:
    print 'requests error: {}'.format(str(e))
    sys.exit(5)

if r.status_code == 200:
    print 'Will fetch {} jobs'.format(r.text)
else:
    print '{} - {}'.format(r.status_code, r.text)
    sys.exit(4)
