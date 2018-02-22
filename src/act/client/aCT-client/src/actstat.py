#!/usr/bin/env python

import argparse
import sys
import requests

import config


conf_dict = {}

# get config from arguments
parser = argparse.ArgumentParser(description="Get jobs' status")
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
parser.add_argument('--arc', default='arcstate',
        help='a list of columns from ARC table')
parser.add_argument('--client', default='id,jobname',
        help='a list of columns from client table')
parser.add_argument('--state', default=None,
        help='the state that jobs should be in')
parser.add_argument('--name', default=None,
        help='substring that jobs should have in name')
args = parser.parse_args()

conf_dict['proxy']  = args.proxy
conf_dict['server'] = args.server
conf_dict['port']   = args.port
conf_dict['cadir']  = args.cadir

config.parse_non_param_conf(conf_dict, args.conf)

request_url = conf_dict['server'] + ':' + str(conf_dict['port']) + '/jobs'

# add parameters
if args.id or args.arc or args.client or args.state or args.name:
    request_url += '?'
    if args.id:
        request_url += 'id=' + args.id + '&'
    if args.arc:
        request_url += 'arc=' + args.arc + '&'
    if args.client:
        request_url += 'client=' + args.client + '&'
    if args.state:
        request_url += 'state=' + args.state + '&'
    if args.name:
        request_url += 'name=' + args.name
    request_url = request_url.rstrip('&')

try:
    r = requests.get(request_url, cert=conf_dict['proxy'], verify=conf_dict['cadir'])
except Exception as e:
    print 'requests error: {}'.format(str(e))
    sys.exit(5)

if args.arc:
    arccols = args.arc.split(',')
else:
    arccols = []
if args.client:
    clicols = args.client.split(',')
else:
    clicols = []

if r.status_code == 200:
    try:
        json_resp = r.json()
    except ValueError as e:
        print 'Response error: {}. Response status: {}'.format(e, r.status_code)
        sys.exit(3)
    # For each column, determine biggest sized value so that output can
    # be nicely formatted.
    colsizes = {}
    for job in json_resp:
        for key, value in job.items():
            # All keys have a letter and underscore prepended, which is not
            # used when printing
            colsize = max(len(str(key[2:])), len(str(value)))
            try:
                if colsize > colsizes[key]:
                    colsizes[key] = colsize
            except KeyError:
                colsizes[key] = colsize
    # Print jobs
    for job in json_resp:
        for col in clicols:
            fullKey = 'c_' + col 
            print '{:<{width}}'.format(job[fullKey], width=colsizes[fullKey]),
        for col in arccols:
            fullKey = 'a_' + col 
            print '{:<{width}}'.format(job[fullKey], width=colsizes[fullKey]),
        print
else:
    print '{} - {}'.format(r.status_code, r.text)
    sys.exit(4)
