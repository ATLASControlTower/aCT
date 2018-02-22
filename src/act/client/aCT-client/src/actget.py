#!/usr/bin/env python

import argparse
import sys
import os
import requests
import zipfile

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
parser.add_argument('--cadir', default=None,
        help='path to directory with CA certificates')
parser.add_argument('--conf', default=None,
        help='path to configuration file')
parser.add_argument('--id', default='',
        help='a list of IDs of jobs that should be queried')
args = parser.parse_args()

conf_dict['proxy']  = args.proxy
conf_dict['server'] = args.server
conf_dict['port']   = args.port
conf_dict['cadir']  = args.cadir

config.parse_non_param_conf(conf_dict, args.conf)

request_url = conf_dict['server'] + ':' + str(conf_dict['port']) + '/results'
request_url += '?id=' + args.id

try:
    r = requests.get(request_url, cert=conf_dict['proxy'], verify=conf_dict['cadir'], stream=True)
except Exception as e:
    print 'requests error: {}'.format(str(e))
    sys.exit(5)

if r.status_code == 200:
    # 'Content-Disposition': 'attachment; filename=ZrcMDm3nK4rneiavIpohlF4nABFKDmABFKDmggFKDmEBFKDm2cmmzn.zip'
    filename = r.headers['Content-Disposition'].split()[1].split('=')[1]

    try:
        with open(filename, 'wb') as result_file:
            for chunk in r.iter_content():
                if chunk: # filter out keep-alive new chunks
                    result_file.write(chunk)
        dirname = os.path.splitext(filename)[0]
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            zip_ref.extractall(dirname)
        os.remove(filename)
    except Exception as e:
        print 'error getting results: {}'.format(str(e))
        sys.exit(2)

    print '{} - results stored in {}'.format(r.status_code, dirname)

    # clean the job
    request_url = conf_dict['server'] + ':' + str(conf_dict['port']) + '/jobs'
    request_url += '?id=' + args.id

    try:
        r = requests.delete(request_url, cert=conf_dict['proxy'], verify=conf_dict['cadir'])
    except Exception as e:
        print 'requests error: {}'.format(str(e))
        sys.exit(5)

    if r.status_code != 200:
        print 'error cleaning job: {}'.format(r.text)
        sys.exit(4)
else:
    print '{} - {}'.format(r.status_code, r.text)
    sys.exit(4)
