"""
Flask application module for aCT's RESTful interface.

Functions anotated with @app.route are ones that respond to requests.
All of them require client certificate to authenticate user and allow
operations. The client certificate has to be a proxy certificate that is
created by user. The only exception is request that submits a new proxy
certificate: this one requires user to use personal certificate.

There are also some utility functions which are common to all response
functions.
"""

import act.client.jobmgr as jobmgr
import act.client.proxymgr as proxymgr
import act.client.clientdb as clientdb
from act.client.errors import *

import json
import os
import shutil
import io

from OpenSSL.crypto import load_certificate, load_privatekey
from OpenSSL.crypto import X509Store, X509StoreContext
from OpenSSL.crypto import FILETYPE_PEM
from six import u, b, binary_type, PY3


from flask import Flask, request, send_file, jsonify
app = Flask(__name__)

@app.route('/test', methods=['GET', 'POST'])
def test():
    print request.form
    print request.files
    for arg in request.args:
        print '{}: {}'.format(arg, request.args[arg])
    return "Hello World!"

@app.route('/jobs', methods=['GET'])
def stat():
    """
    Return status info for jobs in JSON format.

    There are several parameters that can be given in URL. Possible
    filtering parameters are: 
        'id': a list of job IDs
        'name': a substring that has to be present in job names
        'state': state that jobs have to be in

    There are also two parameters that define which attributes should be
    returned:
        'client': a list of column names from client table
        'arc': a list of column names from arc table

    Returns:
        status 200: A JSON list of JSON objects with jobs' status info.
        status 4**: A string with error message.
    """
    try:
        proxyid = getProxyId()
    except NoSuchProxyError:
        return 'Wrong or no client certificate', 401

    try:
        jobids = getIDs()
    except Exception:
        return 'Invalid id parameter', 400

    name_filter = request.args.get('name', default='')
    state_filter = request.args.get('state', default='')

    clicols = request.args.get('client', default=[])
    if clicols:
        clicols = clicols.split(',')

    arccols = request.args.get('arc', default=[])
    if arccols:
        arccols = arccols.split(',')

    jmgr = jobmgr.JobManager()
    try:
        jobdicts = jmgr.getJobStats(proxyid, jobids, state_filter, name_filter, clicols, arccols)
    except Exception as e:
        # TODO: could also be server error
        return str(e), 400
    else:
        return json.dumps(jobdicts)


@app.route('/jobs', methods=['DELETE'])
def clean():
    """
    Clean jobs that satisfy parameters in current request context.

    Parameters are given in request URL, they are:
        'id': a list of job IDs
        'name': a substring that has to be present in job names
        'state': state that jobs have to be in

    Returns:
        status 200: A string with number of cleaned jobs.
        status 401: A string with error message.
    """
    try:
        proxyid = getProxyId()
    except NoSuchProxyError:
        return 'Wrong or no client certificate', 401

    try:
        jobids = getIDs()
    except Exception:
        return 'Invalid id parameter', 400

    name_filter = request.args.get('name', default='')
    state_filter = request.args.get('state', default='')

    jmgr = jobmgr.JobManager()
    numDeleted = jmgr.cleanJobs(proxyid, jobids, state_filter, name_filter)
    return json.dumps(numDeleted)


@app.route('/jobs', methods=['PATCH'])
def patch():
    """
    Set jobs' state based on request parameters.

    Parameter that defines operation is passed in body of request in
    JSON format. It is a JSON object with a single property "arcstate",
    that has to be one of possible settable states
    (for instance {"arcstate": "tofetch"}).

    Other parameters are passed in URL, they are:
        'id': a list of job IDs
        'name': a substring that has to be present in job names
        'state': state that jobs have to be in

    Returns:
        status 200: A string with a number of affected jobs.
        status 4**: A string with error message.
    """
    try:
        proxyid = getProxyId()
    except NoSuchProxyError:
        return 'Wrong or no client certificate', 401

    try:
        jobids = getIDs()
    except Exception:
        return 'Invalid id parameter', 400

    name_filter = request.args.get('name', default='')
    state_filter = request.args.get('state', default='')

    jmgr = jobmgr.JobManager()

    # force ignores incomptable mimetype, silent returns None instead of
    # calling on_json_loading_failed() of request object
    patch = request.get_json(force=True, silent=True)
    if not patch:
        return 'Request data is not valid JSON', 400

    try:
        arcstate = patch['arcstate']
    except KeyError:
        return "Request data has no 'arcstate' property", 400

    if arcstate == 'tofetch':
        num = jmgr.fetchJobs(proxyid, jobids, name_filter)
    elif arcstate == 'tocancel':
        num = jmgr.killJobs(proxyid, jobids, state_filter, name_filter)
    elif arcstate == 'toresubmit':
        num = jmgr.resubmitJobs(proxyid, jobids, name_filter)
    else:
        return "'arcstate' should be either 'tofetch' or 'tocancel' or 'toresubmit'", 400
    return json.dumps(num)


# TODO: is it secure to return exception messages?
@app.route('/jobs', methods=['POST'])
def submit():
    """
    Submit job from current request context.

    Submission of jobs is done with multipart/form-data POST request.
    Job is submitted as a form containing xRSL file (name="xrsl")
    and site name (name="site").

    Returns:
        status 200: A string with id of submitted job.
        status 4** or 500: A string with error message.
    """
    try:
        proxyid = getProxyId()
    except NoSuchProxyError:
        return 'Wrong or no client certificate', 401

    jmgr = jobmgr.JobManager()

    site = request.form.get('site', None)
    if not site:
        return 'No site given', 400
    xrsl_file = request.files.get('xrsl', None)
    if not xrsl_file:
        return 'No job description file given', 400
    jobdesc = xrsl_file.read()
    try:
        jobmgr.checkJobDesc(jobdesc)
        jobmgr.checkSite(site)
    except jobmgr.InvalidJobDescriptionError as e:
        return 'Invalid job description', 400
    except jobmgr.NoSuchSiteError as e:
        return 'Invalid site', 400
    else:
        clidb = clientdb.ClientDB()
        try:
            jobid = clidb.insertJob(jobdesc, proxyid, site)
        except Exception as e:
            return 'Server error: {}'.format(str(e)), 500
        else:
            return str(jobid)


@app.route('/results', methods=['GET'])
def getResults():
    """
    Return a .zip archive of job results folder.

    Request potentionaly accepts a list of IDs but only the first one is
    processed because it's easier to respond for one job.

    Function creates a .zip archive of job results.
    .zip archive could just be sent in response but since response is the last
    thing that function does, there is no simple way of deleting archive
    afterwards and cleaning the job. The solution is therefore to create an
    archive, read it's binary presentation (read it as a binary file) to
    memory, remove the archive and send byte stream as a file to the client.

    Returns:
        status 200: A .zip archive of job results.
        status 4** or 500: A string with error message.
    """
    try:
        proxyid = getProxyId()
    except NoSuchProxyError:
        return 'Wrong or no client certificate', 401

    try:
        jobids = getIDs()
    except Exception:
        return 'Invalid id parameter', 400
    if not jobids:
        return 'No job ID given', 400
    jobid = [jobids[0]] # only take first job

    # get job results
    jmgr = jobmgr.JobManager()
    results = jmgr.getJobs(proxyid, jobid)
    if not results.jobdicts:
        return 'Results for job not found', 404
    resultDir = results.jobdicts[0]['dir']

    # create archive and read bytes
    # TODO: exception handling is not optimal, there should be finally
    # part that closes file but that would require more understanding of
    # the exceptions raised for different functions which is impossible
    # to find in python documentation.
    # TODO: should probably split this for more systematic error handling
    try:
        archivePath = shutil.make_archive(resultDir, 'zip', resultDir)
        archive = open(archivePath, 'rb')
        byteStream = io.BytesIO(archive.read())
        archive.close()
        os.remove(archivePath)
    except Exception as e:
        return 'Server error: {}'.format(str(e)), 500

    return send_file(byteStream,
            mimetype='application/zip',
            as_attachment=True,
            attachment_filename=archivePath.split('/')[-1])


@app.route('/proxies', methods=['PUT'])
def submitProxy():
    """
    Submit a proxy certificate of a user.

    This operation requires user to provide personal certificate as
    client certificate in request. Proxy certificate that is to be submitted
    should be provided as raw data in request body.

    Function first verifies user's personal certificate against a
    CA certificate. If verification is successful, it procedes to read proxy
    certificate in body and insert it to database.

    Returns:
        status 200: A string with ID of a proxy certificate.
        status 401 or 500: A string with error message.
    """
    # The following validation procedure is done as per:
    # https://stackoverflow.com/questions/30700348/how-to-validate-verify-an-x509-certificate-chain-of-trust-in-python

    # user pem is client certificate in header
    user_pem = getCertString()
    if not user_pem:
        return 'Wrong or no client certificate', 401

    # get pem for CA
    caPath = os.path.join(os.environ['PATH'].split(':')[-1], 'ca.pem')
    try:
        caFile = open(caPath, 'r') # TODO: ca.pem in bin directory
    except Exception as e:
        return 'Server error: {}'.format(str(e)), 500
    else:
        root_pem = caFile.read()
        caFile.close()

    # verify
    root_cert = load_certificate(FILETYPE_PEM, root_pem)
    user_cert = load_certificate(FILETYPE_PEM, user_pem)
    store = X509Store()
    store.add_cert(root_cert)
    store_ctx = X509StoreContext(store, user_cert)
    try:
        store_ctx.verify_certificate()
    except Exception as e:
        return 'Client certificate verification failed', 401

    pmgr = proxymgr.ProxyManager() # TODO: handle error with pmgr and jmgr

    try:
        # TODO: ARC API does not fail when given genproxy script as proxy!!!!
        proxyStr = request.get_data()
        dn, exp_time = pmgr.readProxyString(proxyStr)
        proxyid = pmgr.actproxy.updateProxy(proxyStr, dn, '', exp_time)
    except Exception as e:
        return 'Server error: {}'.format(str(e)), 500
    else:
        return json.dumps(proxyid)


@app.route('/proxies', methods=['GET'])
def getProxies():
    """
    Return information on proxies.

    Currently there are no parameters that would allow users to select which
    columns should be fetched from table.

    Returns:
        JSON list of JSON objects with proxy information (status 200).
    """
    dn = getCertDN()
    pmgr = proxymgr.ProxyManager()
    proxies = pmgr.getProxiesWithDN(dn, columns=['id', 'attribute'])
    return json.dumps(proxies)


# TODO: not most efficiently done if user has a lot of proxies: could
# turn around the loops
@app.route('/proxies', methods=['DELETE'])
def deleteProxies():
    """
    Delete proxies from database.

    Parameter has to be given in url: 'id' which is a list of proxy IDs that
    should be deleted.

    Function first fetches all proxies that match the DN of a certificate
    from request. Then it deletes those whose IDs are in 'id' parameter.
    This is done so that user cannot delete any proxies but his own.

    Returns:
        status 200: A string with a number of deleted proxies.
        status 401: A string with error message.
    """
    dn = getCertDN()
    pmgr = proxymgr.ProxyManager()
    jmgr = jobmgr.JobManager()
    proxies = pmgr.getProxiesWithDN(dn, columns=['id'])

    try:
        proxyids = getIDs()
    except Exception:
        return 'Invalid id parameter', 400
    if not proxyids:
        return 'Wrong or no client certificate', 401

    numDeleted = 0
    for proxy in proxies:
        if proxy['id'] in proxyids:
            # do not remove a proxy on which jobs depend
            if not jmgr.getJobStats(proxy['id'], [], '', '', clicols=['id']):
                pmgr.arcdb.deleteProxy(proxy['id'])
                proxyids.remove(proxy['id']) # optimize a little bit
                numDeleted += 1
    return json.dumps(numDeleted)


def getProxyId():
    """Get proxy id from proxy info in current request context."""
    pmgr = proxymgr.ProxyManager()
    dn = getCertDN()
    proxyid = pmgr.getProxyInfo(dn, '', ['id'])['id']
    return proxyid


def getCertDN():
    """Get cert DN from cert in current request context."""
    pmgr = proxymgr.ProxyManager()
    cert = getCertString()
    dn, _ = pmgr.readProxyString(cert)
    return dn


def getCertString():
    """
    Get cert string from current request context.

    Function expects certificate to be in header as X-Ssl-Client-Cert.
    """
    cert = request.headers.get('X-Ssl-Client-Cert', default='')
    fixedCert = fixCertStr(cert)
    return fixedCert


def getIDs():
    """
    Get IDs from current request context.

    IDs are taken from 'id' url parameter. Exceptions for getJobsFromList
    are handled by callers so they can generate appropriate responses.
    """
    ids = request.args.get('id', default=[])
    if ids:
        return jobmgr.getIDsFromList(ids)
    else:
        return []


def fixCertStr(certStr):
    """
    Remove spaces after newlines.

    When certificate is put in header by NGINX, one space is put after each
    newline character, which breaks certificate. This function fixes
    certificate string by removing those spaces.
    """
    newCertStr = certStr.replace('\n ', '\n')
    return newCertStr


