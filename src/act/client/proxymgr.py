"""
This is a module that provides proxy management functionality.
"""

import logging
import os
import arc
import datetime

import act.common.aCTProxy as aCTProxy
import act.arc.aCTDBArc as aCTDBArc
from act.client.errors import *

DEFAULT_PROXY_PATH = '/tmp/x509up_u'


class ProxyManager(object):
    """
    Object for managing proxies with aCT functions.

    This object tries to provide more convenient interface for proxy
    certificate management. Proxy certificates are stored by aCT in a
    dedicated table in database.

    MySQL errors that might happen are ignored unless stated otherwise where
    they are not.

    Attributes:
        logger: An object for logging.
        actproxy: An :class:~`act.common.aCTProxy.aCTProxy` object that
            provides interface to proxies table in aCT.
        arcdb: An object that is interface to ARC engine's table.
    """

    def __init__(self):
        """Initialize object."""
        self.logger = logging.getLogger(__name__)
        self.actproxy = aCTProxy.aCTProxy(self.logger)
        self.arcdb = aCTDBArc.aCTDBArc(self.logger, 'act') # TODO: hardcoded

    def getProxyInfo(self, dn, attribute='', columns=[]):
        """
        Return proxy information from database.

        Args:
            dn: A string with DN of proxy.
            attribute: A string with proxy attributes of proxy.
            columns: A list of string names of table columns.

        Returns:
            A dictionary with column_name: value entries.

        Raises:
            NoSuchProxyError: Searched for proxy is not in database.
        """
        try:
            proxyInfo =  self.actproxy.getProxyInfo(dn, attribute, columns)
        except: # probably some sort of mysql error; log and raise
            self.logger.exception('Error getting info for proxy dn={} attribute={}'.format(dn, attribute))
            raise
        else:
            if not proxyInfo:
                self.logger.error('No proxy with dn={} and attribute={}'.format(dn, attribute))
                raise NoSuchProxyError(dn, attribute)
            else:
                return proxyInfo

    def readProxyFile(self, proxyPath):
        """
        Read proxy info from file.

        Args:
            proxyPath: A string with path to proxy file.
        
        Returns:
            A tuple with proxy string, dn and expiry time."""
        try:
            return self.actproxy._readProxyFromFile(proxyPath)
        except: # probably some file reading error
            self.logger.exception('Error reading proxy file {}'.format(proxyPath))
            raise

    def readProxyString(self, proxyStr):
        """
        Read proxy info from a string.

        Extracting the proxy information is the same as in
        :meth:~`act.common.aCTProxy.aCTProxy._readProxyFromFile`.

        Args:
            proxyStr: A string with proxy content.

        Returns:
            A tuple with string DN and string expiry time.
        """
        cred_type = arc.initializeCredentialsType(arc.initializeCredentialsType.SkipCredentials)
        userconf = arc.UserConfig(cred_type)
        userconf.CredentialString(str(proxyStr))
        cred = arc.Credential(userconf)
        dn = cred.GetIdentityName()
        expirytime = datetime.datetime.strptime(
                cred.GetEndTime().str(arc.UTCTime),
                "%Y-%m-%dT%H:%M:%SZ")
        return dn, expirytime

    def updateProxy(self, proxyPath):
        """
        Update or insert given proxy, return proxyid.

        Args:
            proxyPath: A string with path to proxy file.

        Returns:
            ID of proxy in database.
        """
        proxystr, dn, exptime = self.readProxyFile(proxyPath)
        return self.actproxy.updateProxy(proxystr, dn, '', exptime)

    def getProxyIdForProxyFile(self, path=None):
        """
        Get proxy id for proxy in given file.

        If no path is given, the default location for generated proxies is
        used, which is /tmp/x509up_u<user id>.

        Args:
            path: A string with path to proxy file.

        Returns:
            Proxy ID from database.
        """
        if not path:
            path = DEFAULT_PROXY_PATH + str(os.getuid())

        __, dn, _ = self.readProxyFile(path)
        return self.getProxyInfo(dn, '', ['id'])['id']

    def getProxiesWithDN(self, dn, columns=[]):
        """
        Get info for proxies with given dn.

        Args:
            dn: A string with DN.
            columns: A list of string names of table columns.

        Returns:
            A list of dictionaries with column name:value entries for proxies.
        """
        return self.arcdb.getProxiesInfo(" dn = '{}' ".format(dn), columns)


