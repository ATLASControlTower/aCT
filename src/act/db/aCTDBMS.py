def getDB(log, config):
    '''Factory method for getting specific DB implementation'''

    supported_dbms = {}

    try:
        from .aCTDBSqlite import aCTDBSqlite
        supported_dbms['sqlite'] = aCTDBSqlite
    except:
        pass
    try:
        from .aCTDBMySQL import aCTDBMySQL
        supported_dbms['mysql'] = aCTDBMySQL
    except:
        pass
    try:
        from .aCTDBOracle import aCTDBOracle
        supported_dbms['oracle'] = aCTDBOracle
    except:
        pass

    dbtype = config.get(('db', 'type')).lower()
    if dbtype not in supported_dbms:
        raise Exception("DB type %s is not implemented." % dbtype)
    return supported_dbms[dbtype](log, config)


class aCTDBMS(object):
    '''
    Class for generic DB Mgmt System db operations. Specific subclasses
    implement methods for their own database implementation.
    '''

    def __init__(self, log, config):
        self.log = log
        self.socket = str(config.get(('db', 'socket')))
        self.dbname = str(config.get(('db', 'name')))
        self.user =   str(config.get(('db', 'user')))
        self.passwd = str(config.get(('db', 'password')))
        self.host =   str(config.get(('db', 'host')))
        self.port =   str(config.get(('db', 'port')))

    # Each subclass must implement the 6 methods below
    def getCursor(self):
        raise Exception("Method not implemented")

    def timeStampLessThan(self,column,timediff):
        raise Exception("Method not implemented")

    def timeStampGreaterThan(self,column,timediff):
        raise Exception("Method not implemented")

    def addLock(self):
        raise Exception("Method not implemented")

    def getMutexLock(self, lock_name, timeout=2):
        raise Exception("Method not implemented")

    def releaseMutexLock(self, lock_name):
        raise Exception("Method not implemented")
