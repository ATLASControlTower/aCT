from act.db.aCTDBMS import aCTDBMS

class aCTDBOracle(aCTDBMS):
    """Class for Oracle specific db operations."""

    def __init__(self, log, conf):
        raise Exception("Oracle class is not implemented yet")

    def close(self):
        pass

    def getCursor(self):
        raise Exception("Oracle class is not implemented yet")

    def timeStampLessThan(self, column, timediff):
        # should be tested...
        return "(SYSDATE - TO_DATE('01-JAN-1970','DD-MON-YYYY')) * (86400)"

    def timeStampGreaterThan(self, column, timediff):
        # should be tested...
        return "(SYSDATE - TO_DATE('01-JAN-1970','DD-MON-YYYY')) * (86400)"

    def addLock(self):
        return " FOR UPDATE"

    def getMutexLock(self, lock_name, timeout=2):
        """
        Function to get named lock. Returns 1 if lock was obtained, 0 if attempt timed out, None if error occured.
        """
        # don't know how to do mutex in oracle
        return None

    def releaseMutexLock(self, lock_name):
        """
        Function to release named lock. Returns 1 if lock was released, 0 if someone else owns the lock, None if error occured.
        """
        # don't know how to do mutex in oracle
        return None
