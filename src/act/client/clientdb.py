"""
This module defines object for managing client engine's table in database.
"""
# TODO: Check if all methods from ClientDB are still used after changes.

import arc
import logging

from act.db.aCTDB import aCTDB
from errors import *


class ClientDB(aCTDB):
    """
    Object for managing client engine's table in database.

    The way MySQL exceptions are dealt with is to log and reraise
    the exception. The reason for this is that currently ClientDB does not
    check client's input so it rather passes all info on problems to client
    to deal with them.

    Another approach would be to check input and provide simpler error
    interface, but that is not the priority yet.

    Several methods support lazy flag argument that determines whether
    transaction should be commited after query. When lazy operations are used
    (lazy=True), commit should be called manually. Coneniently, ClientDB
    has :meth:`Commit`  method (inherited from ancestors).
    """

    def __init__(self, logger=logging.getLogger(__name__), dbname='act'):
        """
        Initialize base object.

        Args:
            logger: An object for logging.
            dbname: A string with a name of aCT database.
        """
        aCTDB.__init__(self, logger, dbname)

    def createTable(self):
        """Create clientjobs table."""
        # delete table if already exists
        try:
            c = self.getCursor()
            c.execute('DROP TABLE IF EXISTS clientjobs')
        except:
            self.conn.rollback()
            self.log.exception('Error dropping clientjobs table')
            raise

        # create table
        query = """CREATE TABLE clientjobs (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            modified TIMESTAMP,
            created TIMESTAMP,
            jobname VARCHAR(255),
            jobdesc mediumtext,
            siteName VARCHAR(255),
            arcjobid integer,
            proxyid integer
        )"""
        c = self.getCursor()
        try:
            c.execute(query)
            c.execute('ALTER TABLE clientjobs ADD INDEX (arcjobid)')
        except:
            self.conn.rollback()
            self.log.exception('Error creating clientjobs table')
            raise
        else:
            self.conn.commit()

    def deleteTable(self):
        """Delete clientjobs table."""
        c = self.getCursor()
        try:
            c.execute('DROP TABLE clientjobs')
        except:
            self.conn.rollback()
            self.log.exception('Error dropping clientjobs table')
            raise
        else:
            self.conn.commit()

    def insertJob(self, jobdesc, proxyid, siteName, lazy=False):
        """
        Insert job into clientjobs table.

        Args:
            jobdesc: A string with xRSL job description.
            proxyid: ID from proxies table of a proxy that job will
                be submitted with.
            siteName: A string with name of a site in configuration 
                that job will be submitted to.
            lazy: A boolean that determines whether transaction should be
                commited after operation.

        Returns:
            ID of inserted job.
        """
        # get job name from xRSL
        jobdescs = arc.JobDescriptionList()
        # Error is not checked because caller (actsub.py) already checked
        # validity of xrsl.
        arc.JobDescription_Parse(str(jobdesc), jobdescs)
        jobname = jobdescs[0].Identification.JobName

        # insert job
        query = """
            INSERT INTO clientjobs (created, jobname, jobdesc, siteName, proxyid)
            VALUES (%s, %s, %s, %s, %s)
        """
        c = self.getCursor()
        try:
            c.execute(query, [self.getTimeStamp(), jobname, jobdesc, siteName, proxyid])
            c.execute('SELECT LAST_INSERT_ID()')
            jobid = c.fetchone()['LAST_INSERT_ID()']
        except:
            self.conn.rollback()
            self.log.exception('Error while inserting new job')
            raise
        else:
            if not lazy:
                self.conn.commit()
            return jobid

    def deleteJobs(self, where, params):
        """
        Delete jobs from table.

        Args:
            where: A string with custom WHERE clause for DELETE query. Empty
                means all rows.

        Returns:
            Number of rows deleted.
        """
        query = 'DELETE FROM clientjobs'
        if where:
            query += ' WHERE {}'.format(where)

        c = self.getCursor()
        try:
            c.execute(query, params)
        except:
            self.conn.rollback()
            self.log.exception('Error deleting jobs with query: "{}"'.format(where))
            raise
        else:
            self.conn.commit()
            return c.rowcount

    # Although function is only used inside of aCT, column checking is still
    # done in case it gets into API.
    def getJobsInfo(self, columns=[], **kwargs):
        """
        Return info for selected jobs.

        Args:
            columns: A list of column names that will be fetched.
            **kwargs: Additional arguments for SQL statement:
                where: A string with WHERE clause.
                where_params: A list with values for WHERE clause.
                order_by: A string with ORDER BY clause.
                order_by_params: A list with values for ORDER BY clause.
                limit: An integer with number of rows to be returned.

        Returns:
            A list of dictionaries of column_name:value.
        """
        if self._checkColumns('clientjobs', columns) == False:
            return None

        # query params
        params = []
        # create query
        query = 'SELECT {} FROM clientjobs '.format(
            self._column_list2str(columns))
        if 'where' in kwargs.keys():
            query += ' WHERE {} '.format(kwargs['where'])
            params.extend(kwargs['where_params'])
        if 'order_by' in kwargs.keys():
            query += ' ORDER BY {} '.format(kwargs['order_by'])
            params.extend(kwargs['order_by_params'])
        if 'limit' in kwargs.keys():
            query += ' LIMIT %s'
            params.append(kwargs['limit'])

        # execute
        c = self.getCursor()
        try:
            c.execute(query, params)
        except:
            self.log.exception('Error getting job info')
            raise
        else:
            return c.fetchall()

    def getProxies(self):
        """Return a list of all proxies in client engine's table."""
        c = self.getCursor()
        try:
            c.execute('SELECT DISTINCT(proxyid) AS proxyid FROM clientjobs')
        except:
            self.log.exception('Error getting proxies')
            raise
        else:
            rows = c.fetchall()
            return [row['proxyid'] for row in rows]

    def updateJob(self, jobid, patch, lazy=False):
        """
        Update job wih given information.
        
        Information is given as a dictionary, keys are column names and values
        are new column values.

        Args:
            jobid: ID of a job to be changed.
            patch: A dictionary with new job information. Keys are fields
                where values should be set.
            lazy: A boolean that determines whether transaction should be
                commited after operation.
        """
        if self._checkColumns('clientjobs', patch.keys()) == False:
            raise Exception("Invalid job attribute")

        query = 'UPDATE clientjobs SET '
        params = []
        for key in patch.keys():
            query += '{} = %s, '.format(key)
            params.append(patch[key])
        query = query.rstrip(', ')
        query += ' WHERE id = %s'
        params.append(jobid)

        c = self.getCursor()
        try:
            c.execute(query, params)
        except:
            self.conn.rollback()
            self.log.exception('Error updating job {}'.format(jobid))
            raise
        else:
            if not lazy:
                self.conn.commit()

    # TODO: mysql escaping
    def getJoinJobsInfo(self, clicols=[], arccols=[], **kwargs):
        """
        Return job info from ARC engine's and client engine's table inner join.

        Args:
            clicols: A list of fields from client engine's table that will
                be fetched.
            arccols: A list of fields from arc engine's table that will
                be fetched.
            **kwargs: Additional arguments for SQL statement:
                where: A string with WHERE clause.
                where_params: A list with values for WHERE clause.
                order_by: A string with ORDER BY clause.
                order_by_params: A list with values for ORDER BY clause.
                limit: An integer with number of rows to be returned.

        Returns:
            A list of dictionaries with column_name:value. Column names
            will have 'c_' prepended for columns from client engine's table
            and 'a_' for columns from ARC engine's table.
        """
        if not clicols and not arccols:
            return []

        if self._checkColumns('clientjobs', clicols) == False or \
                self._checkColumns('arcjobs', arccols) == False:
            raise Exception("Invalid columns")

        c = self.getCursor()
        query = "SELECT "
        # prepend table name for all columns and add them to query
        for col in clicols:
            query += 'c.{} AS c_{}, '.format(col, col)
        for col in arccols:
            query += 'a.{} AS a_{}, '.format(col, col)
        query = query.rstrip(', ') # strip last comma and space

        # inner join
        query += " FROM clientjobs c "
        query += " INNER JOIN arcjobs a ON c.arcjobid = a.id"

        params = []
        # select job
        if 'where' in kwargs.keys():
            query += ' WHERE {}'.format(kwargs['where'])
            params.extend(kwargs['where_params'])
        if 'order_by' in kwargs.keys():
            query += ' ORDER BY {}'.format(kwargs['order_by'])
            params.extend(kwargs['order_by_params'])
        if 'limit' in kwargs.keys():
            query += ' LIMIT %s'
            params.append(kwargs['limit'])

        c = self.getCursor()
        try:
            c.execute(query, params)
        except:
            self.log.exception('Error getting inner join for query {}'.format(query))
            raise
        else:
            return c.fetchall()

    # TODO: Most of code is duplicated, refactor!
    # TODO: mysql escaping
    def getLeftJoinJobsInfo(self, clicols=[], arccols=[], **kwargs):
        """
        Return job info from ARC engine's and client engine's table left join.

        This method is almost exactly the same as :meth:`getJoinJobsInfo`
        but it uses left join instead of inner join. That is necessary to be
        able to get info for jobs which haven't been fed to ARC engine yet.

        Args:
            clicols: A list of fields from client engine's table that will
                be fetched.
            arccols: A list of fields from arc engine's table that will
                be fetched.
            **kwargs: Additional arguments for SQL statement:
                where: A string with WHERE clause.
                where_params: A list with values for WHERE clause.
                order_by: A string with ORDER BY clause.
                order_by_params: A list with values for ORDER BY clause.
                limit: An integer with number of rows to be returned.

        Returns:
            A list of dictionaries with column_name:value. Column names
            will have 'c_' prepended for columns from client engine's table
            and 'a_' for columns from ARC engine's table.
        """
        if not clicols and not arccols:
            return []

        if self._checkColumns('clientjobs', clicols) == False or \
                self._checkColumns('arcjobs', arccols) == False:
            raise Exception("Invalid columns")

        c = self.getCursor()
        query = "SELECT "
        # prepend table name for all columns and add them to query
        for col in clicols:
            query += 'c.{} AS c_{}, '.format(col, col)
        for col in arccols:
            query += 'a.{} AS a_{}, '.format(col, col)
        query = query.rstrip(', ') # strip last comma and space

        # left join
        query += " FROM clientjobs c "
        query += "LEFT JOIN arcjobs a ON c.arcjobid = a.id"

        params = []
        # select job
        if 'where' in kwargs.keys():
            query += ' WHERE {}'.format(kwargs['where'])
            params.extend(kwargs['where_params'])
        if 'order_by' in kwargs.keys():
            query += ' ORDER BY {}'.format(kwargs['order_by'])
            params.extend(kwargs['order_by_params'])
        if 'limit' in kwargs.keys():
            query += ' LIMIT %s'
            params.append(kwargs['limit'])

        c = self.getCursor()
        try:
            c.execute(query, params)
        except:
            self.log.exception('Error getting left join for query {}'.format(query))
            raise
        else:
            return c.fetchall()

    # TODO: mysql escaping
    def getColumns(self, tableName):
        """
        Get column names for table in database.

        The reason why this method allows to get column names for any table
        in database is because other aCT engines do not provide this
        functionality. So to keep things simple, this method is used also to
        get column names for ARC engine's table.

        The most semantic solution would be that this method just returned
        column names from client engine's table and that ARC engine's object
        for database (:class:~`act.arc.aCTDBArc.aCTDBArc`) implemented
        the same interface.
        """
        c = self.getCursor()
        query = 'SHOW columns FROM {}'.format(tableName)
        try:
            c.execute(query)
        except:
            self.log.exception('Error getting columns for table {}'.format(tableName))
            raise
        else:
            rows = c.fetchall()
            return [row['Field'] for row in rows]

    def _checkColumns(self, tableName, columns):
        """Return True if all columns are in table, false otherwise."""
        tableColumns = self.getColumns(tableName)
        for col in columns:
            if col not in tableColumns:
                return False
        return True


def createMysqlEscapeList(num):
    """Create a string with a list of %s for escaping."""
    esc_str = ''
    for i in range(num):
        esc_str += '%s, '
    return esc_str.rstrip(', ')


