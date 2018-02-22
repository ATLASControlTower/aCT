#!/usr/bin/env python

import mysql.connector
import logging

import act.common.aCTConfig as aCTConfig
import act.client.clientdb as clientdb
import act.arc.aCTDBArc as aCTDBArc

# get db connection info
conf = aCTConfig.aCTConfigARC()
socket = conf.get(['db', 'socket'])
dbname = conf.get(['db', 'name'])

# connect to mysql
print 'Connecting to mysql ...'
conn = mysql.connector.connect(unix_socket=socket)

# create database if it doesn't exist
print 'Creating database {} ...'.format(dbname)
cursor = conn.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(dbname))
conn.commit()

# create tables for in database
print 'Creating aCT tables ...'
clidb = clientdb.ClientDB(dbname=dbname)
arcdb = aCTDBArc.aCTDBArc(logging.getLogger(__name__), dbname=dbname)
clidb.createTable()
arcdb.createTables()
conn.commit()
