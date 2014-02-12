import mysql.connector
import logging
from HydraLib import config
import sqlite3

global CNX
CNX = None

class HydraMySqlCursor(mysql.connector.cursor.MySQLCursor):

    def execute_sql(self, qry):
        self.execute(qry)
        return self.get_rs()

    def get_rs(self):
        rs = []

        rows      = self.fetchall()
        for row in rows:
            rs.append(HydraRSRow(zip(self.column_names, row)))
        return rs

#class HydraSqLiteCursor(sqlite3.cursor):
#
#    def execute_sql(self, qry):
#        self.execute(qry)
#        return self.get_rs()
#
#    def get_rs(self):
#        rs = []
#
#        rows = self.fetchall()
#        for row in rows:
#            rs.append(HydraRSRow(zip(self.column_names, row)))
#        return rs


class HydraRSRow(object):
    def __init__(self, rs_zip):
        self.rs_zip = rs_zip

        for k, v in rs_zip:
            self.__setattr__(k, v)

    def get_as_dict(self):
        return dict(self.rs_zip)

def connect(user=None, password=None, db_name=None):
    logging.debug("CONNECTING")
    if user is None:
        user = config.get('mysqld', 'user')
    if password is None:
        password = config.get('mysqld', 'password')
    if db_name is None:
        db_name = config.get('mysqld', 'db_name')

    global CNX

    if CNX is None:
        cnx = mysql.connector.connect(user=user,
                                      password=password,
                                      database=db_name)
        CNX = cnx
    return CNX

def commit():
    CNX.commit()

def rollback():
    CNX.rollback()

def disconnect():
    logging.debug("DIS - CONNECTING")
    global CNX
    if CNX is not None:
        CNX.disconnect()
    CNX = None


def get_connection():
    return CNX


