# (c) Copyright 2013, 2014, University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
import mysql.connector
import logging
from HydraLib import config
import sqlite3

global CNX
CNX = None


def make_param(param_list):
    if len(param_list) == 0:
        return "(0)"#Needs to be 0 because 0 is never an ID and sql will not accept an empty list.
    elif len(param_list) == 1:
        return "(%s)"%(param_list[0])
    else:
        return str(tuple(param_list))

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


