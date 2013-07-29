import datetime
import mysql.connector
import logging

global CNX
CNX = None

def connect(user='root', password='', db_name='hydra_initial'):
    logging.debug("CONNECTING")
    global CNX
    if CNX is None:
        cnx = mysql.connector.connect(user=user, password=password, database=db_name)
        CNX = cnx
    return CNX

def connect_tmp(user='root', password='', db_name='hydra_initial'):
    logging.debug("CONNECTING")
    return mysql.connector.connect(user=user, password=password, database=db_name)

def disconnect():
    logging.debug("DIS - CONNECTING")
    global CNX
    if CNX is not None:
        CNX.disconnect()
    CNX = None

def get_connection():
    return CNX

def load_csv(cnx, cursor, filepath):

    sql = "insert into %(table_name)s (%(col_names)s) values (%(values)s)"

    filename = filepath.split('/')[-1]

    with open(filepath, 'r') as f:
        col_names = f.readlines[0]
        for w in f.readlines()[1:]:
            w = w.strip()
            entry = w.split(',')
            params = dict(
                table_name=filename,
                col_names=col_names,
                values=entry,
            )

        dt = datetime.datetime.strptime("%s %s" % (entry[0], entry[1]),
                                        "%d/%m/%Y %H:%M:%S")

        params['time'] = dt

        cursor.execute(sql, params)
        cnx.commit()
        cursor.close()
        cnx.close()
