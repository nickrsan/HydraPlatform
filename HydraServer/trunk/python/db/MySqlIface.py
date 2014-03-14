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
import logging
from decimal import Decimal
from HydraLib import config
from hdb import HydraMySqlCursor
from HydraLib.HydraException import HydraError
from mysql.connector import DatabaseError
import pytz

global DB_STRUCT
DB_STRUCT = {}

global CONNECTION
CONNECTION = None

global DB_HIERARCHY
DB_HIERARCHY = None

#Used to count how often a single piece of sql is called.
global SQL_CALL_COUNT
SQL_CALL_COUNT = {}

#Used to count how often the results of a single piece of sql change.
global SQL_RESULT_DIFF_COUNT
SQL_RESULT_DIFF_COUNT = {}

def execute(sql):
#    update_call_count(sql)

    cursor = CONNECTION.cursor(cursor_class=HydraMySqlCursor)
    rs = cursor.execute_sql(sql)

    #Keep track of result sets changing for a given piece of sql.
    #If the SQL never changes, we should be caching the result.
 #   update_rs_count(sql,rs)

    cursor.close()

    return rs

def update_call_count(sql):
    global SQL_CALL_COUNT

    if sql in SQL_CALL_COUNT.keys():
        SQL_CALL_COUNT[sql] += 1
    else:
        SQL_CALL_COUNT[sql] = 1

def update_rs_count(sql, new_rs):
    global SQL_RESULT_DIFF_COUNT
    new = [r.get_as_dict() for r in new_rs]

    if sql in SQL_RESULT_DIFF_COUNT.keys():
        old_rs, count = SQL_RESULT_DIFF_COUNT[sql]
        old = [r.get_as_dict() for r in old_rs]
        if old != new:
            count = count + 1
            SQL_RESULT_DIFF_COUNT[sql] = (new_rs, count)
    else:
        SQL_RESULT_DIFF_COUNT[sql] = (new_rs, 0)

def init(cnx, db_hierarchy):

    global CONNECTION
    global DB_STRUCT
    global DB_HIERARCHY

    DB_HIERARCHY = db_hierarchy

    CONNECTION = cnx

    sql = """
        select
            table_name,
            column_name,
            column_default,
            is_nullable,
            column_type,
            column_key,
            extra
        from
            information_schema.columns
        where
            table_schema = '%s'
    """%(config.get('mysqld', 'db_name'),)

    cursor = CONNECTION.cursor(cursor_class=HydraMySqlCursor)
    #rs = cursor.execute_sql(sql)
    cursor.execute(sql)

    #Table desc gives us:
    #[...,(col_name, col_type, nullable, key ('PRI' or 'MUL'), default, auto_increment),...]
    for r in cursor.fetchall():
        table_name     = r[0]
        column_name    = r[1]
        column_default = r[2]
        is_nullable    = r[3]
        column_type    = r[4]
        column_key     = r[5]
        extra          = r[6]

        col_info    = {}

        col_info['default']        = column_default
        col_info['nullable']       = True if is_nullable == 'YES' else False
        col_info['type']           = column_type
        col_info['primary_key']    = True if column_key == 'PRI' else False
        col_info['auto_increment'] = True if extra == 'auto_increment' else False

        tab_info = DB_STRUCT.get(table_name.lower(), {'columns' : {}})
        tab_info['columns'][column_name] = col_info
        tab_info['child_info'] = {}
        DB_STRUCT[table_name.lower()] = tab_info

    fk_qry    = """
        select
            table_name,
            column_name,
            referenced_table_name,
            referenced_column_name
        from
            information_schema.key_column_usage
        where
        table_schema  = '%s'
        and referenced_table_name is not null
        and constraint_name != 'PRIMARY'
        and constraint_name != column_name
    """%(config.get('mysqld', 'db_name'),)

    cursor.execute(fk_qry)
    for r in cursor.fetchall():
        child_dict = {}

        child_dict['column_name'] = r[1]
        child_dict['referenced_column_name'] = r[3]
        DB_STRUCT[r[2].lower()]['child_info'][r[0]] = child_dict
        #DB_STRUCT[r[2].lower()]['child_info'][r[0].lower()] = child_dict

def bulk_insert(objs, table_name=""):
    """
        Insert all objects into the DB using a single query.
        to_insert is a list of objects, which much be the same type.
    """

    def get_val(attr, db_type):
        val = attr
        if val is None:
            return None
        elif db_type.find('varchar') != -1 or db_type in ('blob', 'datetime', 'timestamp') :
            return '%s'%val
        elif db_type == 'text':
            return repr(val)
        elif db_type.lower().find('double') >= 0 or db_type.lower().find('decimal') >= 0:
            return str(val)
        else:
            return val

    if len(objs) == 0:
        logging.info("Cannot bulk insert to %s. Nothing to insert!", table_name)
        return

    cursor = CONNECTION.cursor(cursor_class=HydraMySqlCursor)
    base_insert = "insert into %(table)s (%(cols)s) values (%(vals)s)"

    vals = []
    for obj in objs:
        if 'cr_date' in obj.db.db_attrs:
            del(obj.db.db_attrs[obj.db.db_attrs.index('cr_date')])

        val = [get_val(getattr(obj.db, n), obj.db.attr_types[n]) for n in obj.db.db_attrs]
        vals.append(tuple(val))

    complete_insert = base_insert % dict(
        table = objs[0].db.table_name,
        cols  = ",".join([n for n in objs[0].db.db_attrs]),
        vals  = ",".join('%s' for v in objs[0].db.db_attrs),
    )

    logging.info("Running bulk insert into %s with %s values",
                                                objs[0].db.table_name,
                                                len(vals))

    cursor.executemany(complete_insert, vals)

    warnings = cursor._fetch_warnings()
    if warnings is not None:
        raise HydraError("Bulk insert created Warnings: %s"%warnings)

    #the executemany seems to return the bottom index, rather than the top,
    #so we have to work out the top index.
    last_row_id = cursor.lastrowid + cursor.rowcount-1

    cursor.close()

    return last_row_id

class DBIface(object):
    """
        The class which represents the actual row in the database. Accessed
        from the base class using '.db'
    """

    def __init__(self, class_name):

        self.db_attrs = []
        self.pk_attrs = []
        self.db_data  = {}
        self.nullable_attrs = []
        self.attr_types = {}
        self.seq = None

        #This indicates whether any values in this table have changed.
        self.has_changed = False

        #this turns 'Project' into 'tProject' so it can identify the table
        self.class_name = class_name.lower()
        self.table_name = DB_HIERARCHY[self.class_name]['table_name']

        self.db_struct = DB_STRUCT
        self.db_hierarchy = DB_HIERARCHY

        col_dict = DB_STRUCT[self.table_name.lower()]['columns']

        for col_name, col_data in col_dict.items():

            self.db_attrs.append(col_name)

            #[...,(col_name, col_type, nullable, key ('PRI' or 'MUL'), default, auto_increment),...]

            self.db_data[col_name] = col_data['default']

            self.attr_types[col_name] = col_data['type']

            if col_data['nullable'] is True:
                self.nullable_attrs.append(col_name)

            if col_data['primary_key'] is True:
                self.pk_attrs.append(col_name)

            if col_data['auto_increment'] is True:
                self.seq = col_name

    def load_children(self, child_info_dict):
        child_dict = {}
        for table_name, ref_cols in child_info_dict.items():
            column_name = ref_cols['column_name']
            referenced_column_name = ref_cols['referenced_column_name']

            logging.debug("Loading child %s", table_name)

            base_child_sql = """
                select
                    *
                from
                    %(table)s
                where
                %(fk_col)s = %(fk_val)s
                order by
                    %(fk_col)s
            """

            if self.__getattr__(referenced_column_name) is None:
                continue

            complete_child_sql = base_child_sql % dict(
                table  = table_name,
                fk_col = column_name,
                fk_val = self.__getattr__(referenced_column_name)
            )

            cursor = CONNECTION.cursor()
            #rs = cursor.execute_sql(complete_child_sql)
            cursor.execute(complete_child_sql)
            rs = cursor.fetchall()
            child_rs = []
            for r in rs:
                child_rs.append(zip(cursor.column_names, r))

            cursor.close()

            child_dict[table_name] = child_rs
        return child_dict

    def get_parent(self):
        parent_key = DB_HIERARCHY[self.class_name]['parent']

        if parent_key is None:
            return None

        #logging.debug("PARENT: %s", parent_key)

        parent_class = DB_HIERARCHY[parent_key]['obj']
        parent_table = DB_HIERARCHY[parent_key]['table_name']
        parent_pk    = DB_HIERARCHY[parent_key]['pk']

        for k in parent_pk:
            if self.__getattr__(k) is None:
                logging.debug("Cannot load parent. %s is None", k)
                return

        logging.debug("%s Loading parent %s", self.table_name, parent_key)

        base_parent_sql = """
            select
                *
            from
                %(table)s
            where
                %(pk)s
        """

        complete_parent_sql = base_parent_sql % dict(
                table = parent_table,
                pk    = " and ".join(["%s = %s"%(n, self.get_val(n)) for n in parent_pk]),
        )

        rs = execute(complete_parent_sql)

        if len(rs) == 0:
            logging.info("Object %s has no parent with pk: %s", self.table_name, parent_pk)
            return None

        parent_obj = parent_class()
        logging.debug(rs[0].get_as_dict())
        for k, v in rs[0].get_as_dict().items():
            parent_obj.db.__setattr__(k, v)

        parent_obj.load()

        return parent_obj

    def __getattr__(self, name):
        """
            Return the value containe id db_data if it exists as
            a key in this dictionary. Return the value as the appropriate
            type, based on the type specified in the table
        """
        db_type = self.attr_types[name]

        if name in self.db_attrs:

            #Don't do a cast if there is no value to cast...
            if self.db_data[name] is None:
                return None

            val = self.db_data[name]

            #Cast the value to the correct DB data type
            if db_type.lower().find("double") != -1 or db_type.lower().find('decimal') >= 0:
                if type(val) == Decimal:
                    return val
                elif type(val) == str:
                    return Decimal(val)
                else:
                    #otherwise it's a float. To preserve precision repr needs
                    #to be used
                    return Decimal(repr(val))
            elif db_type.find('int') != -1:
                return int(str(val))
            elif db_type == 'blob':
                try:
                    return eval(str(val))
                except:
                    return str(val)
            elif db_type == 'datetime':
                dt = self.db_data[name]
                return dt

            return str(val)

        else:
            raise AttributeError("Attribute %s not set."%name)

    def __setattr__(self, name, value):
        if name != 'db_attrs' and name in self.db_attrs:
            #old_val = self.db_data[name]
            old_val = self.__getattr__(name)
            if value != old_val:
                self.db_data[name] = value
                self.has_changed = True
        else:
            super(DBIface, self).__setattr__(name, value)


    def insert(self):
        """
            If this object has not been stored in the DB as yet, then insert it.
            Generates an insert statement and runs it.
        """

        cursor = CONNECTION.cursor(cursor_class=HydraMySqlCursor)
        base_insert = "insert into %(table)s (%(cols)s) values (%(vals)s);"


        if 'cr_date' in self.db_attrs:
            del(self.db_attrs[self.db_attrs.index('cr_date')])

        complete_insert = base_insert % dict(
            table = self.table_name,
            cols  = ",".join([n for n in self.db_attrs]),
            vals  = ",".join([self.get_val(n) for n in self.db_attrs]),
        )

        logging.info("Running insert: %s", complete_insert)
        old_seq = cursor.lastrowid

        try:
            cursor.execute(complete_insert)
        except DatabaseError, e:
            raise HydraError("Error inserting into %s: %s"%(self.table_name, e.msg))

        if self.seq is not None:
            if old_seq is None or old_seq != cursor.lastrowid:
                setattr(self, self.seq, cursor.lastrowid)
        cursor.close()

        self.has_changed = False

    def update(self):
        """
            Updates all the values for a table in the DB..
            Generates an update statement and runs it.
        """

        base_update = "update %(table)s set %(sets)s where %(pk)s;"
        complete_update = base_update % dict(
            table = self.table_name,
            sets  = ",".join(["%s = %s"%(n, self.get_val(n)) for n in self.db_attrs]),
            pk    = " and ".join(["%s = %s"%(n, self.get_val(n)) for n in self.pk_attrs]),
        )
        logging.info("Running update: %s", complete_update)
        cursor = CONNECTION.cursor(cursor_class=HydraMySqlCursor)
        cursor.execute(complete_update)
        cursor.close()

        self.has_changed = False

    def load(self):
        """
            Loads a row from the DB and assigns the values as entries to
            the self.db_data dictionary. These are accessible direcly from
            the object, without any need to look in the db_data dictionary.
        """
        #Idenitfy the primary key, which is used to get a single row in the db.
        for pk in self.pk_attrs:
            if self.__getattr__(pk) is None:
                logging.warning("%s: Primary key is not set. Cannot load row from DB.", self.table_name)
                return False

        #Create a skeleton query
        base_load = "select * from %(table_name)s where %(pk)s;"

        #Fill in the query with the appropriate table name and PK values.
        complete_load = base_load % dict(
            table_name = self.table_name,
            pk         = " and ".join(["%s = %s"%(n, self.get_val(n)) for n in self.pk_attrs]),
        )

        logging.debug("Running load: %s", complete_load)

        cursor = CONNECTION.cursor(cursor_class=HydraMySqlCursor)
        rs = cursor.execute_sql(complete_load)
        cursor.close()

        if len(rs) == 0:
            logging.warning("No entry found for table %s", self.table_name)

            return False

        for r in rs:
            for k, v in r.get_as_dict().items():
                #logging.debug("Setting column %s to %s", k, v)
                self.db_data[k] = v

        self.has_changed = False

        return True

    def delete(self):
        """
            Deletes this object's row from the DB.
        """
        base_load = "delete from %(table_name)s where %(pk)s;"
        complete_delete = base_load % dict(
            table_name = self.table_name,
            pk         = " and ".join(["%s = %s"%(n, self.get_val(n)) for n in self.pk_attrs]),
        )
        logging.info("Running delete: %s", complete_delete)
        cursor = CONNECTION.cursor(cursor_class=HydraMySqlCursor)
        cursor.execute(complete_delete)
        cursor.close()

    def get_val(self, attr):
        val = self.__getattr__(attr)
        db_type = self.attr_types[attr]

        if val is None:
            return 'null'
        elif db_type.find('varchar') != -1 or db_type in ('blob', 'datetime', 'timestamp') :
            return "'%s'"%val
        elif db_type == 'text':
            return repr(val)
        else:
            return str(val)

    def commit(self):
        CONNECTION.commit()
