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
from sqlalchemy import create_engine,\
        MetaData,\
        Table,\
        Column,\
        Integer,\
        String,\
        TIMESTAMP,\
        text,\
        DDL
from sqlalchemy.engine import reflection
import logging

def run():
    from mysql.connector.connection import MySQLConnection

    MySQLConnection.get_characterset_info = MySQLConnection.get_charset

    db = create_engine("mysql+mysqlconnector://root:@localhost/hydradb")
    db.echo = True
    db.connect()
    metadata = MetaData(db)


    insp = reflection.Inspector.from_engine(db)

    tables = []
    for table_name in insp.get_table_names():
        if not table_name.endswith('_aud'):
            table = Table(table_name, metadata, autoload=True, autoload_with=db)
            tables.append(table)
        else:
            table = Table(table_name, metadata, autoload=True, autoload_with=db)
            table.drop(db)
            metadata.remove(table)        

    for t in tables:
        copy_table(t)

    create_triggers(db, tables)
    metadata.create_all()

def create_triggers(db, tables):


    from mysql.connector.connection import MySQLConnection

    MySQLConnection.get_characterset_info = MySQLConnection.get_charset

    db = create_engine("mysql+mysqlconnector://root:@localhost/hydradb")
    db.echo = True
    db.connect()
    metadata = MetaData(db)


    insp = reflection.Inspector.from_engine(db)

    tables = []
    for table_name in insp.get_table_names():
        if not table_name.endswith('_aud'):
            table = Table(table_name, metadata, autoload=True, autoload_with=db)
            tables.append(table)
            #print("TABLE: %s"%table)
            #print table.__repr__
        else:
            table = Table(table_name, metadata, autoload=True, autoload_with=db)
            table.drop(db)
            metadata.remove(table)        


    drop_trigger_text = """DROP TRIGGER IF EXISTS %(trigger_name)s;"""
    for table in tables:
        pk_cols = [c.name for c in table.primary_key]
        pks = []
        for pk_col in pk_cols:
            try:
                db.execute(drop_trigger_text % {
                    'trigger_name' : table.name + "_ins_trig",
                })
            except:
                pass

        for pk_col in pk_cols:
            try:
                db.execute(drop_trigger_text % {
                    'trigger_name' : table.name + "_upd_trig",
                })
            except:
                pass
    #metadata.create_all()

    trigger_text = """
                    CREATE TRIGGER
                        %(trigger_name)s
                    AFTER %(action)s ON
                        %(table_name)s
                    FOR EACH ROW
                        BEGIN
                            select
                                count(*)
                            into
                                @rowcount
                            from
                                %(table_name)s_aud
                            where
                                %(pk)s;
                            
                           IF @rowcount >= %(upper_bound)s THEN
                                CALL export%(table_name)stofile();
                           END IF;
                            
                            INSERT INTO %(table_name)s_aud
                            SELECT
                                d.*,
                                '%(action)s',
                                NULL,
                                NOW()
                            FROM
                                %(table_name)s
                                AS d
                            WHERE
                                %(pkd)s;
                        END
                        """
    for table in tables:

        create_export_proc(db, table)

        pk_cols = [c.name for c in table.primary_key]
        pkd = []
        pk = []
        
        for pk_col in pk_cols:
            pkd.append("d.%s = NEW.%s"%(pk_col, pk_col))

        for pk_col in pk_cols:
            pk.append("%s = NEW.%s"%(pk_col, pk_col))
        
        text_dict = {
            'action'       : 'INSERT',
            'trigger_name' : table.name + "_ins_trig",
            'table_name'   : table.name,
            'pkd'           : ' and '.join(pkd),
            'pk'           : ' and '.join(pk),
            'output_file'  : '/tmp/%s_aud_'%(table.name,),
            'upper_bound'  : 100,
            'lower_bound'  : 3,
            'limit'        : 98,
        }

        logging.info(trigger_text % text_dict)
        trig_ddl = DDL(trigger_text % text_dict)
        trig_ddl.execute_at('after-create', table.metadata)  

        text_dict['action'] = 'UPDATE'
        text_dict['trigger_name'] = table.name + "_upd_trig"
        trig_ddl = DDL(trigger_text % text_dict)
        trig_ddl.execute_at('after-create', table.metadata)  

    metadata.create_all()

def create_export_proc(db, table):
        pk_cols = [c.name for c in table.primary_key]
        pk = []

        for pk_col in pk_cols:
            pk.append("%s = NEW.%s"%(pk_col, pk_col))

        text_dict = {
            'trigger_name' : table.name + "_file_export",
            'table_name'   : table.name,
            'pk'           : ' and '.join(pk),
            'output_file'  : '/tmp/%s_aud_'%(table.name,),
            'upper_bound'  : 100,
            'lower_bound'  : 3,
            'limit'        : 98,
        }

        drop_proc_text = """DROP PROCEDURE IF EXISTS export%(table_name)stofile;"""%{'table_name':table.name}
        try:
            db.execute(drop_proc_text)
        except:
            pass
        proc_text = """
            CREATE PROCEDURE export%(table_name)stofile()
            BEGIN

            SET @s = CONCAT('SELECT * INTO OUTFILE ', CONCAT('%(output_file)s', localtime),'from
                                    %(table_name)s_aud
                                where
                                    %(pk)s
                                order by cr_date desc
                                LIMIT %(limit)s;');
            PREPARE stmt2 FROM @s;
            EXECUTE stmt2;
            DEALLOCATE PREPARE stmt2;

            delete from %(table_name)s_aud
            where
                %(pk)s
            order by cr_date desc
            LIMIT %(limit)s;

            END
        """%text_dict

        trig_ddl = DDL(proc_text % text_dict)
        trig_ddl.execute_at('after-create', table.metadata)  

def copy_table(table):
    args = []
    for c in table.columns:
        col = c.copy()
        #if col.name == 'cr_date':
        #    continue
        col.primary_key=False
        col.foreign_keys = set([])
        col.server_default=None
        col.default=None
        col.nullable=True
        args.append(col)
    args.append(Column('action', String(12)))
    args.append(Column('aud_id', Integer, primary_key=True))
#    args.append(Column('aud_user_id', Integer, ForeignKey('tUser.user_id')))
    args.append(Column('aud_time', TIMESTAMP, server_default=text('LOCALTIMESTAMP')))
    return Table(table.name+"_aud", table.metadata, *args, extend_existing=True)

if __name__ == '__main__':
    logging.basicConfig(level='INFO')
    run()
   # create_triggers()
