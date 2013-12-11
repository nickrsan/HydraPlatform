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
            #print("TABLE: %s"%table)
            #print table.__repr__
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

    ins_trigger_text = """CREATE TRIGGER %(trigger_name)s AFTER INSERT ON %(table_name)s FOR EACH ROW INSERT INTO %(table_name)s_aud SELECT d.*, 'insert', NULL, NOW() FROM %(table_name)s AS d where %(pks)s;"""
    for table in tables:
        pk_cols = [c.name for c in table.primary_key]
        pks = []
        for pk_col in pk_cols:
            pks.append("d.%s = NEW.%s"%(pk_col, pk_col))
        trig_ddl = DDL(ins_trigger_text % {
            'trigger_name' : table.name + "_ins_trig",
            'table_name'   : table.name,
            'pks'          : ' and '.join(pks)
        })
        
        trig_ddl.execute_at('after-create', table.metadata)  

    upd_trigger_text = """CREATE TRIGGER %(trigger_name)s AFTER UPDATE ON %(table_name)s FOR EACH ROW INSERT INTO %(table_name)s_aud SELECT d.*, 'update', NULL, NOW() FROM %(table_name)s AS d where %(pks)s;"""
    for table in tables:
        pk_cols = [c.name for c in table.primary_key]
        pks = []
        for pk_col in pk_cols:
            pks.append("d.%s = NEW.%s"%(pk_col, pk_col))
        trig_ddl = DDL(upd_trigger_text % {
            'trigger_name' : table.name + "_upd_trig",
            'table_name'   : table.name,
            'pks'          : ' and '.join(pks)
        })
        trig_ddl.execute_at('after-create', table.metadata)  

    metadata.create_all()

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
