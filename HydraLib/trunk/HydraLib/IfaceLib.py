import logging
from decimal import Decimal
from soap_server import hydra_complexmodels
import datetime
import config

import MySqlIface

global DB

def init(cnx, db_hierarchy):
    global DB
    db_instance = config.get('db', 'instance')
    if db_instance == 'MySQL':
        from MySqlIface import DBIface as DB 
        import MySqlIface as DBIface
    elif db_instance == 'SQLite':
        from SQLiteIface import DBIface as DB
        import SQLiteIface as DBIface
    
    DBIface.init(cnx, db_hierarchy)

def execute(sql):
    return MySqlIface.execute(sql)

def bulk_insert(objs, table_name=""):
    return MySqlIface.bulk_insert(objs, table_name=table_name)

class IfaceBase(object):
    """
        The base database interface class.
    """
    def __init__(self, parent, class_name):
        global DB
    
        logging.debug("Initialising %s", class_name)

        self.db = DB(class_name)

        self.name = class_name

        self.in_db = False

        #Indicates that the 'delete' function has been called on this object
        self.deleted = False

        self.children   = []
        self.child_info = self.get_children()
        self.parent = parent

    def load(self):
        if self.in_db is False:
            self.in_db = self.db.load()

            if self.parent is None:
                self.get_parent()

        return self.in_db

    def load_all(self):
        self.in_db = self.load()

        if self.in_db:
            self.load_children()

        return self.in_db

    def commit(self):
        """
            Commit any inserts or updates to the DB. No going back from here...
        """
        self.db.commit()
        if self.deleted == True:
            self.in_db = False

    def delete(self):
        if self.in_db:
            self.db.delete()
        self.deleted = True
        self.in_db   = False

    def save(self):
        """
            Call the appropriate insert or update function, depending on
            whether the object is already in the DB or not
        """
        if self.deleted == True:
            return

        if self.in_db is True:
            if self.db.has_changed is True:
                self.db.update()
            else:
                logging.debug("No changes to %s, not continuing", self.db.table_name)
        else:
            self.db.insert()
            self.in_db = True

    def bulk_save_children(self):
        children_to_insert = []
        children_to_update = []

        for child in self.children:
            if child.in_db == False:
                children_to_insert.append(child)
            else:
                children_to_update.append(child)

        self.db.bulk_insert(children_to_insert)

        for child in children_to_update:
            child.save()

    def get_children(self):
        children = self.db.db_struct[self.db.table_name.lower()]['child_info']
        for name, rows in children.items():
            #turn 'link' into 'links'
            attr_name              = name[1:].lower() + 's'
            #if it's not already set, set it to a default []
            if hasattr(self, attr_name) is False:
                self.__setattr__(attr_name, [])
                self.children.append(attr_name)
        return children

    def load_children(self):

        self.children = []

        child_rs = self.db.load_children(self.child_info)
        for name, rows in child_rs.items():
            #turn 'tLink' into 'links'
            attr_name              = name[1:].lower() + 's'

            child_objs = []
            for row in rows:
                child_obj = self.db.db_hierarchy[name[1:].lower()]['obj'](self)

                child_obj.parent = self
                child_obj.__setattr__(self.name.lower(), self)
                for col, val in row:
                    child_obj.db.__setattr__(col, val)

                child_obj.in_db = True

                child_obj.load_all()

                child_objs.append(child_obj)

            #ex: set network.links = [link1, link2, link3]
            self.__setattr__(attr_name, child_objs)
            #ex: self.children.append('links')
            self.children.append(attr_name)

    def get_parent(self):
        if self.parent is None:
            parent = self.db.get_parent()
            if parent is None:
                return None
            parent_name = parent.__class__.__name__.lower()
            logging.debug("Parent Name: %s", parent_name)
            logging.debug("Parent: %s", parent)
            self.parent = parent
            self.__setattr__(parent_name, parent)

    def get_as_complexmodel(self):
        """
            Converts this object into a spyne.model.ComplexModel type
            which can be used by the soap library.
        """
        #first create the appropriate soap complex model
        cm = getattr(hydra_complexmodels, self.name)()
        #If self is not in the DB, then return an empty
        #complex model.
        if self.in_db is False:
            cm.error = "Not in DB"
            return cm

        #assign values for each database attribute
        for attr in self.db.db_attrs:
            value = getattr(self.db, attr)
            if type(value) == Decimal:
                value = float(value)

            attr_prefix = "%s_"%self.name.lower()
            if attr.find(attr_prefix) == 0:
                attr = attr.replace(attr_prefix, "")

            setattr(cm, attr, value)

        #get my children, convert them and assign them to the new
        #soap object
        for name in self.children:
            objs = getattr(self, name)
            child_objs = []
            for obj in objs:
                obj.load_all()
                child_cm = obj.get_as_complexmodel()
                child_objs.append(child_cm)
            setattr(cm, name, child_objs) 

        return cm


