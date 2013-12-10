from HydraLib import util
from HydraLib.hdb import HydraMySqlCursor
import logging
from decimal import Decimal
from soap_server import hydra_complexmodels 

global DB_STRUCT
DB_STRUCT = {}

global CONNECTION
CONNECTION = None

def init(cnx):
    config = util.load_config()

    global CONNECTION
    global DB_STRUCT

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
    rs = cursor.execute_sql(sql)
    
    #Table desc gives us:
    #[...,(col_name, col_type, nullable, key ('PRI' or 'MUL'), default, auto_increment),...]
    for r in rs:

        col_info    = {}

        col_info['default']        = r.column_default
        col_info['nullable']       = True if r.is_nullable == 'YES' else False
        col_info['type']           = r.column_type
        col_info['primary_key']    = True if r.column_key == 'PRI' else False
        col_info['auto_increment'] = True if r.extra == 'auto_increment' else False
 
        tab_info = DB_STRUCT.get(r.table_name.lower(), {'columns' : {}})
        tab_info['columns'][r.column_name] = col_info
        tab_info['child_info'] = {}
        DB_STRUCT[r.table_name.lower()] = tab_info

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
        and constraint_name != 'PRIMARY'
    """%(config.get('mysqld', 'db_name'),)

    rs = cursor.execute_sql(fk_qry)
    for r in rs:
        child_dict = {}

        child_dict['column_name'] = r.column_name
        child_dict['referenced_column_name'] = r.referenced_column_name
        DB_STRUCT[r.referenced_table_name.lower()]['child_info'][r.table_name] = child_dict

class IfaceBase(object):
    """
        The base database interface class.
    """
    def __init__(self, parent, class_name):
        logging.info("Initialising %s", class_name)
        self.db = IfaceDB(class_name)

        self.name = class_name

        self.in_db = False

        #Indicates that the 'delete' function has been called on this object
        self.deleted = False

        self.child_info = self.get_children()
        self.children   = {}
        self.parent = parent 

    def load(self):
        self.in_db = self.db.load()

        if self.in_db:
            self.load_children()
            self.get_parent()

        return self.in_db

    def commit(self):
        """
            Commit any inserts or updates to the DB. No going back from here...
        """
        CONNECTION.commit()
        if self.deleted == True:
            self.in_db = False

    def delete(self):
        if self.in_db:
            self.db.delete()
        self.deleted = True

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

    def get_children(self):
        children = DB_STRUCT[self.db.table_name.lower()]['child_info']
        for name, rows in children.items():
            #turn 'link' into 'links'
            attr_name              = name[1:].lower() + 's'
            #if it's not already set, set it to a default []
            if hasattr(self, attr_name) is False:
                self.__setattr__(attr_name, [])
        return children

    def load_children(self):

        child_rs = self.db.load_children(self.child_info)
        for name, rows in child_rs.items():
            #turn 'tLink' into 'links'
            attr_name              = name[1:].lower() + 's'

            child_objs = []
            for row in rows:
                row = row.get_as_dict()
                child_obj = db_hierarchy[name[1:].lower()]['obj'](self)

                child_obj.parent = self
                child_obj.__setattr__(self.name.lower(), self)
                for col, val in row.items():
                    child_obj.db.__setattr__(col, val)

                child_objs.append(child_obj)
            self.__setattr__(attr_name, child_objs)
            self.children[attr_name] = self.__getattribute__(attr_name)

    def get_parent(self):
        if self.parent is None and self.in_db:
            parent = self.db.get_parent()
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
        self.load_children()
        for name, objs in self.children.items():
            child_objs = []
            for obj in objs:
                obj.load()
                child_cm = obj.get_as_complexmodel()
                child_objs.append(child_cm)
            setattr(cm, name, child_objs)

        #if I have attributes, convert them
        #and assign them to the new object too.
        if hasattr(self, 'attributes'):
            attributes = []
            for attr in self.attributes:
                attributes.append(attr.get_as_complexmodel())
            setattr(cm, 'attributes', attributes)
        return cm

class IfaceDB(object):
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
        self.table_name = db_hierarchy[self.class_name]['table_name']

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

            logging.info("Loading child %s", table_name)

            base_child_sql = """
                select
                    *
                from
                    %(table)s
                where
                %(fk_col)s = %(fk_val)s
            """

            if self.__getattr__(referenced_column_name) is None:
                continue

            complete_child_sql = base_child_sql % dict(
                table  = table_name,
                fk_col = column_name,
                fk_val = self.__getattr__(referenced_column_name)
            )

            cursor = CONNECTION.cursor(cursor_class=HydraMySqlCursor)
            rs = cursor.execute_sql(complete_child_sql)

            child_dict[table_name] = rs

        return child_dict

    def get_parent(self):
        parent_key = db_hierarchy[self.class_name]['parent']

        if parent_key is None:
            return None

        logging.debug("PARENT: %s", parent_key)

        parent_class = db_hierarchy[parent_key]['obj']
        parent_table = db_hierarchy[parent_key]['table_name']
        parent_pk    = db_hierarchy[parent_key]['pk']

        for k in parent_pk:
            if self.__getattr__(k) is None:
                logging.debug("Cannot load parent. %s is None", k)
                return

        logging.info("Loading parent %s", self.table_name)

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

        cursor = CONNECTION.cursor(cursor_class=HydraMySqlCursor)
        rs = cursor.execute_sql(complete_parent_sql)

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

            val = str(self.db_data[name])

            #Cast the value to the correct DB data type
            if db_type == "double":
                return Decimal(val)
            elif db_type.find('int') != -1:
                return int(val)
            elif db_type == 'blob':
                return eval(val)

            return val

        else:
            raise AttributeError("Attribute %s not set."%name)

    def __setattr__(self, name, value):
        if name != 'db_attrs' and name in self.db_attrs:
            self.db_data[name] = value
            self.has_changed = True
        else:
            super(IfaceDB, self).__setattr__(name, value)

    def insert(self):
        """
            If this object has not been stored in the DB as yet, then insert it.
            Generates an insert statement and runs it.
        """

        cursor = CONNECTION.cursor(cursor_class=HydraMySqlCursor)
        base_insert = "insert into %(table)s (%(cols)s) values (%(vals)s);"
        complete_insert = base_insert % dict(
            table = self.table_name,
            cols  = ",".join([n for n in self.db_attrs]),
            vals  = ",".join([self.get_val(n) for n in self.db_attrs]),
        )

        logging.debug("Running insert: %s", complete_insert)
        old_seq = cursor.lastrowid

        cursor.execute(complete_insert)

        if self.seq is not None:
            if old_seq is None or old_seq != cursor.lastrowid:
                setattr(self, self.seq, cursor.lastrowid)

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
        logging.debug("Running update: %s", complete_update)
        cursor = CONNECTION.cursor(cursor_class=HydraMySqlCursor)
        cursor.execute(complete_update)

    def load(self):
        """
            Loads a row from the DB and assigns the values as entries to
            the self.db_data dictionary. These are accessible direcly from
            the object, without any need to look in the db_data dictionary.
        """

        #Idenitfy the primary key, which is used to get a single row in the db.
        for pk in self.pk_attrs:
            if self.__getattr__(pk) is None:
                logging.warning("Primary key is not set. Cannot load row from DB.")
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

        if len(rs) == 0:
            logging.warning("No entry found for table")
            return False

        for r in rs:
            for k, v in r.get_as_dict().items():
                logging.debug("Setting column %s to %s", k, v)
                self.db_data[k] = v


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
        logging.debug("Running delete: %s", complete_delete)
        cursor = CONNECTION.cursor(cursor_class=HydraMySqlCursor)
        cursor.execute(complete_delete)

    def get_val(self, attr):
        val = self.__getattr__(attr)
        db_type = self.attr_types[attr]

        if val is None:
            return 'null'
        elif db_type.find('varchar') != -1 or db_type in ('blob', 'datetime', 'timestamp') :
            return "'%s'"%val
        else:
            return str(val)

class GenericResource(IfaceBase):
    """
        A superclass for all 'resource' types -- Network, Node, Link, Scenario and Project.
    """
    def __init__(self, parent, class_name, ref_key, ref_id=None):
        self.parent = parent
        
        self.ref_key = ref_key
        self.ref_id  = ref_id
        
        IfaceBase.__init__(self, parent, class_name)

        self.attributes = self.get_attributes()

    def save(self):
        super(GenericResource, self).save()
        pk = self.db.__getattr__(self.db.pk_attrs[0])
        self.ref_id = pk

    def load(self):
        result = super(GenericResource, self).load()

        pk = self.db.__getattr__(self.db.pk_attrs[0])
        self.ref_id = pk

        return result

    def get_attributes(self):
        if self.ref_id is None:
            return []
        attributes = []
        sql = """
                    select
                        attr_id,
                        resource_attr_id
                    from
                        tResourceAttr
                    where
                        ref_id = %(ref_id)s
                    and ref_key = '%(ref_key)s'
            """
        cursor = CONNECTION.cursor(cursor_class=HydraMySqlCursor)
        rs = cursor.execute_sql(sql%dict(ref_key = self.ref_key, ref_id = self.ref_id))

        for r in rs:
            ra = ResourceAttr(resource_attr_id=r.resource_attr_id)
            ra.load()
            attributes.append(ra)

        self.attributes = attributes

        return attributes

    def add_attribute(self, attr_id, attr_is_var='N'):
        attr = ResourceAttr()
        attr.db.attr_id = attr_id
        attr.db.attr_is_var = attr_is_var
        attr.db.ref_key = self.ref_key
        attr.db.ref_id  = self.ref_id
        attr.save()
        self.attributes.append(attr)

        return attr

    def assign_value(self, scenario_id, resource_attr_id, data_type, val,
                     units, name, dimension):

        rs = ResourceScenario(scenario_id=scenario_id, resource_attr_id=resource_attr_id)

        if rs.db.dataset_id is not None:
            sd = Dataset(dataset_id = rs.db.dataset_id)
        else:
            sd = Dataset()

        sd.set_val(data_type, val)

        sd.db.data_type  = data_type
        sd.db.data_units = units
        sd.db.data_name  = name
        sd.db.data_dimen = dimension
        sd.save()
        sd.commit()

        if rs.db.dataset_id is None:
            rs.db.dataset_id       = sd.db.dataset_id

        rs.save()
        rs.commit()

        return rs


class Project(GenericResource):
    """
        A logical container for a piece of work.
        Contains networks and scenarios.
    """
    def __init__(self, project_id = None):
        GenericResource.__init__(self, None, self.__class__.__name__, 'PROJECT', ref_id=project_id)

        self.db.project_id = project_id
        self.networks = []
        if project_id is not None:
            self.load()
    
   # def load(self):
   #     success = super(Project, self).load()
   #     if success:
   #         self.get_networks()

   #     return success

   # def get_networks(self):
   #     sql = """
   #         select
   #             network_id
   #         from
   #             tNetwork
   #         where
   #             project_id=%s
   #     """ % self.db.project_id

   #     cursor = CONNECTION.cursor(cursor_class=HydraMySqlCursor)
   #     rs = cursor.execute_sql(sql)

   #     for r in rs:
   #         n = Network(project=self, network_id = r.network_id)
   #         self.networks.append(n)

   #     return self.networks

class Scenario(GenericResource):
    """
        A set of nodes and links
    """
    def __init__(self, network = None, scenario_id = None):
        GenericResource.__init__(self, network, self.__class__.__name__, 'SCENARIO', ref_id=scenario_id)

        self.db.scenario_id = scenario_id
        if scenario_id is not None:
            self.load()


class Network(GenericResource):
    """
        A set of nodes and links
    """
    def __init__(self, project = None, network_id = None):
        GenericResource.__init__(self,project, self.__class__.__name__, 'NETWORK', ref_id=network_id)
    
        self.project = project
        self.db.network_id = network_id
        self.nodes = []
        self.links = []
        if network_id is not None:
            self.load()
    
    def add_link(self, name, desc, node_1_id, node_2_id):
        """
            Add a link to a network. Links are what effectively
            define the network topology, by associating two already
            existing nodes.
        """
        l = Link()
        l.db.link_name = name
        l.db.link_description = desc
        l.db.node_1_id = node_1_id
        l.db.node_2_id = node_2_id
        l.db.network_id = self.db.network_id
        self.links.append(l)
        return l
    
    
    def add_node(self, name, desc, node_x, node_y):
        """
            Add a node to a network.
        """
        n = Node()
        n.db.node_name = name
        n.db.node_description = desc
        n.db.node_x = node_x
        n.db.node_y = node_y
        n.db.network_id = self.db.network_id
        self.nodes.append(n)
        return n

    def get_link(self, link_id):
        """
        Return a link with id link_id if it is in this
        network
        """
        link = None
        for l in self.links:
                if l.db.link_id == link_id:
                    l.load()
                    link = l
        return link

    def get_node(self, node_id):
        """
        Return node object with id node_id if it is
        in this network.
        """
        node = None
        for n in self.nodes:
            if n.db.node_id == node_id:
                node = n
                node.load()
        return node

class Node(GenericResource):
    """
        Representation of a resource.
    """
    def __init__(self, network = None, node_id = None):
        GenericResource.__init__(self,network, self.__class__.__name__, 'NODE', ref_id=node_id)

        self.network = network
        self.db.node_id = node_id
        if node_id is not None:
            self.load()

class Link(GenericResource):
    """
        Representation of a connection between nodes.
    """
    def __init__(self,network = None, link_id = None):
        GenericResource.__init__(self,network, self.__class__.__name__, 'LINK', ref_id=link_id)

        self.db.link_id = link_id
        self.network    = network
        if link_id is not None:
            self.load()

class Attr(IfaceBase):
    """
        An attribute is a piece of data, with a name and a dimension.
        For example, an attribute might be 'volume' and 'metres-cubed'
        A piece of information is associated with a resource using a resource
        attribute.
    """
    def __init__(self, attr_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.attr_id = attr_id

        if attr_id is not None:
            self.load()

class AttrMap(IfaceBase):
    """
       Defines equality between attributes ('volume' is equivalent to 'vol')
    """
    def __init__(self, attr_id_a = None, attr_id_b = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.attr_id_a = attr_id_a
        self.db.attr_id_b = attr_id_b

        if attr_id_a is not None and attr_id_b is not None:
            self.load()

class ResourceAttr(IfaceBase):
    """
        A 'resource' can be either a node, link or network.
        A resource attribute is a instance of an attribute for
        a given resource.
    """
    def __init__(self, attr = None, resource_attr_id = None):
        IfaceBase.__init__(self, attr, self.__class__.__name__)

        self.db.resource_attr_id = resource_attr_id

        if resource_attr_id is not None:
            self.load()
    
    def get_resource(self):
        ref_key_map = {
            'NODE'     : Node,
            'LINK'     : Link,
            'NETWORK'  : Network,
            'SCENARIO' : Scenario,
            'PROJECT'  : Project,
        }

        obj = ref_key_map[self.db.ref_key]()
        obj.db.__setattr__(obj.db.pk_attrs[0], self.db.ref_id)  
        obj.load()
        return obj

class ResourceTemplate(IfaceBase):
    """
        A resource template is a grouping of attributes which define
        a resource. For example, a "reservoir" template may have "volume",
        "discharge" and "daily throughput".
    """
    def __init__(self, resourcetemplategroup  = None, template_id = None):
        IfaceBase.__init__(self, resourcetemplategroup, self.__class__.__name__)

        self.db.template_id = template_id

        if template_id is not None:
            self.load()

class ResourceTemplateItem(IfaceBase):
    """
        A resource template item is a link between a resource template
        and attributes.
    """
    def __init__(self, resourcetemplate=None, attr_id = None, template_id = None):
        IfaceBase.__init__(self, resourcetemplate, self.__class__.__name__)

        self.db.attr_id = attr_id
        self.db.template_id = template_id

        if attr_id is not None and template_id is not None:
            self.load()

class ResourceTemplateGroup(IfaceBase):
    """
        A resource template group is a set of templates, usually categorised
        by the plugin which they were defined for.
    """
    def __init__(self, group_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.group_id = group_id
        if group_id is not None:
            self.load()

class ResourceScenario(IfaceBase):
    """
        A resource scenario is what links the actual piece of data
        with a resource -- the data per resource will change per scenario.
    """
    def __init__(self, scenario = None, scenario_id = None, resource_attr_id=None):
        IfaceBase.__init__(self, scenario, self.__class__.__name__)

        self.db.scenario_id = scenario_id
        self.db.resource_attr_id = resource_attr_id
        self.resourceattr = None
        self.dataset = None

        if scenario_id is not None and resource_attr_id is not None:
            self.load()
    
    def load(self):
        """
            Override the base load function to also load sibling
            objects -- resource attribute and scenario data.
        """
        super(ResourceScenario, self).load()
        self.get_resource_attr()
        self.get_dataset()

    def get_resource_attr(self):
        ra = ResourceAttr(resource_attr_id = self.db.resource_attr_id)
        self.resourceattr = ra
        return ra

    def get_dataset(self):
        ds = None
        if self.db.dataset_id is not None:
            ds = Dataset(dataset_id = self.db.dataset_id)
            self.dataset = ds
        return ds

    def get_as_complexmodel(self):
        """
            This method overrides the base method as it hides
            some of the DB complexities from the soap interface
            and makes a simpler structure
        """
        #first create the appropriate soap complex model
        cm = hydra_complexmodels.ResourceScenario()
        cm.resource_attr_id = self.db.resource_attr_id
        cm.attr_id = self.resourceattr.db.attr_id

        if self.dataset is not None:
            cm.type = self.dataset.db.data_type
            cm.value = self.dataset.get_as_complexmodel()

        return cm


class Dataset(IfaceBase):
    """
        A table recording all pieces of data, including the
        type, units, name and dimension. The actual data value is stored
        in another table, which is identified based on the type.
    """
    def __init__(self, dataset_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.dataset_id = dataset_id

        if dataset_id is not None:
            self.load()

    def get_val(self):
        val = None
        if self.db.data_type == 'descriptor':
            d = Descriptor(data_id = self.db.data_id)
            val = d.db.desc_val
        elif self.db.data_type == 'timeseries':
            ts = TimeSeries(data_id=self.db.data_id)
            ts_datas = ts.timeseriesdatas
            val = [(ts_data.db.time, ts_data.db.ts_value) for ts_data in ts_datas]
        elif self.db.data_type == 'eqtimeseries':
            eqts = EqTimeSeries(data_id = self.db.data_id)
            val  = eqts.db.arr_data
        elif self.db.data_type == 'scalar':
            s = Scalar(data_id = self.db.data_id)
            val = s.db.param_value
        elif self.db.data_type == 'array':
            a = Array(data_id = self.db.data_id)
            val = a.db.arr_data

        logging.debug("VALUE IS: %s", val)
        return val

    def set_val(self, data_type, val):
        data = None
        if data_type == 'descriptor':
            data = Descriptor(data_id=self.db.data_id)
            data.db.desc_val = val
        elif data_type == 'timeseries':
            data = TimeSeries(data_id=self.db.data_id)
            data.set_ts_values(val)
            #data.db.ts_time  = val[0]
            #data.db.ts_value = val[1]
        elif data_type == 'eqtimeseries':
            data = EqTimeSeries(data_id = self.db.data_id)
            data.db.start_time = val[0]
            data.db.frequency  = val[1]
            data.db.arr_data = val[2]
        elif data_type == 'scalar':
            data = Scalar(data_id = self.db.data_id)
            data.db.param_value = val
        elif data_type == 'array':
            data = Array(data_id = self.db.data_id)
            data.db.arr_data = val
        data.save()
        data.commit()
        data.load()
        self.db.data_type = data_type
        self.db.data_id = data.db.data_id
        return data
    
    def get_as_complexmodel(self):
        """
            This method overrides the base method as it hides
            some of the DB complexities from the soap interface
            and makes a simpler structure, in a ScenarioAttr object.
        """

        complexmodel = None
        if self.db.data_type == 'descriptor':
            d = Descriptor(data_id = self.db.data_id)
            complexmodel = {'desc_val': [d.db.desc_val]} 
        elif self.db.data_type == 'timeseries':
            ts = TimeSeries(data_id=self.db.data_id)
            ts_datas = ts.timeseriesdatas
            ts_values = []
            for ts in ts_datas:
                ts_values.append({
                    'ts_time'  : [ts.db.ts_time],
                    'ts_value' : [ts.db.ts_value]
                })
            complexmodel = {
                'ts_values' : ts_values
            }
        elif self.db.data_type == 'eqtimeseries':
            eqts = EqTimeSeries(data_id = self.db.data_id)
            complexmodel = {
                'start_time' : eqts.db.start_time,
                'frequency'  : eqts.db.frequency,
                'arr_data'   : eqts.db.arr_data,
            }
        elif self.db.data_type == 'scalar':
            s = Scalar(data_id = self.db.data_id)
            complexmodel = {
                 'param_value' : s.db.param_value,  
            }
        elif self.db.data_type == 'array':
            a = Array(data_id = self.db.data_id)
            #complexmodel = self.make_complexmodel_array(a.db.arr_data)
            complexmodel = {
                'arr_data' : a.db.arr_data
            }

        return complexmodel

    def make_complexmodel_array(self, value):
#        cm_array = hydra_complexmodels.Array()

        cm_arr_data = []
        for v in value:
            if type(v) is list:
                cm_arr_data.append(self.make_complexmodel_array(v))
            else:
                cm_arr_data.append(v)
        cm_array = {
            'arr_data' : cm_arr_data
        }
        return cm_array

class DataAttr(IfaceBase):
    """
        Holds additional information on data.
    """
    def __init__(self, d_attr_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.d_attr_id = d_attr_id

        if d_attr_id is not None:
            self.load()

class Descriptor(IfaceBase):
    """
        A non numeric data value
    """
    def __init__(self, data_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.data_id = data_id
        if data_id is not None:
            self.load()

class TimeSeries(IfaceBase):
    """
        Non-equally spaced time series data
        Links to multiple entries in time series data, which
        actually stores the info.
    """
    def __init__(self, data_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.data_id = data_id
        if data_id is not None:
            self.load()

    def set_ts_value(self, time, value):
        """
            Adds a single timeseries value to the timeseries.
            This consists of a timestamp and a value
        """

        for ts in self.timeseriesdatas:
            if ts.db.data_id == self.db.data_id and ts.db.ts_time == str(time):
                ts.db.ts_value = value
                return
        #else:
        ts_val = TimeSeriesData()
        ts_val.db.data_id = self.db.data_id
        ts_val.db.ts_time = time
        ts_val.db.ts_value = value

        self.timeseriesdatas.append(ts_val)

    def set_ts_values(self, values):
        """
            Adds multiple timeseries values to a timeseries.
            This is takes a list of tuples as an argument, as follows:
            [(time_1, value_1), (time_2, value_2), ...(time_n, value_n)]
        """

        for time, value in values:
            self.set_ts_value(time, value)

    def get_ts_value(self, time):
        """
            returns the value at a given time for a timeseries
        """
        for ts_data in self.timeseriesdatas:
            logging.debug("%s vs %s", ts_data.db.ts_time, time)
            if ts_data.db.ts_time == str(time):
                return ts_data.db.ts_value
        logging.info("No value found at %s for data_id %s", time, self.db.data_id)
        return None

    def delete(self):
        for ts_data in self.timeseriesdatas:
            ts_data.delete()
        super(TimeSeries, self).delete()

    def save(self):
        super(TimeSeries, self).save()

        for ts_data in self.timeseriesdatas:
            ts_data.db.data_id = self.db.data_id
            ts_data.save()

    def commit(self):
        super(TimeSeries, self).commit()
        for ts_data in self.timeseriesdatas:
            ts_data.commit()

class TimeSeriesData(IfaceBase):
    """
        Non-equally spaced time series data
        In other words: a value and a timestamp.
    """
    def __init__(self, data_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.data_id = data_id
        if data_id is not None:
            self.load()

class EqTimeSeries(IfaceBase):
    """
        Equally spaced time series data
        -- a start time, frequency and an associated array.
    """
    def __init__(self, data_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.data_id = data_id

        if data_id is not None:
            self.load()

class Scalar(IfaceBase):
    """
        The values contained in an equally spaced time series.
    """
    def __init__(self, data_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.data_id = data_id
        if data_id is not None:
            self.load()

class Array(IfaceBase):
    """
        List of values, stored as a BLOB.
    """
    def __init__(self, data_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.data_id = data_id
        if data_id is not None:
            self.load()

class Constraint(IfaceBase):
    """
        A constraint or rule placed on a network, perhaps
        to ensure mutual exclusion of certain resources..
    """
    def __init__(self, scenario=None, constraint_id = None):
        IfaceBase.__init__(self, scenario, self.__class__.__name__)

        self.scenario=scenario
        self.db.constraint_id = constraint_id
        if constraint_id is not None:
            self.load()

    def eval_condition(self):
        grp_1 = ConstraintGroup(constraint=self, group_id = self.db.group_id)

        condition_string = "%s %s %s"%(grp_1.eval_group(), self.db.op, self.db.constant)

        return condition_string


class ConstraintGroup(IfaceBase):
    """
        a connector class for constraints. Used for grouping constraints
        into logical sections, not unlike parentheses in a mathematical equation.
    """
    def __init__(self, constraint=None, group_id = None):
        IfaceBase.__init__(self, constraint, self.__class__.__name__)

        self.constraint = constraint
        self.db.group_id = group_id
        if group_id is not None:
            self.load()

    def eval_group(self):

        str_1 = None
        str_2 = None

        if self.db.ref_key_1 == 'GRP':
            group = ConstraintGroup(self.constraint, group_id=self.db.ref_id_1)
            str_1 = group.eval_group()
        elif self.db.ref_key_1 == 'ITEM':
            item = ConstraintItem(item_id=self.db.ref_id_1)

            r = ResourceScenario(
                    scenario_id      = self.constraint.db.scenario_id,
                    resource_attr_id = item.db.resource_attr_id
            )

            d = Dataset(dataset_id=r.db.dataset_id)
            str_1 = d.get_val()

        if self.db.ref_key_2 == 'GRP':
            group = ConstraintGroup(self.constraint, group_id=self.db.ref_id_2)
            str_2 = group.eval_group()
        elif self.db.ref_key_2 == 'ITEM':
            item = ConstraintItem(item_id=self.db.ref_id_2)

            r = ResourceScenario(
                    scenario_id      = self.constraint.db.scenario_id,
                    resource_attr_id = item.db.resource_attr_id
            )

            d = Dataset(dataset_id=r.db.dataset_id)
            str_2 = d.get_val()


        return "%s %s %s"%(str_1, self.db.op, str_2)


class ConstraintItem(IfaceBase):
    """
        The link to the resource, upon which the constraint is being applied.
    """
    def __init__(self, constraint=None, item_id = None):
        IfaceBase.__init__(self, constraint, self.__class__.__name__)

        self.constraint = constraint
        self.db.item_id = item_id
        if item_id is not None:
            self.load()

class User(IfaceBase):
    """
        Users of the hydra
    """
    def __init__(self, user_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.user_id = user_id
        if user_id is not None:
            self.load()

class Role(IfaceBase):
    """
        Roles for hydra users
    """
    def __init__(self, role_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.role_id = role_id
        if role_id is not None:
            self.load()


class Perm(IfaceBase):
    """
        Hydra Permissions
    """
    def __init__(self, perm_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.perm_id = perm_id
        if perm_id is not None:
            self.load()

class RoleUser(IfaceBase):
    """
        Roles for hydra users
    """
    def __init__(self, role=None, user_id = None, role_id = None):
        IfaceBase.__init__(self, role, self.__class__.__name__)

        self.db.user_id = user_id
        self.db.role_id = role_id
        if user_id is not None and role_id is not None:
            self.load()

class RolePerm(IfaceBase):
    """
        Permissions for hydra Roles
    """
    def __init__(self, role=None, perm_id = None, role_id = None):
        IfaceBase.__init__(self, role, self.__class__.__name__)

        self.db.perm_id = perm_id
        self.db.role_id = role_id
        if perm_id is not None and role_id is not None:
            self.load()


db_hierarchy = dict(
    project  = dict(
        obj   = Project,
        parent = None,
        table_name = 'tProject',
        pk     = ['project_id']
    ),
    network  = dict(
        obj   = Network,
        parent = 'project',
        table_name = 'tNetwork',
        pk     = ['network_id']
    ),
    node  = dict(
        obj   = Node,
        parent = None,
        table_name = 'tNode',
        pk     = ['node_id']
   ),
    link  = dict(
        obj   = Link,
        parent = 'network',
        table_name = 'tLink',
        pk     = ['link_id']
    ),
    scenario  = dict(
        obj    = Scenario,
        parent = 'network',
        table_name = 'tScenario',
        pk     = ['scenario_id']
    ),
    attr  = dict(
        obj   = Attr,
        parent = None,
        table_name = 'tAttr',
        pk     = ['attr_id']
    ),
    attrmap  = dict(
        obj   = AttrMap,
        parent = None,
        table_name = 'tAttrMap',
        pk     = ['attr_id_a', 'attr_id_b']
    ),
    resourceattr  = dict(
        obj   = ResourceAttr,
        parent = 'attr',
        table_name = 'tResourceAttr',
        pk     = ['resource_attr_id']
    ),
    resourcetemplate  = dict(
        obj   = ResourceTemplate,
        parent = 'resourcetemplategroup',
        table_name = 'tResourceTemplate',
        pk     = ['template_id']
    ),
    resourcetemplateitem  = dict(
        obj   = ResourceTemplateItem,
        parent = 'resourcetemplate',
        table_name = 'tResourceTemplateItem',
        pk     = ['attr_id', 'template_id'],
    ),
    resourcetemplategroup  = dict(
        obj   = ResourceTemplateGroup,
        parent = None,
        table_name = 'tResourceTemplateGroup',
        pk     = ['group_id']
    ),
    resourcescenario  = dict(
        obj   = ResourceScenario,
        parent = 'scenario',
        table_name = 'tResourceScenario',
        pk     = ['resource_attr_id', 'scenario_id']
    ),
    dataset  = dict(
        obj   = Dataset,
        parent = None,
        table_name = 'tDataset',
        pk     = ['dataset_id']
    ),
    dataattr  = dict(
        obj   = DataAttr,
        parent = None,
        table_name = 'tDataAttr',
        pk     = ['d_attr_id'],
    ),
    descriptor  = dict(
        obj   = Descriptor,
        parent = None,
        table_name = 'tDescriptor',
        pk     = ['data_id']
    ),
    timeseries  = dict(
        obj   = TimeSeries,
        parent = None,
        table_name = 'tTimeSeries',
        pk     = ['data_id']
    ),
    timeseriesdata  = dict(
        obj   = TimeSeriesData,
        parent = 'timeseries',
        table_name = 'tTimeSeriesData',
        pk     = ['data_id', 'ts_time']
    ),
    eqtimeseries  = dict(
        obj   = EqTimeSeries,
        parent = None,
        table_name = 'tEqTimeSeries',
        pk     = ['data_id']
    ),
    scalar  = dict(
        obj   = Scalar,
        parent = None,
        table_name = 'tScalar',
        pk     = ['data_id']
    ),
    array  = dict(
        obj   = Array,
        parent = None,
        table_name = 'tArray',
        pk     = ['data_id']
    ),
    constraint  = dict(
        obj   = Constraint,
        parent = 'scenario',
        table_name = 'tConstraint',
        pk     = ['constraint_id']
    ),
    constraintgroup  = dict(
        obj   = ConstraintGroup,
        parent = 'constraint',
        table_name = 'tConstraintGroup',
        pk     = ['group_id']
    ),
    constraintitem  = dict(
        obj   = ConstraintItem,
        parent = 'constraint',
        table_name = 'tConstraintItem',
        pk     = ['item_id']
    ),
    user  = dict(
        obj   = User,
        parent = None,
        table_name = 'tUser',
        pk     = ['user_id']
    ),
    role  = dict(
        obj   = Role,
        parent = None,
        table_name = 'tRole',
        pk     = ['role_id']
    ),
    perm  = dict(
        obj   = Perm,
        parent = None,
        table_name = 'tPerm',
        pk     = ['perm_id']
    ),
    roleuser  = dict(
        obj   = RoleUser,
        parent = 'role',
        table_name = 'tRoleUser',
        pk     = ['user_id', 'role_id']
    ),
    roleperm  = dict(
        obj   = RolePerm,
        parent = 'role',
        table_name = 'tRolePerm',
        pk     = ['perm_id', 'role_id']
    )
)
