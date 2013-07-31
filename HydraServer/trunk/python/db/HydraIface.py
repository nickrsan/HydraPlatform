from HydraLib import hdb
from HydraLib.hdb import HydraMySqlCursor
import logging
from decimal import Decimal

class IfaceBase(object):
    def __init__(self, class_name):
        logging.info("Initialising %s", class_name)
        self.db = IfaceDB(class_name)

        self.name = class_name

        self.in_db = False

        #Indicates that the 'delete' function has been called on this object
        self.deleted = False

        self.children = {}
        self.parent = None

    def load(self):
        self.in_db = self.db.load()

        if self.in_db:
            self.get_children()
            self.get_parent()

        return self.in_db

    def commit(self):
        """
            Commit any inserts or updates to the DB. No going back from here...
        """
        self.db.connection.commit()
        if self.deleted == True:
            self.in_db = False
        self.load()

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
        children = self.db.get_children()
        for name, rows in children.items():
            #turn 'link' into 'links'
            attr_name              = name[1:].lower() + 's'

            child_objs = []
            for row in rows:
                row = row.get_as_dict()
                child_obj = eval(name[1:])()

                child_obj.parent = self
                child_obj.__setattr__(self.name.lower(), self)

                for col, val in row.items():
                    child_obj.db.__setattr__(col, val)

                child_objs.append(child_obj)

            self.__setattr__(attr_name, child_objs)
            self.children[name.lower()] = self.__getattribute__(attr_name)

    def get_parent(self):
        if self.parent is None and self.in_db:
            parent = self.db.get_parent()
            parent_name = parent.__class__.__name__.lower()
            logging.debug("Parent Name: %s", parent_name)
            logging.debug("Parent: %s", parent)
            self.parent = parent
            self.__setattr__(parent_name, parent)

class IfaceDB(object):

    def __init__(self, class_name):

        self.db_attrs = []
        self.pk_attrs = db_hierarchy['t'+class_name.lower()]['pk']
        self.db_data  = {}
        self.nullable_attrs = []
        self.attr_types = {}
        self.seq = None

        self.connection = hdb.get_connection()

        self.cursor = self.connection.cursor(cursor_class=HydraMySqlCursor)

        #This indicates whether any values in this table have changed.
        self.has_changed = False

        #this turns 'Project' into 'tProject' so it can identify the table
        self.table_name = "t%s"%class_name

        self.cursor.execute('desc %s' % self.table_name)

        table_desc = self.cursor.fetchall()

        logging.debug("Table desc: %s", table_desc)

        """
            The table description from MySql looks like:
            [...,(col_name, col_type, nullable, key ('PRI' or 'MUL'), default, auto_increment),...]
        """

        for col_desc in table_desc:
            col_name = col_desc[0]

            self.db_attrs.append(col_name)
            self.db_data[col_name] = col_desc[4]

            self.attr_types[col_name] = col_desc[1]

            if col_desc[2] == 'YES':
                self.nullable_attrs.append(col_name)

            if col_desc[3] == 'PRI':
                pass
                #self.pk_attrs.append(col_name)

            if col_desc[5] == 'auto_increment':
                self.seq = col_name

    def get_children(self):
        """
            Find all the things that reference me in the db. They are tentatively
            called 'children'. A dictionary is returned, keyed on the name
            of the child table and valued with a list of HydraRSRows
        """

        schema_qry    = """
                            select
                                table_name,
                                column_name,
                                referenced_column_name
                            from
                                key_column_usage
                            where
                                referenced_table_name = '%s'
                            and constraint_name != 'PRIMARY'
                        """%(self.table_name)
        schema_cnx    = hdb.connect_tmp(db_name='information_schema')
        schema_cursor = schema_cnx.cursor(cursor_class=HydraMySqlCursor)

        rs = schema_cursor.execute_sql(schema_qry)

        schema_cnx.disconnect()

        logging.debug("Children for %s are: %s", self.table_name, [r.table_name for r in rs])

        #select all of my children out of the DB.
        child_dict = {}
        for r in rs:
            logging.info("Loading child %s", r.table_name)

            base_child_sql = """
                select
                    *
                from
                    %(table)s
                where
                 %(fk_col)s = %(fk_val)s
            """

            if self.__getattr__(r.referenced_column_name) is None:
                continue

            complete_child_sql = base_child_sql % dict(
                 table = r.table_name,
                 fk_col = r.column_name,
                 fk_val = self.__getattr__(r.referenced_column_name)
            )

            rs = self.cursor.execute_sql(complete_child_sql)

            child_dict[r.table_name] = rs

        return child_dict

    def get_parent(self):

        parent = db_hierarchy[self.table_name.lower()]['parent']

        if parent is None:
            return None

        logging.debug("PARENT: %s", parent)

        parent_class = db_hierarchy[parent]['obj']
        parent_pk    = db_hierarchy[parent]['pk']

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
                table = parent,
                pk    = " and ".join(["%s = %s"%(n, self.get_val(n)) for n in parent_pk]),
        )

        rs = self.cursor.execute_sql(complete_parent_sql)

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

        base_insert = "insert into %(table)s (%(cols)s) values (%(vals)s);"
        complete_insert = base_insert % dict(
            table = self.table_name,
            cols  = ",".join([n for n in self.db_attrs]),
            vals  = ",".join([self.get_val(n) for n in self.db_attrs]),
        )

        logging.debug("Running insert: %s", complete_insert)
        old_seq = self.cursor.lastrowid

        self.cursor.execute(complete_insert)

        if self.seq is not None:
            if old_seq is None or old_seq != self.cursor.lastrowid:
                setattr(self, self.seq, self.cursor.lastrowid)

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
        self.cursor.execute(complete_update)

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
        logging.debug(self.cursor)
        rs = self.cursor.execute_sql(complete_load)

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
        self.cursor.execute(complete_delete)

    def get_val(self, attr):
        val = self.__getattr__(attr)
        db_type = self.attr_types[attr]

        if val is None:
            return 'null'
        elif db_type.find('varchar') != -1 or db_type in ('blob', 'datetime', 'timestamp') :
            return "'%s'"%val
        else:
            return str(val)

class Project(IfaceBase):
    """
        A logical container for a piece of work.
        Contains networks and scenarios.
    """
    def __init__(self, project_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.project_id = project_id
        if project_id is not None:
            self.load()

class Scenario(IfaceBase):
    """
        A set of nodes and links
    """
    def __init__(self, scenario_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.scenario_id = scenario_id
        if scenario_id is not None:
            self.load()


class Network(IfaceBase):
    """
        A set of nodes and links
    """
    def __init__(self, network_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.network_id = network_id
        if network_id is not None:
            self.load()

class Node(IfaceBase):
    """
        Representation of a resource.
    """
    def __init__(self, node_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.node_id = node_id
        if node_id is not None:
            self.load()
        self.attributes = self.get_attributes()

    def get_attributes(self):
        if self.db.node_id is None:
            return []
        attributes = []
        cursor = self.db.connection.cursor(cursor_class=HydraMySqlCursor)
        cursor.execute("""
                    select
                        attr_id
                    from
                        tResourceAttr
                    where
                        ref_id = %s
                    and ref_key = 'NODE'
                      """%self.db.node_id)

        for att in cursor.fetchall():
            a = Attr(attr_id=int(att[0]))
            a.load()
            attributes.append(a)

        return attributes

    def add_attribute(self, attr_id, attr_is_var='N'):
        attr = ResourceAttr()
        attr.db.attr_id = attr_id
        attr.db.attr_is_var = attr_is_var
        attr.db.ref_key = 'NODE'
        attr.db.ref_id  = self.db.node_id
        attr.save()
        attr.commit()
        attr.load()
        self.attributes.append(attr)

        return attr

    def assign_value(self, scenario_id, resource_attr_id, data_type, val,
                     units, name, dimension):
        attr = ResourceAttr(resource_attr_id = resource_attr_id)

        sd = ScenarioData()
        sd.set_val(data_type, val)

        sd.db.data_type  = data_type
        sd.db.data_units = units
        sd.db.data_name  = name
        sd.db.data_dimen = dimension
        sd.save()
        sd.commit()
        sd.db.load()

        rs = ResourceScenario()

        rs.db.scenario_id      = scenario_id
        rs.db.dataset_id       = sd.db.dataset_id
        rs.db.resource_attr_id = attr.db.resource_attr_id
        rs.save()
        rs.commit()

        return rs


class Link(IfaceBase):
    """
        Representation of a connection between nodes.
    """
    def __init__(self, link_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.link_id = link_id
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
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.attr_id = attr_id

        if attr_id is not None:
            self.load()

class AttrMap(IfaceBase):
    """
       Defines equality between attributes ('volume' is equivalent to 'vol')
    """
    def __init__(self, attr_id_a = None, attr_id_b = None):
        IfaceBase.__init__(self, self.__class__.__name__)

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
    def __init__(self, resource_attr_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.resource_attr_id = resource_attr_id

        if resource_attr_id is not None:
            self.load()

class ResourceTemplate(IfaceBase):
    """
        A resource template is a grouping of attributes which define
        a resource. For example, a "reservoir" template may have "volume",
        "discharge" and "daily throughput".
    """
    def __init__(self, template_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.template_id = template_id

        if template_id is not None:
            self.load()

class ResourceTemplateItem(IfaceBase):
    """
        A resource template item is a link between a resource template
        and attributes.
    """
    def __init__(self, attr_id = None, template_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

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
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.group_id = group_id
        if group_id is not None:
            self.load()

class ResourceScenario(IfaceBase):
    """
        A resource scenario is what links the actual piece of data
        with a resource -- the data per resource will change per scenario.
    """
    def __init__(self, scenario_id = None, resource_attr_id=None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.scenario_id = scenario_id
        self.db.resource_attr_id = resource_attr_id

        if scenario_id is not None and resource_attr_id is not None:
            self.load()

class ScenarioData(IfaceBase):
    """
        A table recording all pieces of data, including the
        type, units, name and dimension. The actual data value is stored
        in another table, which is identified based on the type.
    """
    def __init__(self, dataset_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.dataset_id = dataset_id

        if dataset_id is not None:
            self.load()

    def get_val(self):
        val = None
        if self.db.data_type == 'descriptor':
            d = Descriptor(data_id = self.db.data_id)
            val = d.desc_val
        elif self.db.data_type == 'timeseries':
            ts = TimeSeries(data_id=self.db.data_id)
            val = ts.db.ts_value
        elif self.db.data_type == 'eqtimeseries':
            eqts = EquallySpacedTimeSeries(data_id = self.db.data_id)
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
            data.desc_val = val
        elif data_type == 'timeseries':
            data = TimeSeries(data_id=self.db.data_id)
            data.db.ts_time  = val[0]
            data.db.ts_value = val[1]
        elif data_type == 'eqtimeseries':
            data = EquallySpacedTimeSeries(data_id = self.db.data_id)
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



class DataAttr(IfaceBase):
    """
        Holds additional information on data.
    """
    def __init__(self, d_attr_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.d_attr_id = d_attr_id

        if d_attr_id is not None:
            self.load()

class Descriptor(IfaceBase):
    """
        A non numeric data value
    """
    def __init__(self, data_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.data_id = data_id
        if data_id is not None:
            self.load()

class TimeSeries(IfaceBase):
    """
        Non-equally spaced time series data
        In other words: a value and a timestamp.
    """
    def __init__(self, data_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.data_id = data_id
        if data_id is not None:
            self.load()

class EquallySpacedTimeSeries(IfaceBase):
    """
        Equally spaced time series data
        -- a start time, frequency and an associated array.
    """
    def __init__(self, data_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.data_id = data_id

        if data_id is not None:
            self.load()

class Scalar(IfaceBase):
    """
        The values contained in an equally spaced time series.
    """
    def __init__(self, data_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.data_id = data_id
        if data_id is not None:
            self.load()

class Array(IfaceBase):
    """
        List of values, stored as a BLOB.
    """
    def __init__(self, data_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.data_id = data_id
        if data_id is not None:
            self.load()

class Constraint(IfaceBase):
    """
        A constraint or rule placed on a network, perhaps
        to ensure mutual exclusion of certain resources..
    """
    def __init__(self, scenario=None, constraint_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

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
        IfaceBase.__init__(self, self.__class__.__name__)

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

            d = ScenarioData(dataset_id=r.db.dataset_id)
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

            d = ScenarioData(dataset_id=r.db.dataset_id)
            str_2 = d.get_val()


        return "%s %s %s"%(str_1, self.db.op, str_2)


class ConstraintItem(IfaceBase):
    """
        The link to the resource, upon which the constraint is being applied.
    """
    def __init__(self, item_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.item_id = item_id
        if item_id is not None:
            self.load()

class User(IfaceBase):
    """
        Users of the hydra
    """
    def __init__(self, user_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.user_id = user_id
        if user_id is not None:
            self.load()

class Role(IfaceBase):
    """
        Roles for hydra users
    """
    def __init__(self, role_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.role_id = role_id
        if role_id is not None:
            self.load()


class Perm(IfaceBase):
    """
        Hydra Permissions
    """
    def __init__(self, perm_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.perm_id = perm_id
        if perm_id is not None:
            self.load()

class RoleUser(IfaceBase):
    """
        Roles for hydra users
    """
    def __init__(self, user_id = None, role_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.user_id = user_id
        self.db.role_id = role_id
        if user_id is not None and role_id is not None:
            self.load()

class RolePerm(IfaceBase):
    """
        Permissions for hydra Roles
    """
    def __init__(self, perm_id = None, role_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.perm_id = perm_id
        self.db.role_id = role_id
        if perm_id is not None and role_id is not None:
            self.load()


db_hierarchy = dict(
    tproject  = dict(
        obj   = Project,
        name  = 'Project',
        parent = None,
        pk     = ['project_id']
    ),
    tnetwork  = dict(
        obj   = Network,
        name  = 'network',
        parent = 'tproject',
        pk     = ['network_id']
    ),
    tnode  = dict(
        obj   = Node,
        name  = 'node',
        parent = None,
        pk     = ['node_id']
   ),
    tlink  = dict(
        obj   = Link,
        name  = 'link',
        parent = 'tnetwork',
        pk     = ['link_id']
    ),
    tscenario  = dict(
        obj    = Scenario,
        name   = 'scenario',
        parent = 'tnetwork',
        pk     = ['scenario_id']
    ),
    tattr  = dict(
        obj   = Attr,
        name  = 'attr',
        parent = None,
        pk     = ['attr_id']
    ),
    tattrmap  = dict(
        obj   = AttrMap,
        name  = 'attrmap',
        parent = None,
        pk     = ['attr_id_a', 'attr_id_b']
    ),
    tresourceattr  = dict(
        obj   = ResourceAttr,
        name  = 'resourceattr',
        parent = None,
        pk     = ['resource_attr_id']
    ),
    tresourcetemplate  = dict(
        obj   = ResourceTemplate,
        name  = 'resourcetemplate',
        parent = 'tresourcetemplategroup',
        pk     = ['template_id']
    ),
    tresourcetemplateitem  = dict(
        obj   = ResourceTemplateItem,
        name  = 'resourcetemplateitem',
        parent = 'tresourcetemplate',
        pk     = ['attr_id', 'template_id'],
    ),
    tresourcetemplategroup  = dict(
        obj   = ResourceTemplateGroup,
        name  = 'resourcetemplategroup',
        parent = None,
        pk     = ['group_id']
    ),
    tresourcescenario  = dict(
        obj   = ResourceScenario,
        name  = 'resourcescenario',
        parent = 'tscenario',
        pk     = ['resource_attr_id', 'scenario_id']
    ),
    tscenariodata  = dict(
        obj   = ScenarioData,
        name  = 'scenariodata',
        parent = None,
        pk     = ['dataset_id']
    ),
    tdataattr  = dict(
        obj   = DataAttr,
        name  = 'dataattr',
        parent = None,
        pk     = ['d_attr_id'],
    ),
    tdescriptor  = dict(
        obj   = Descriptor,
        name  = 'descriptor',
        parent = None,
        pk     = ['data_id']
    ),
    ttimeseries  = dict(
        obj   = TimeSeries,
        name  = 'timeseries',
        parent = None,
        pk     = ['data_id']
    ),
    tequallyspacedtimeseries  = dict(
        obj   = EquallySpacedTimeSeries,
        name  = 'equallyspacedtimeseries',
        parent = None,
        pk     = ['data_id']
    ),
    tscalar  = dict(
        obj   = Scalar,
        name  = 'scalar',
        parent = None,
        pk     = ['data_id']
    ),
    tarray  = dict(
        obj   = Array,
        name  = 'array',
        parent = None,
        pk     = ['data_id']
    ),
    tconstraint  = dict(
        obj   = Constraint,
        name  = 'constraint',
        parent = 'tscenario',
        pk     = ['constraint_id']
    ),
    tconstraintgroup  = dict(
        obj   = ConstraintGroup,
        name  = 'constraintgroup',
        parent = 'tconstraint',
        pk     = ['group_id']
    ),
    tconstraintitem  = dict(
        obj   = ConstraintItem,
        name  = 'constraintitem',
        parent = 'tconstraint',
        pk     = ['item_id']
    ),
    tuser  = dict(
        obj   = User,
        name  = 'user',
        parent = None,
        pk     = ['user_id']
    ),
    trole  = dict(
        obj   = Role,
        name  = 'role',
        parent = None,
        pk     = ['role_id']
    ),
    tperm  = dict(
        obj   = Perm,
        name  = 'perm',
        parent = None,
        pk     = ['perm_id']
    ),
    troleuser  = dict(
        obj   = RoleUser,
        name  = 'roleuser',
        parent = 'trole',
        pk     = ['user_id', 'role_id']
    ),
    troleperm  = dict(
        obj   = RolePerm,
        name  = 'roleperm',
        parent = 'trole',
        pk     = ['perm_id', 'role_id']
    )
)
