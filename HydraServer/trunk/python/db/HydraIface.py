import util
import logging
from HydraLib.HydraException import DBException

global CNX
CNX = None

class IfaceBase(object):
    def __init__(self, class_name):
        logging.info("Initialising")
        global CNX
        if CNX is None:
            CNX = util.connect()
        self.db = IfaceDB(class_name)

        self.in_db = False

    def load(self):
        self.db.load()

    def commit(self):
        CNX.commit()

    def delete(self):
        if self.in_db:
            self.db.delete()

    def save(self):

        if self.in_db is True:
            if self.db.has_changed is True:
                self.db.update()
            else:
                logging.debug("No changes to %s, not continuing", self.db.table_name)
        else:
            self.db.insert()
            self.in_db = True

class IfaceDB(object):

    def __init__(self, class_name):
        self.db_attrs = []
        self.pk_attrs = []
        self.nullable_attrs = []
        self.attr_types = {}

        self.cursor = CNX.cursor()

        #This indicates whether any values in this table have changed.
        self.has_changed = False

        #this turns 'Project' into 'tProject' so it can identify the table
        self.table_name = "t%s"%class_name

        self.cursor.execute('desc %s' % self.table_name)

        table_desc = self.cursor.fetchall()

        logging.debug("Table desc: %s", table_desc)

        for col_desc in table_desc:
            col_name = col_desc[0]

            setattr(self, col_name, col_desc[4])
            self.db_attrs.append(col_name)

            self.attr_types[col_name] = col_desc[1]

            if col_desc[2] == 'YES':
                self.nullable_attrs.append(col_name)

            if col_desc[3] == 'PRI':
                self.pk_attrs.append(col_name)

            if col_desc[5] == 'auto_increment':
                self.seq = col_name

    def __setattr__(self, name, value):
        if name != 'db_attrs' and name in self.db_attrs:
            self.has_changed = True

        super(IfaceDB, self).__setattr__(name, value)

    def insert(self):
        #A function to return 'null' if the inputted value is None. Otherwise return the inputted value.
        base_insert = "insert into %(table)s (%(cols)s) values (%(vals)s);"
        complete_insert = base_insert % dict(
            table = self.table_name,
            cols  = ",".join([n for n in self.db_attrs]),
            vals  = ",".join([self.get_val(n) for n in self.db_attrs]),
        )

        logging.debug("Running insert: %s", complete_insert)
        old_seq = self.cursor.lastrowid

        self.cursor.execute(complete_insert)

        if old_seq is None or old_seq != self.cursor.lastrowid:
            setattr(self, self.seq, self.cursor.lastrowid)

    def update(self):
        #A function to return 'null' if the inputted value is None. Otherwise return the inputted value.

        base_update = "update %(table)s set %(sets)s where %(pk)s;"
        complete_update = base_update % dict(
            table = self.table_name,
            sets  = ",".join(["%s = %s"%(n, self.get_val(n)) for n in self.db_attrs]),
            pk    = " and ".join(["%s = %s"%(n, self.get_val(n)) for n in self.pk_attrs]),
        )
        logging.debug("Running update: %s", complete_update)
        self.cursor.execute(complete_update)

    def load(self):

        for pk in self.pk_attrs:
            if getattr(self, pk) is None:
                logging.info("Primary key is not set. Cannot load row from DB.")
                return None

        base_load = "select * from %(table_name)s where %(pk)s;"
        complete_load = base_load % dict(
            table_name = self.table_name,
            pk         = " and ".join(["%s = %s"%(n, self.get_val(n)) for n in self.pk_attrs]),
        )
        logging.debug("Running load: %s", complete_load)
        self.cursor.execute(complete_load)

        row_columns  = self.cursor.column_names
        row_data = self.cursor.fetchall()[0]#there should only be one entry

        if len(row_data) == 0:
            raise DBException("No entry found for table")

        for idx, column_name in enumerate(row_columns):
            logging.debug("Setting column %s to %s", column_name, row_data[idx])
            setattr(self, column_name, row_data[idx])

    def delete(self):
        base_load = "delete from %(table_name)s where %(pk)s;"
        complete_delete = base_load % dict(
            table_name = self.table_name,
            pk         = " and ".join(["%s = %s"%(n, self.get_val(n)) for n in self.pk_attrs]),
        )
        logging.debug("Running delete: %s", complete_delete)
        self.cursor.execute(complete_delete)

    def get_val(self, attr):
        val = getattr(self, attr)
        if val is None:
            return 'null'
        else:
            if self.attr_types[attr].find('varchar') >= 0:
                return "\'%s\'"%val
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
        
        self.db.group_id = None
        if group_id is not None:
            self.load()

class ResourceScenario(IfaceBase):
    """
        A resource scenario is what links the actual piece of data
        with a resource -- the data per resource will change per scenario.
    """
    def __init__(self, data_id = None, scenario_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.data_id = data_id
        self.db.scenario_id = scenario_id

        if data_id is not None and scenario_id is not None:
            self.load()

class ScenarioData(IfaceBase):
    """
        A table recording all pieces of data, including the
        type, units, name and dimension. The actual data value is stored
        in another table, which is identified based on the type.
    """
    def __init__(self, data_id = None, data_type = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.data_id = data_id
        self.db.data_type = data_type

        if data_id is not None and data_type is not None:
            self.load()

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

class TimeSeriesArray(IfaceBase):
    """
        The values contained in an equally spaced time series.
    """
    def __init__(self, ts_array_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.ts_array_id = ts_array_id
        if ts_array_id is not None:
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
    def __init__(self, constraint_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.constraint_id = constraint_id
        if constraint_id is not None:
            self.load()

class ConstraintGroup(IfaceBase):
    """
        a connector class for constraints. Used for grouping constraints
        into logical sections, not unlike parentheses in a mathematical equation.
    """
    def __init__(self, group_id = None):
        IfaceBase.__init__(self, self.__class__.__name__)

        self.db.group_id = group_id
        if group_id is not None:
            self.load()

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



