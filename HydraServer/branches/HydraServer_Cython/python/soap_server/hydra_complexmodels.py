from spyne.model.complex import Array as SpyneArray, ComplexModel 
from spyne.model.primitive import String, Integer, Decimal, DateTime, Boolean, AnyDict
from datetime import datetime 
from spyne.util.odict import odict

#This is on hold until we come up with a solution
#for dynamically generating the classes below.
#global typemap
#typemap = {
#    'varchar'    : String,
#    'int'       : Integer,
#    'double'    : Decimal,
#    'blob'      : SpyneArray,
#    'timestamp' : DateTime,
#}
#
#def get_spyne_type(db_type_dict):
#    type_info = []
#    for col_name, col_type in db_type_dict.items():
#        base_type = col_type.split('(')[0]
#        spyne_type = typemap[base_type]
#        type_info.append((col_name, spyne_type))
#    return type_info

global FORMAT
FORMAT = "%Y-%m-%d %H:%M:%S.%f"
#"2013-08-13T15:55:43.468886Z"

def parse_value(data_type, data):
    """
    Turn a complex model object into a hydraiface - friendly value.
    returns a tuple containing:
    (data_type, units, name, dimension, value)
    """
    #attr_data.value is a dictionary,
    #but the keys have namespaces which must be stripped.
    val_names = data.value.keys()
    value = []
    for name in val_names:
        value.append(data.value[name])

    if data_type == 'descriptor':
        return value[0][0]
    elif data_type == 'timeseries':
        ts = []
        for ts_val in value[0]:
            ts_time = datetime.strptime(ts_val[0][0], FORMAT) 
            series = eval(ts_val[1][0])
            ts.append((ts_time, series))
        return ts
    elif data_type == 'eqtimeseries':
        start_time = datetime.strptime(value[0][0], FORMAT)
        frequency  = value[1][0]
        arr_data   = eval(value[2][0])
        return (start_time, frequency, arr_data)
    elif data_type == 'scalar':
       return value[0][0] 
    elif data_type == 'array':
        val = eval(value[0][0])
        return val


def get_array(arr):
    
    if len(arr) == 0:
        return []
    
    #am I a dictionary? If so, i'm only iterested in the values
    if type(arr) is odict:
        arr = arr[0]

    if type(arr[0]) is str:
        return [float(val) for val in arr]

    #arr must therefore be a list.
    current_level = []
    for level in arr:
        current_level.append(get_array(level))

    return current_level

class Descriptor(ComplexModel):
   _type_info = [
        ('desc_val', String),
    ]

class TimeSeriesData(ComplexModel):
 _type_info = [
        ('ts_time', DateTime),
        ('ts_value', AnyDict),
    ]

class TimeSeries(ComplexModel):
    _type_info = [
        ('ts_values', SpyneArray(TimeSeriesData)),
    ]
    
class EqTimeSeries(ComplexModel):
    _type_info = [
        ('start_time', DateTime),
        ('frequency', Decimal),
        ('arr_data', AnyDict),
    ]

class Scalar(ComplexModel):
    _type_info = [
        ('param_value', Decimal),
    ]

class Array(ComplexModel):
    _type_info = [
        ('arr_data', SpyneArray(AnyDict)),
    ]

class Attr(ComplexModel):
    _type_info = [
        ('id', Integer),
        ('name', String),
        ('dimen', String),
    ]

class ResourceAttr(ComplexModel):
    _type_info = [
        ('id',      Integer),
        ('attr_id', Integer),
        ('ref_id',  Integer),
        ('ref_key', String),
        ('is_var',  Boolean),
    ]

class TemplateGroup(ComplexModel):
    _type_info = [
        ('id',   Integer),
        ('name', String),
    ]

class Template(ComplexModel):
    _type_info = [
        ('name', Integer),
        ('id', Integer),
        ('group_id',    Integer)
    ]

class TemplateItem(ComplexModel):
    _type_info = [
        ('attr_id', Integer),
        ('template_id', Integer),
    ]

class Node(ComplexModel):
    _type_info = [
        ('id',          Integer),
        ('name',        String),
        ('description', String),
        ('x',           Decimal),
        ('y',           Decimal),
        ('status',      String),
        ('attributes',  SpyneArray(ResourceAttr)),
    ]
    def __eq__(self, other):
        if self.node_x == other.node_x and self.node_y == other.node_y \
        and self.node_name == other.node_name:
            return True
        else:
            return False

class Link(ComplexModel):
    _type_info = [
        ('id',          Integer),
        ('name',        String),
        ('description', String),
        ('node_1_id',   Integer),
        ('node_2_id',   Integer),
        ('status',      String),
        ('attributes',  SpyneArray(ResourceAttr)),
    ]

class ResourceScenario(ComplexModel):
    _type_info = [
        ('resource_attr_id', Integer),
        ('attr_id',          Integer),
        ('type',             String),
        ('value',            AnyDict),
    ]

class Scenario(ComplexModel):
    _type_info = [
        ('id',                   Integer),
        ('name',                 String),
        ('description',          String),
        ('network_id',           Integer),
        ('status',               String),
        ('attributes',           SpyneArray(ResourceAttr)),
        ('resourcescenarios',    SpyneArray(ResourceScenario)),
    ]

class Network(ComplexModel):
    _type_info = [
        ('project_id',          Integer),
        ('id',                  Integer),
        ('name',                String),
        ('description',         String),
        ('status',              String),
        ('attributes',          SpyneArray(ResourceAttr)),
        ('scenarios',           SpyneArray(Scenario)),
        ('nodes',               SpyneArray(Node)),
        ('links',               SpyneArray(Link)),
    ]

class Project(ComplexModel):
    _type_info = [
        ('id',          Integer),
        ('name',        String),
        ('description', String),
        ('status',      String),
        ('attributes',  SpyneArray(ResourceAttr)),

    ]

class ConstraintItem(ComplexModel):
    _type_info = [
        ('id',               Integer),
        ('constraint_id',    Integer),
        ('resource_attr_id', Integer),
        ('resource_attr',    ResourceAttr),
    ]

class ConstraintGroup(ComplexModel):
    _type_info = [
        ('id',            Integer),
        ('constraint_id', Integer),
        ('op',            String),
        ('ref_key_1',     String),
        ('ref_id_1',      Integer),
        ('ref_key_2',     String),
        ('ref_id_2',      Integer),
    ]

class Constraint(ComplexModel):
    _type_info = [
        ('id',            Integer),
        ('scenario_id',   Integer),
        ('group_id',      Integer),
        ('constant',      Decimal),
        ('op',            String),
        ('groups',        SpyneArray(ConstraintGroup)),
        ('items',         SpyneArray(ConstraintItem)),
    ]

class User(ComplexModel):
    _type_info = [
        ('user_id',  Integer),
        ('username', String),
        ('password', String),
    ]

class Perm(ComplexModel):
    _type_info = [
        ('perm_id',   Integer),
        ('perm_name', String),
    ]

class Role(ComplexModel):
    _type_info = [
        ('role_id',     Integer),
        ('role_name',   String),
        ('permissions', SpyneArray(Perm)),
        ('users',       SpyneArray(User)),
    ]

class RoleUser(ComplexModel):
    _type_info = [
        ('role_id',   Integer),
        ('role_name', String),
           ]

class PluginParam(ComplexModel):
    _type_info = [
        ('name',        String),
        ('value',       String),
    ]

class Plugin(ComplexModel):
    _type_info = [
        ('plugin_name',        String),
        ('plugin_description', String),
        ('params',           SpyneArray(PluginParam)),
    ]




