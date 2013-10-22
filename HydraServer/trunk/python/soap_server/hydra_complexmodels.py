from spyne.model.complex import Array as SpyneArray, ComplexModel
from spyne.model.primitive import String, Integer, Decimal, DateTime, Boolean, AnyDict
import datetime
from spyne.util.odict import odict


global FORMAT
FORMAT = "%Y-%m-%d %H:%M:%S.%f"
#"2013-08-13T15:55:43.468886Z"

def parse_value(data):
    """
    Turn a complex model object into a hydraiface - friendly value.
    """

    ns = '{soap_server.hydra_complexmodels}'
    data_type = data.type

    #attr_data.value is a dictionary,
    #but the keys have namespaces which must be stripped.
    val_names = data.value.keys()
    value = []
    for name in val_names:
        value.append(data.value[name])

    if data_type == 'descriptor':
        return value[0][0]
    elif data_type == 'timeseries':
        # The brand new way to parse time series data:
        ts = []
        for ts_val in value[0][0][ns + 'TimeSeriesData']:
            timestamp = ts_val[ns + 'ts_time'][0]
            try:
                ts_time = datetime.datetime.strptime(timestamp, FORMAT)
            except ValueError as e:
                if e.message.split(' ', 1)[0].strip() == 'unconverted':
                    utcoffset = e.message.split()[3].strip()
                    timestamp = timestamp.replace(utcoffset, '')
                    ts_time = datetime.datetime.strptime(timestamp, FORMAT)
                    # Apply offset
                    tzoffset = datetime.timedelta(hours=int(utcoffset[0:3]),
                                                  minutes=int(utcoffset[3:5]))
                    ts_time -= tzoffset
                else:
                    raise e

            # Convert time to Gregorian ordinal (1 = January 1st, year 1)
            ordinal_ts_time = ts_time.toordinal()
            fraction = (ts_time -
                        datetime.datetime(ts_time.year,
                                          ts_time.month,
                                          ts_time.day,
                                          0, 0, 0)).total_seconds()
            fraction = fraction / (86400)
            ordinal_ts_time += fraction

            series = []
            for val in ts_val[ns + 'ts_value']:
                series.append(eval(val))
            ts.append((ordinal_ts_time, series))
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
        #val = eval(value[0][0][ns + 'anyType'][0])
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

class HydraComplexModel(ComplexModel):
    error = String()

class Dataset(ComplexModel):
    _type_info = [
        ('type',             String),
        ('dimension',        String(default=None)),
        ('unit',             String(default=None)),
        ('name',             String(default=None)),
        ('value',            AnyDict),
    ]

class Descriptor(HydraComplexModel):
   _type_info = [
        ('desc_val', String),
    ]

class TimeSeriesData(HydraComplexModel):
 _type_info = [
        #('ts_time', DateTime),
        ('ts_time', String),
        ('ts_value', AnyDict),
    ]

class TimeSeries(HydraComplexModel):
    _type_info = [
        ('ts_values', SpyneArray(TimeSeriesData)),
    ]

class EqTimeSeries(HydraComplexModel):
    _type_info = [
        ('start_time', DateTime),
        ('frequency', Decimal),
        ('arr_data', AnyDict),
    ]

class Scalar(HydraComplexModel):
    _type_info = [
        ('param_value', Decimal),
    ]

class Array(HydraComplexModel):
    _type_info = [
        ('arr_data', AnyDict),
    ]

class Attr(HydraComplexModel):
    _type_info = [
        ('id', Integer),
        ('name', String),
        ('dimen', String),
    ]

class ResourceAttr(HydraComplexModel):
    _type_info = [
        ('id',      Integer),
        ('attr_id', Integer),
        ('ref_id',  Integer),
        ('ref_key', String),
        ('is_var',  Boolean),
    ]

class ResourceTemplateItem(HydraComplexModel):
    _type_info = [
        ('attr_id', Integer),
        ('template_id', Integer),
    ]

class ResourceTemplate(HydraComplexModel):
    _type_info = [
        ('name', String),
        ('id', Integer),
        ('group_id',    Integer(default=None)),
        ('resourcetemplateitems', SpyneArray(ResourceTemplateItem, default=[])),
    ]

class ResourceTemplateGroup(HydraComplexModel):
    _type_info = [
        ('id',   Integer),
        ('name', String),
        ('resourcetemplates', SpyneArray(ResourceTemplate, default=[])),
    ]

class Resource(HydraComplexModel):
    pass

class Node(Resource):
    _type_info = [
        ('id',          Integer),
        ('name',        String),
        ('description', String(default="")),
        ('layout',      String(default="")),
        ('x',           Decimal),
        ('y',           Decimal),
        ('status',      String),
        ('attributes',  SpyneArray(ResourceAttr, default=[])),
    ]
    def __eq__(self, other):
        if self.node_x == other.node_x and self.node_y == other.node_y \
        and self.node_name == other.node_name:
            return True
        else:
            return False

class Link(Resource):
    _type_info = [
        ('id',          Integer),
        ('name',        String),
        ('description', String(Default="")),
        ('layout',      String(Default="")),
        ('node_1_id',   Integer),
        ('node_2_id',   Integer),
        ('status',      String),
        ('attributes',  SpyneArray(ResourceAttr, default=[])),
    ]

class ResourceScenario(Resource):
    _type_info = [
        ('resource_attr_id', Integer),
        ('attr_id',          Integer),
        ('value',            Dataset),
    ]

class Scenario(Resource):
    _type_info = [
        ('id',                   Integer),
        ('name',                 String),
        ('description',          String),
        ('network_id',           Integer),
        ('status',               String),
        ('attributes',           SpyneArray(ResourceAttr, default=[])),
        ('resourcescenarios',    SpyneArray(ResourceScenario, default=[])),
    ]

class Network(Resource):
    _type_info = [
        ('project_id',          Integer),
        ('id',                  Integer),
        ('name',                String),
        ('description',         String),
        ('layout',              String),
        ('status',              String),
        ('attributes',          SpyneArray(ResourceAttr, default=[])),
        ('scenarios',           SpyneArray(Scenario, default=[])),
        ('nodes',               SpyneArray(Node, default=[])),
        ('links',               SpyneArray(Link, default=[])),
    ]

class NetworkSummary(Resource):
    _type_info = [
        ('project_id',          Integer),
        ('id',                  Integer),
        ('name',                String),
    ]

class Project(Resource):
    _type_info = [
        ('id',          Integer),
        ('name',        String),
        ('description', String),
        ('status',      String),
        ('cr_date',     String),
        ('created_by',  Integer),
        ('attributes',  SpyneArray(ResourceAttr, default=[])),

    ]

class ProjectSummary(Resource):
    _type_info = [
        ('id',          Integer),
        ('name',        String),
        ('cr_date',     String),
    ]

class ConstraintItem(HydraComplexModel):
    _type_info = [
        ('id',               Integer),
        ('constraint_id',    Integer),
        ('resource_attr_id', Integer),
    ]

class ConstraintGroup(HydraComplexModel):
    _type_info = [
        ('id',            Integer),
        ('constraint_id', Integer),
        ('op',            String),
        ('items',         SpyneArray(ConstraintItem, default=[]))
    ]

ConstraintGroup._type_info['groups'] = SpyneArray(ConstraintGroup, default=[])

class Constraint(HydraComplexModel):
    _type_info = [
        ('id',            Integer),
        ('scenario_id',   Integer),
        ('constant',      Decimal),
        ('op',            String),
        ('group',         ConstraintGroup),
    ]

class User(HydraComplexModel):
    _type_info = [
        ('id',  Integer),
        ('username', String),
        ('password', String),
    ]

class Perm(HydraComplexModel):
    _type_info = [
        ('id',   Integer),
        ('name', String),
    ]

class RoleUser(HydraComplexModel):
    _type_info = [
        ('user_id',  Integer),
    ]

class RolePerm(HydraComplexModel):
    _type_info = [
        ('perm_id',   Integer),
    ]


class Role(HydraComplexModel):
    _type_info = [
        ('id',     Integer),
        ('name',   String),
        ('roleperms', SpyneArray(RolePerm, default=[])),
        ('roleusers', SpyneArray(RoleUser, default=[])),
    ]

class PluginParam(HydraComplexModel):
    _type_info = [
        ('name',        String),
        ('value',       String),
    ]

class Plugin(HydraComplexModel):
    _type_info = [
        ('name',        String),
        ('location',    String),
        ('params',      SpyneArray(PluginParam, default=[])),
    ]


class ProjectOwner(HydraComplexModel):
    _type_info = [
        ('project_id',   Integer),
        ('user_id',   Integer),
    ]

class DatasetOwner(HydraComplexModel):
    _type_info = [
        ('dataset_id',   Integer),
        ('user_id',   Integer),
    ]
