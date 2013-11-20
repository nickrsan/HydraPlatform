from spyne.model.complex import Array as SpyneArray, ComplexModel
from spyne.model.primitive import Unicode, String, Integer, Decimal, DateTime, AnyDict
import datetime
from spyne.util.odict import odict

global FORMAT
FORMAT = "%Y-%m-%d %H:%M:%S.%f"
#"2013-08-13T15:55:43.468886Z"


def parse_value(data):
    """
    Turn a complex model object into a hydraiface - friendly value.
    """

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
        ns = '{soap_server.hydra_complexmodels}'
        ts = []
        for ts_val in value[0][0][ns + 'TimeSeriesData']:
            timestamp = ts_val[ns + 'ts_time'][0]
            # Check if we have received a seasonal time series first
            if timestamp[0:4] == 'XXXX':
                # Do seasonal time series stuff...
                timestamp = timestamp.replace('XXXX', '0001')
            # and proceed as usual
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
                    print ts_time, tzoffset, utcoffset
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
        print
        print value[0][0]
        print
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

class HydraComplexModel(ComplexModel):
    error = String()

class Dataset(ComplexModel):
    _type_info = [
        ('id',               Integer(min_occurs=1, default=None)),
        ('type',             String),
        ('dimension',        String(min_occurs=1, default=None)),
        ('unit',             String(min_occurs=1, default=None)),
        ('name',             String(min_occurs=1, default=None)),
        ('value',            AnyDict),
    ]

class Descriptor(ComplexModel):
   _type_info = [
        ('desc_val', String),
    ]

class TimeSeriesData(ComplexModel):
 _type_info = [
        #('ts_time', DateTime),
        ('ts_time', String),
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
        ('arr_data', AnyDict),
    ]

class DatasetGroupItem(ComplexModel):
    _type_info = [
        ('group_id', Integer(default=None)),
        ('dataset_id', Integer(default=None)),
    ]

class DatasetGroup(ComplexModel):
    _type_info = [
        ('group_name', String(default=None)),
        ('group_id'  , Integer(default=None)),
        ('datasetgroupitems', SpyneArray(DatasetGroupItem)),
    ]

class Attr(HydraComplexModel):
    _type_info = [
        ('id', Integer(default=None)),
        ('name', String(default=None)),
        ('dimen', String(default=None)),
    ]

class ResourceAttr(HydraComplexModel):
    _type_info = [
        ('id',      Integer(default=None)),
        ('attr_id', Integer(default=None)),
        ('ref_id',  Integer(default=None)),
        ('ref_key', String(default=None)),
        ('attr_is_var',  String(min_occurs=1, default='N')),
    ]

class ResourceTemplateItem(HydraComplexModel):
    _type_info = [
        ('attr_id',     Integer),
        ('template_id', Integer),
    ]

class ResourceTemplate(HydraComplexModel):
    _type_info = [
        ('id',                    Integer(default=None)),
        ('name',                  String(default=None)),
        ('group_id',              Integer(min_occurs=1, default=None)),
        ('resourcetemplateitems', SpyneArray(ResourceTemplateItem, default=[])),
    ]

class ResourceTemplateGroup(HydraComplexModel):
    _type_info = [
        ('id',                Integer(default=None)),
        ('name',              String(default=None)),
        ('resourcetemplates', SpyneArray(ResourceTemplate, default=[])),
    ]


class ResourceGroupSummary(HydraComplexModel):
    _type_info = [
        ('name',    String),
        ('templates', SpyneArray(String)),
    ]


class Resource(HydraComplexModel):
    pass

class Node(Resource):
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        String(default=None)),
        ('description', String(min_occurs=1, default="")),
        ('layout',      String(min_occurs=1, default="")),
        ('x',           Decimal(min_occurs=1, default=0)),
        ('y',           Decimal(min_occurs=1, default=0)),
        ('status',      String(default='A')),
        ('attributes',  SpyneArray(ResourceAttr, default=[])),
        ('templates',   SpyneArray(ResourceGroupSummary, default=[])),
    ]
    def __eq__(self, other):
        if self.node_x == other.node_x and self.node_y == other.node_y \
        and self.node_name == other.node_name:
            return True
        else:
            return False

class Link(Resource):
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        String(default=None)),
        ('description', String(min_occurs=1, default="")),
        ('layout',      String(min_occurs=1, default="")),
        ('node_1_id',   Integer(default=None)),
        ('node_2_id',   Integer(default=None)),
        ('status',      String(default='A')),
        ('attributes',  SpyneArray(ResourceAttr, default=[])),
        ('templates',   SpyneArray(ResourceGroupSummary, default=[])),
    ]

class ResourceScenario(Resource):
    _type_info = [
        ('resource_attr_id', Integer(default=None)),
        ('attr_id',          Integer(default=None)),
        ('value',            Dataset),
    ]

class ConstraintItem(HydraComplexModel):
    """
        A constraint item is the atomic element of a conatraint.
        The value of a constraint item can be value of a particular resource attribute
        or an arbitrary static value, such as an integer, so we can support:

        X.attr + Y.attr > 0
        or
        (X.attr * 5) + Y.attr > 0

    """
    _type_info = [
        ('id',               Integer(default=None)),
        ('constraint_id',    Integer(default=None)),
        ('resource_attr_id', Integer(default=None)),
        ('constant',            Unicode(default=None)),
    ]

class ConstraintGroup(HydraComplexModel):
    _type_info = [
        ('id',            Integer(default=None)),
        ('constraint_id', Integer(default=None)),
        ('op',            String(default=None, values=['+', '-', '*', '/', '^', 'and', 'or', 'not'])),
        ('constraintitems', SpyneArray(ConstraintItem, default=[]))
    ]

ConstraintGroup._type_info['constraintgroups'] = SpyneArray(ConstraintGroup, default=[])

class Constraint(HydraComplexModel):
    _type_info = [
        ('id',            Integer(default=None)),
        ('scenario_id',   Integer(default=None)),
        ('constant',      Decimal(default=None)),
        ('op',            String(default=None, values=['<', '>', '<=', '>=', '==', '!='])),
        ('value',         String(default=None)),
        ('constraintgroup', ConstraintGroup),
    ]

class Scenario(Resource):
    _type_info = [
        ('id',                   Integer(default=None)),
        ('name',                 String(default=None)),
        ('description',          String(min_occurs=1, default="")),
        ('network_id',           Integer(default=None)),
        ('status',               String(default='A')),
        ('resourcescenarios',    SpyneArray(ResourceScenario, default=[])),
        ('constraints',          SpyneArray(Constraint, default=[])),
        ('templates',            SpyneArray(ResourceGroupSummary, default=[])),
    ]

class Network(Resource):
    _type_info = [
        ('project_id',          Integer(default=None)),
        ('id',                  Integer(default=None)),
        ('name',                String(default=None)),
        ('description',         String(min_occurs=1, default=None)),
        ('layout',              String(min_occurs=1, default=None)),
        ('status',              String(default='A')),
        ('attributes',          SpyneArray(ResourceAttr, default=[])),
        ('scenarios',           SpyneArray(Scenario, default=[])),
        ('nodes',               SpyneArray(Node, default=[])),
        ('links',               SpyneArray(Link, default=[])),
        ('templates',           SpyneArray(ResourceGroupSummary, default=[])),
    ]

class NetworkSummary(Resource):
    _type_info = [
        ('project_id',          Integer(default=None)),
        ('id',                  Integer(default=None)),
        ('name',                String(default=None)),
    ]

class Project(Resource):
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        String(default=None)),
        ('description', String(default=None)),
        ('status',      String(default='A')),
        ('cr_date',     String(default=None)),
        ('created_by',  Integer(default=None)),
        ('attributes',  SpyneArray(ResourceScenario, default=[])),
    ]

class ProjectSummary(Resource):
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        String(default=None)),
        ('cr_date',     String(default=None)),
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
