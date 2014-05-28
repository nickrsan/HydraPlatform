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
from spyne.model.complex import Array as SpyneArray, ComplexModel
from spyne.model.primitive import Unicode
from spyne.model.primitive import String
from spyne.model.primitive import Integer
from spyne.model.primitive import Decimal
from spyne.model.primitive import Float
from spyne.model.primitive import DateTime
from spyne.model.primitive import AnyDict
from spyne.model.primitive import Double
from spyne.util.odict import odict
import sys
from HydraLib.util import timestamp_to_ordinal
import logging
from numpy import array
global FORMAT
FORMAT = "%Y-%m-%d %H:%M:%S.%f"
#"2013-08-13T15:55:43.468886Z"
NS = "soap_server.hydra_complexmodels"
current_module = sys.modules[__name__]
log = logging.getLogger(__name__)


def get_as_complexmodel(ctx, iface_obj, **kwargs):

    kwargs['user_id'] = int(ctx.in_header.user_id)
    obj_dict    = iface_obj.get_as_dict(**kwargs)
    object_type = obj_dict['object_type']

    cm = getattr(current_module, object_type)(obj_dict)

    return cm

def parse_value(data):
    """
        Turn a complex model object into a hydraiface - friendly value.
    """

    data_type = data.type

    if data.value is None:
        log.warn("Cannot parse dataset. No value specified.")
        return None

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
        for ts_val in value[0]:
            #The value is a list, so must get index 0
            timestamp = ts_val['{%s}ts_time'%NS][0]
            # Check if we have received a seasonal time series first
            ordinal_ts_time = timestamp_to_ordinal(timestamp)
            arr_data = ts_val['{%s}ts_value'%NS][0]
            if type(arr_data) is odict:
                try:
                    ts_value = eval(arr_data)
                except:
                    ts_value = parse_array(arr_data)
            else:
                try:
                    ts_value = float(arr_data)
                except:
                    ts_value = arr_data

            ts.append((ordinal_ts_time, ts_value))

        return ts
    elif data_type == 'eqtimeseries':
        start_time = data.value['{%s}start_time'%NS][0]
        start_time = timestamp_to_ordinal(start_time) 
        frequency  = data.value['{%s}frequency'%NS][0]
        arr_data   = data.value['{%s}arr_data'%NS][0]
        try:
            val = eval(arr_data)
        except:
            val = parse_array(arr_data)

        arr_data   = val

        return (start_time, frequency, arr_data)
    elif data_type == 'scalar':
        return value[0][0]
    elif data_type == 'array':
        arr_data   = data.value['{%s}arr_data'%NS][0]

        try:
            val = eval(arr_data)
        except:
            val = parse_array(arr_data)
        return val

def parse_array(arr):
    """
        Take a list of nested dictionaries and return a python list containing
        a single value, a string or sub lists.
    """
    ret_arr = []
    sub_arr = arr.get('{%s}array'%NS, None)
    for val in sub_arr:
        sub_arr = val.get('{%s}array'%NS, None)
        if sub_arr is not None:
            ret_arr.append(parse_array(val))
            continue

        actual_vals = val.get('{%s}item'%NS)
        for v in actual_vals:
            try:
                ret_arr.append(float(v))
            except:
                ret_arr.append(v)
    return ret_arr

def create_dict(arr_data):
    arr_data = array(arr_data)

    arr = {'array': []}
    if arr_data.ndim == 0:
        return {'array': []}
    elif arr_data.ndim == 1:
        return {'array': [{'item': list(arr_data)}]}
    else:
        for a in arr_data:
            val = create_dict(a)
            arr['array'].append(val)

        return arr


class LoginResponse(ComplexModel):
    __namespace__ = 'soap_server.hydra_complexmodels'
    _type_info = [
        ('session_id', String(min_occurs=1)),
        ('user_id',    Integer(min_occurs=1)),
    ]

class HydraComplexModel(ComplexModel):
    __namespace__ = 'soap_server.hydra_complexmodels'

    error = SpyneArray(String())

    def __init__(self, obj_dict=None):
        super(HydraComplexModel, self).__init__()

        if obj_dict is None:
            return

        name = obj_dict['object_type']


        for attr_name, attr in obj_dict.items():
            if attr_name == 'object_type':
                continue
            elif attr_name.find('layout') >= 0:
                if attr:
                    attr = eval(attr)
                else:
                    attr = {}
            if type(attr) is list:
                children = []
                for child_obj_dict in attr:
                    if child_obj_dict is not None:
                        cm = getattr(current_module, child_obj_dict['object_type'])(child_obj_dict)
                        children.append(cm)
                setattr(self, attr_name, children)
            elif type(attr) is dict and attr.get('object_type') is not None:
                cm = getattr(current_module, attr['object_type'])(attr)

                setattr(self, attr_name, cm)
            else:
                #turn someething like 'project_name' into just 'name'
                #So that it's project.name instead of project.project_name.
                attr_prefix = "%s_"%name.lower()
                if attr_name.find(attr_prefix) == 0:
                    attr_name = attr_name.replace(attr_prefix, "")
                setattr(self, attr_name, attr)

class Metadata(ComplexModel):
    _type_info = [
        ('name'  , String(min_occurs=1, default=None)),
        ('value' , String(min_occurs=1, default=None)),
    ]

    def __init__(self, obj_dict=None):
        if obj_dict is None:
            return
        self.name    = obj_dict['metadata_name']
        self.value   = obj_dict['metadata_val']



class Dataset(HydraComplexModel):
    _type_info = [
        ('id',               Integer(min_occurs=1, default=None)),
        ('type',             String),
        ('dimension',        String(min_occurs=1, default=None)),
        ('unit',             String(min_occurs=1, default=None)),
        ('name',             String(min_occurs=1, default=None)),
        ('value',            AnyDict(min_occurs=1, default=None)),
        ('locked',           String(min_occurs=1, default='N', pattern="[YN]")),
        ('metadata',         SpyneArray(Metadata, default=[])),
    ]

    def __init__(self, obj_dict=None):
        super(Dataset, self).__init__()
        if obj_dict is None:
            return

        self.locked    = obj_dict['locked']
        self.id        = obj_dict['dataset_id']
        self.type     = obj_dict['data_type']
        self.name      = obj_dict['data_name']

        self.dimension = obj_dict['data_dimen']
        self.unit      = obj_dict['data_units']

        if obj_dict.get('value', None):
            val = obj_dict['value']

            self.value     = getattr(current_module, val['object_type'])(val)
            self.value     = self.value.__dict__
        
        metadata = []
        for m in obj_dict.get('metadatas', []):
            complex_m = Metadata(m).__dict__
            metadata.append(complex_m)
        self.metadata = metadata

class Descriptor(HydraComplexModel):
    _type_info = [
        ('desc_val', String),
    ]

    def __init__(self, val=None):
        super(Descriptor, self).__init__()
        if val is None:
            return
        self.desc_val = [val['desc_val']]

class TimeSeriesData(HydraComplexModel):
    _type_info = [
        #('ts_time', DateTime),
        ('ts_time', String),
        ('ts_value', AnyDict),
    ]

    def __init__(self, val=None):
        super(TimeSeriesData, self).__init__()
        if val is None:
            return
        self.ts_time  = [val['ts_time']]

        ts_value = val['ts_value']
        if type(ts_value) is list:
            self.ts_value = [create_dict(ts_value)]
        else:
            self.ts_value = [ts_value]

class TimeSeries(HydraComplexModel):
    _type_info = [
        ('ts_values', SpyneArray(TimeSeriesData)),
    ]

    def __init__(self, val=None):
        super(TimeSeries, self).__init__()
        if val is None:
            return
        ts_vals = []
        for ts in val['timeseriesdatas']:
            ts_vals.append(TimeSeriesData(ts).__dict__)
        self.ts_values = ts_vals

class EqTimeSeries(HydraComplexModel):
    """
        An equally spaced timeseries value.
        Frequency is stored in seconds
        Value must be an array.
    """
    _type_info = [
        ('start_time', DateTime),
        ('frequency', Decimal),
        ('arr_data',  AnyDict),
    ]

    def __init__(self, val=None):
        super(EqTimeSeries, self).__init__()
        if val is None:
            return

        self.start_time = [val['start_time']]
        self.frequency  = [val['frequency']]
        self.arr_data   = [create_dict(val['arr_data'])]

class Scalar(HydraComplexModel):
    _type_info = [
        ('param_value', Float),
    ]
    def __init__(self, val=None):
        super(Scalar, self).__init__()
        if val is None:
            return
        self.param_value = [val['param_value']]

class Array(HydraComplexModel):
    _type_info = [
        ('arr_data', AnyDict),
    ]
    def __init__(self, val=None):
        super(Array, self).__init__()
        if val is None:
            return
        arr_data = val['arr_data']
        if type(arr_data) is list:
            self.arr_data = [create_dict(arr_data)]
        else:
            self.arr_data = [arr_data]

class DatasetGroupItem(HydraComplexModel):
    _type_info = [
        ('group_id', Integer(default=None)),
        ('dataset_id', Integer(default=None)),
    ]

class DatasetGroup(HydraComplexModel):
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

    def __init__(self, obj_dict=None):
        super(ResourceAttr, self).__init__(obj_dict)
        if obj_dict is None:
            return

        self.id = obj_dict['resource_attr_id']

class ResourceTypeDef(HydraComplexModel):
    _type_info = [
        ('ref_key', String(default=None)),
        ('ref_id',  Integer(default=None)),
        ('type_id', Integer(default=None)),
    ]

class TypeAttr(HydraComplexModel):
    _type_info = [
        ('attr_id',   Integer(default=None)),
        ('attr_name', String(default=None)),
        ('type_id',   Integer(default=None)),
        ('data_type', String(default=None)),
        ('dimension', String(default=None)),
        ('default_dataset_id', Integer(default=None)),
        ('is_var',    String(default=None)),
    ]

class TemplateType(HydraComplexModel):
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        String(default=None)),
        ('alias',       String(default=None)),
        ('layout',      AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('template_id', Integer(min_occurs=1, default=None)),
        ('typeattrs',   SpyneArray(TypeAttr, default=[])),
    ]

    def __init__(self, obj_dict=None):
        super(TemplateType, self).__init__()
        if obj_dict is None:
            return

        self.id        = obj_dict['type_id']
        self.name      = obj_dict['type_name']
        self.alias     = obj_dict['alias']
        if obj_dict['layout'] is not None:
            self.layout    = eval(obj_dict['layout'])
        else:
            self.layout = {}
        self.template_id  = obj_dict['template_id']

        typeattrs = []
        for typeattr_dict in obj_dict['typeattrs']:
            typeattrs.append(TypeAttr(typeattr_dict))

        self.typeattrs = typeattrs

class Template(HydraComplexModel):
    _type_info = [
        ('id',        Integer(default=None)),
        ('name',      String(default=None)),
        ('layout',    AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('types',     SpyneArray(TemplateType, default=[])),
    ]

    def __init__(self, obj_dict=None):
        super(Template, self).__init__(obj_dict)
        if obj_dict is None:
            return

        self.name   = obj_dict['template_name']
        self.id     = obj_dict['template_id']
        if obj_dict['layout'] is not None:
            self.layout    = eval(obj_dict['layout'])
        else:
            self.layout = {}

        types = []
        for templatetype in obj_dict['templatetypes']:
            types.append(TemplateType(templatetype))
        self.types = types

class TypeSummary(HydraComplexModel):
    _type_info = [
        ('name',    String(default=None)),
        ('id',      Integer),
        ('template_name', String(default=None)),
        ('template_id', Integer(default=None)),
    ]

    def __init__(self, obj_dict=None):
        super(TypeSummary, self).__init__()

        if obj_dict is None:
            return

        self.name = obj_dict['type_name']
        self.id   = obj_dict['type_id']
        self.template_name = obj_dict['template_name']
        self.template_id   = obj_dict['template_id']

class Resource(HydraComplexModel):
    pass

class Node(Resource):
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        String(default=None)),
        ('description', String(min_occurs=1, default="")),
        ('layout',      AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('x',           Decimal(min_occurs=1, default=0)),
        ('y',           Decimal(min_occurs=1, default=0)),
        ('status',      String(default='A', pattern="[AX]")),
        ('attributes',  SpyneArray(ResourceAttr, default=[])),
        ('types',       SpyneArray(TypeSummary, default=[])),
    ]
    def __eq__(self, other):
        super(Node, self).__init__()
        if self.x == other.x and self.y == other.y \
        and self.name == other.name:
            return True
        else:
            return False

class Link(Resource):
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        String(default=None)),
        ('description', String(min_occurs=1, default="")),
        ('layout',      AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('node_1_id',   Integer(default=None)),
        ('node_2_id',   Integer(default=None)),
        ('status',      String(default='A', pattern="[AX]")),
        ('attributes',  SpyneArray(ResourceAttr, default=[])),
        ('types',       SpyneArray(TypeSummary, default=[])),
    ]

class ResourceScenario(Resource):
    _type_info = [
        ('resource_attr_id', Integer(default=None)),
        ('attr_id',          Integer(default=None)),
        ('value',            Dataset),
    ]

    def __init__(self, obj_dict=None):
        super(ResourceScenario, self).__init__()
        if obj_dict is None:
            return
        self.resource_attr_id = obj_dict['resource_attr_id']
        self.attr_id          = obj_dict['attr_id']

        self.value = Dataset(obj_dict['value'])

class ResourceGroupItem(HydraComplexModel):
    _type_info = [
        ('id',       Integer(default=None)),
        ('ref_id',   Integer(default=None)),
        ('ref_key',  String(default=None)),
        ('group_id', Integer(default=None)),
    ]

    def __init__(self, obj_dict=None):
        super(ResourceGroupItem, self).__init__(obj_dict)
        if obj_dict is None:
            return
        self.id = obj_dict['item_id']

class ResourceGroup(HydraComplexModel):
    _type_info = [
        ('id',          Integer(default=None)),
        ('network_id',  Integer(default=None)),
        ('name',        String(default=None)),
        ('description', String(min_occurs=1, default="")),
        ('status',      String(default='A', pattern="[AX]")),
        ('attributes',  SpyneArray(ResourceAttr, default=[])),
        ('types',       SpyneArray(TypeSummary, default=[])),
    ]

    def __init__(self, obj_dict=None):
        super(ResourceGroup, self).__init__(obj_dict)

        if obj_dict is None:
            return

        name = obj_dict['object_type']

        for attr_name, attr in obj_dict.items():
            if attr_name == 'object_type':
                continue
            if attr_name.find('group')==0:
                attr_name = attr_name.replace('group_', '')
                setattr(self, attr_name, attr)

class ResourceGroupDiff(HydraComplexModel):
    _type_info = [
       ('scenario_1_items', SpyneArray(ResourceGroupItem, default=[])),
       ('scenario_2_items', SpyneArray(ResourceGroupItem, default=[]))
    ]

class ConstraintItem(HydraComplexModel):
    """
        A constraint item is the atomic element of a conatraint.  The value of
        a constraint item can be value of a particular resource attribute or an
        arbitrary static value, such as an integer, so we can support:

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

    def __init__(self, obj_dict=None):
        super(Constraint, self).__init__()

        if obj_dict is None:
            return

        self.id          = obj_dict['constraint_id']
        self.scenario_id = obj_dict['scenario_id']
        self.constant    = obj_dict['constant']
        self.op          = obj_dict['op']
        self.group_id    = obj_dict['group_id']
        self.constraintgroups = obj_dict['constraintgroups']
        self.constraintitems  = obj_dict['constraintitems']

        base_grp = self.get_group(self.group_id)

        self.value = self.eval_constraintgroup(base_grp)

    def get_group(self, group_id):
        group = None
        for grp in self.constraintgroups:
            if grp['group_id'] == group_id:
                return grp

    def get_item(self, item_id):
        item = None
        for i in self.constraintitems:
            if i['item_id'] == item_id:
                return i

    def eval_constraintgroup(self, group):
        """
            Turn the group -> item/group structure into a string.
        """
        op = group['op']

        if group['ref_key_1'] == 'GRP':
            sub_grp = self.get_group(group['ref_id_1'])
            str_1 = self.eval_constraintgroup(sub_grp)
        else:
            sub_item = self.get_item(group['ref_id_1'])
            str_1 = self.eval_constraintitem(sub_item)

        if group['ref_key_2'] == 'GRP':
            sub_grp = self.get_group(group['ref_id_2'])
            str_2 = self.eval_constraintgroup(sub_grp)
        else:
            sub_item = self.get_item(group['ref_id_2'])
            str_2 = self.eval_constraintitem(sub_item)

        return "(%s %s %s)"%(str_1, op, str_2)

    def eval_constraintitem(self, item):
        """
            Turn a constraint item into a string
        """
        if item['constant'] is not None:
            return item['constant']
        else:
            ref_key = item['ref_key']
            ref_id  = item['ref_id']
            attr_name = item['attr_name']
            item_str = "%s[%s][%s]" % (ref_key, ref_id, attr_name)
            return item_str

class Scenario(Resource):
    _type_info = [
        ('id',                   Integer(default=None)),
        ('name',                 String(default=None)),
        ('description',          String(min_occurs=1, default="")),
        ('network_id',           Integer(default=None)),
        ('status',               String(default='A', pattern="[AX]")),
        ('locked',               String(default='N', pattern="[YN]")),
        ('start_time',           String(default=None)),
        ('end_time',             String(default=None)),
        ('time_step',            String(default=None)),
        ('resourcescenarios',    SpyneArray(ResourceScenario, default=[])),
        ('constraints',          SpyneArray(Constraint, default=[])),
        ('resourcegroupitems',   SpyneArray(ResourceGroupItem, default=[])),
    ]

class ResourceScenarioDiff(HydraComplexModel):
    _type_info = [
        ('resource_attr_id',     Integer(default=None)),
        ('scenario_1_dataset',   Dataset),
        ('scenario_2_dataset',   Dataset),
    ]

class ConstraintDiff(HydraComplexModel):
    _type_info = [
        #Constraints common to both scenarios
        ('common_constraints',  SpyneArray(Constraint, default=[])),
        #Constraints in scenario 1 but not in scenario 2
        ('scenario_1_constraints', SpyneArray(Constraint, default=[])),
        #Constraints in scenario 2 but not in scenario 1
        ('scenario_2_constraints', SpyneArray(Constraint, default=[])),
    ]

class ScenarioDiff(HydraComplexModel):
    _type_info = [
        ('resourcescenarios',    SpyneArray(ResourceScenarioDiff, default=[])),
        ('constraints',          ConstraintDiff),
        ('groups',               ResourceGroupDiff),
    ]

class Network(Resource):
    _type_info = [
        ('project_id',          Integer(default=None)),
        ('id',                  Integer(default=None)),
        ('name',                String(default=None)),
        ('description',         String(min_occurs=1, default=None)),
        ('created_by',          Integer(default=None)),
        ('cr_date',             String(default=None)),
        ('layout',              AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('status',              String(default='A', pattern="[AX]")),
        ('attributes',          SpyneArray(ResourceAttr, default=[])),
        ('scenarios',           SpyneArray(Scenario, default=[])),
        ('nodes',               SpyneArray(Node, default=[])),
        ('links',               SpyneArray(Link, default=[])),
        ('resourcegroups',      SpyneArray(ResourceGroup, default=[])),
        ('types',               SpyneArray(TypeSummary, default=[])),
    ]

class NetworkSummary(Resource):
    _type_info = [
        ('project_id',          Integer(default=None)),
        ('id',                  Integer(default=None)),
        ('name',                String(default=None)),
    ]

class NetworkExtents(Resource):
    _type_info = [
        ('network_id', Integer(default=None)),
        ('min_x',      Decimal(default=0)),
        ('min_y',      Decimal(default=0)),
        ('max_x',      Decimal(default=0)),
        ('max_y',      Decimal(default=0)),
    ]

class Project(Resource):
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        String(default=None)),
        ('description', String(default=None)),
        ('status',      String(default='A', pattern="[AX]")),
        ('cr_date',     String(default=None)),
        ('created_by',  Integer(default=None)),
        ('attributes',  SpyneArray(ResourceScenario, default=[])),
    ]

class ProjectSummary(Resource):
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        String(default=None)),
        ('cr_date',     String(default=None)),
        ('created_by',  Integer(default=None)),
    ]

class User(HydraComplexModel):
    _type_info = [
        ('id',  Integer),
        ('username', String(default=None)),
        ('display_name', String(default=None)),
        ('password', String(default=None)),
    ]

class Perm(HydraComplexModel):
    _type_info = [
        ('id',   Integer),
        ('name', String),
        ('code', String),
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
        ('code',   String),
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

class Owner(HydraComplexModel):
    _type_info = [
        ('ref_id',   Integer),
        ('ref_key',  String),
        ('user_id',  Integer),
        ('edit',     String),
        ('view',     String)
    ]

class Unit(HydraComplexModel):
    _type_info = [
        ('name', String),
        ('abbr', String),
        ('cf', Double),
        ('lf', Double),
        ('info', String),
        ('dimension', String),
    ]
