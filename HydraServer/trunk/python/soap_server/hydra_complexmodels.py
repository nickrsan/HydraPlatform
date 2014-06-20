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
from spyne.model.primitive import Integer
from spyne.model.primitive import Decimal
from spyne.model.primitive import Float
from spyne.model.primitive import DateTime
from spyne.model.primitive import AnyDict
from spyne.model.primitive import Double
from decimal import Decimal as Dec
import sys
from HydraLib.util import timestamp_to_ordinal, ordinal_to_timestamp
import logging
from numpy import array
global FORMAT
FORMAT = "%Y-%m-%d %H:%M:%S.%f"
#"2013-08-13T15:55:43.468886Z"
NS = "soap_server.hydra_complexmodels"
current_module = sys.modules[__name__]
log = logging.getLogger(__name__)

def get_timestamp(ordinal):
    if ordinal is None:
        return None

    if type(ordinal) in (str, unicode):
        ordinal = Dec(ordinal)
    timestamp = str(ordinal_to_timestamp(ordinal))
    return timestamp

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
        ('session_id', Unicode(min_occurs=1)),
        ('user_id',    Integer(min_occurs=1)),
    ]

class HydraComplexModel(ComplexModel):
    __namespace__ = 'soap_server.hydra_complexmodels'

    error = SpyneArray(Unicode())

    def __init__(self, sa_obj=None):
        super(HydraComplexModel, self).__init__()

        if sa_obj is None:
            return

        name = sa_obj.__table__.name[1:]
        cols = [c.name for c in sa_obj.__table__.columns]

        for col in cols:
            val = getattr(sa_obj, col)
            setattr(self, col, val)

            if col == 'cr_date':
                val = str(val)

            if col == 'layout':
                if val:
                    val = eval(val)
                else:
                    val = {}
            #turn someething like 'project_name' into just 'name'
            #So that it's project.name instead of project.project_name.

            attr_prefix = "%s_"%name.lower()
            if col.find(attr_prefix) == 0:
                attr_name = col.replace(attr_prefix, "")
                setattr(self, attr_name, val)

class Metadata(ComplexModel):
    _type_info = [
        ('name'  , Unicode(min_occurs=1, default=None)),
        ('value' , Unicode(min_occurs=1, default=None)),
    ]

    def __init__(self, parent=None):
        if parent is None:
            return
        self.name    = parent.metadata_name
        self.value   = parent.metadata_val



class Dataset(HydraComplexModel):
    _type_info = [
        ('id',               Integer(min_occurs=1, default=None)),
        ('type',             Unicode),
        ('dimension',        Unicode(min_occurs=1, default=None)),
        ('unit',             Unicode(min_occurs=1, default=None)),
        ('name',             Unicode(min_occurs=1, default=None)),
        ('value',            AnyDict(min_occurs=1, default=None)),
        ('locked',           Unicode(min_occurs=1, default='N', pattern="[YN]")),
        ('metadata',         SpyneArray(Metadata, default=None)),
    ]

    def __init__(self, parent=None):
        super(Dataset, self).__init__()
        if  parent is None:
            return

        self.locked    = parent.locked
        self.id        = parent.dataset_id
        self.type      = parent.data_type
        self.name      = parent.data_name

        self.dimension = parent.data_dimen
        self.unit      = parent.data_units
        if parent.value is not None: 
            if parent.data_type == 'descriptor':
                self.value = Descriptor(parent.value)
            elif parent.data_type == 'array':
                self.value = Array(parent.value)
            elif parent.data_type == 'scalar':
                self.value = Scalar(parent.value)
            elif parent.data_type == 'eqtimeseries':
                self.value = EqTimeSeries(parent.start_time, parent.frequency, parent.value)
        if parent.timeseriesdata and len(parent.timeseriesdata) > 0:
            self.value = TimeSeries(parent.timeseriesdata)
        
        if self.value:
            self.value = self.value.__dict__
        
        metadata = []
        for m in parent.metadata:
            complex_m = Metadata(m)
            metadata.append(complex_m)
        self.metadata = metadata

    def parse_value(self):
        """
            Turn a complex model object into a  - friendly value.
        """

        is_soap_req = False
        data = str(self.value)
        if data.find('{%s}'%NS) >= 0:
            data = data.replace('{%s}'%NS, '')
            is_soap_req = True
        data = eval(data)
        
        if data is None:
            log.warn("Cannot parse dataset. No value specified.")
            return None
       
        data_type = self.type

        #attr_data.value is a dictionary,
        #but the keys have namespaces which must be stripped.
        val_names = data.keys()
        value = []
        for name in val_names:
            value.append(data[name])

        if data_type == 'descriptor':
            return value[0][0]
        elif data_type == 'timeseries':

            # The brand new way to parse time series data:
            ts = []
            for ts_val in value[0]:
                #The value is a list, so must get index 0
                timestamp = ts_val['ts_time']
                if is_soap_req:
                    timestamp = timestamp[0]
                # Check if we have received a seasonal time series first
                ordinal_ts_time = timestamp_to_ordinal(timestamp)
                arr_data = ts_val['ts_value']
                if is_soap_req:
                    arr_data = arr_data[0]
                try:
                    arr_data = dict(arr_data)
                    try:
                        ts_value = eval(arr_data)
                    except:
                        ts_value = self.parse_array(arr_data)
                except:
                    try:
                        ts_value = float(arr_data)
                    except:
                        ts_value = arr_data

                ts.append((ordinal_ts_time, str(ts_value)))

            return ts
        elif data_type == 'eqtimeseries':
            start_time = data['start_time']
            frequency  = data['frequency']
            arr_data   = data['arr_data']
            if is_soap_req:
                start_time = start_time[0]
                frequency = frequency[0]
                arr_data = arr_data[0]
            start_time = timestamp_to_ordinal(start_time) 

            log.info(arr_data)
            try:
                val = eval(arr_data)
            except:
                val = self.parse_array(arr_data)

            arr_data   = str(val)

            return (start_time, frequency, arr_data)
        elif data_type == 'scalar':
            val = value[0]
            if type(value) == list:
                val = value[0]
            return val
        elif data_type == 'array':
            
            arr_data = data['arr_data']
            if is_soap_req:
                arr_data = arr_data[0]
            logging.info("Arr data: %s", arr_data)
            if type(arr_data) == dict:
                val = self.parse_array(arr_data)
            else:
                val = eval(arr_data)

            return str(val)


    def parse_array(self, arr):
        """
            Take a list of nested dictionaries and return a python list containing
            a single value, a string or sub lists.
        """
        ret_arr = []
        sub_arr = arr.get('array', None)
        for val in sub_arr:
            sub_arr = val.get('array', None)
            if sub_arr is not None:
                ret_arr.append(self.parse_array(val))
                continue

            actual_vals = val.get('item')
            for v in actual_vals:
                try:
                    ret_arr.append(float(v))
                except:
                    ret_arr.append(v)
        return ret_arr

    def get_metadata_as_dict(self):
        
        if self.metadata is None:
            return {}
        
        metadata_dict = {}
        for m in self.metadata:
            metadata_dict[m.name] = m.value
        return metadata_dict

    def get_hash(self):
        
        metadata = self.get_metadata_as_dict()
        
        val = self.parse_value()

        hash_string = "%s %s %s %s %s, %s"%(self.name,
                                       self.unit,
                                       self.dimension,
                                       self.type,
                                       str(val),
                                       metadata)
        data_hash  = hash(hash_string)

        self.data_hash = data_hash
        return data_hash

class Descriptor(HydraComplexModel):
    _type_info = [
        ('desc_val', Unicode),
    ]

    def __init__(self, val=None):
        super(Descriptor, self).__init__()
        if  val is None:
            return
        self.desc_val = [val]

class TimeSeriesData(HydraComplexModel):
    _type_info = [
        #('ts_time', DateTime),
        ('ts_time', Unicode),
        ('ts_value', AnyDict),
    ]

    def __init__(self, val=None):
        super(TimeSeriesData, self).__init__()
        if  val is None:
            return

        self.ts_time  = [get_timestamp(Dec(val.ts_time))]
        
        try:
            ts_val = eval(val.ts_value)
        except:
            ts_val = val.ts_value

        if type(ts_val) is list:
            self.ts_value = [create_dict(ts_val)]
        else:
            self.ts_value = [ts_val]

class TimeSeries(HydraComplexModel):
    _type_info = [
        ('ts_values', SpyneArray(TimeSeriesData)),
    ]

    def __init__(self, val=None):
        super(TimeSeries, self).__init__()
        if  val is None:
            return
        ts_vals = []
        for ts in val:
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

    def __init__(self, start_time=None, frequency=None, val=None):
        super(EqTimeSeries, self).__init__()
        if  val is None:
            return

        self.start_time = [ordinal_to_timestamp(Dec(start_time))]
        self.frequency  = [Dec(frequency)]
        self.arr_data   = [create_dict(eval(val))]

class Scalar(HydraComplexModel):
    _type_info = [
        ('param_value', Float),
    ]

    def __init__(self, val=None):
        super(Scalar, self).__init__()
        if  val is None:
            return
        self.param_value = [val]

class Array(HydraComplexModel):
    _type_info = [
        ('arr_data', AnyDict),
    ]

    def __init__(self, val=None):
        super(Array, self).__init__()
        if  val is None:
            return
        try:
           val = eval(val)
        except:
            pass
        if type(val) is list:
            self.arr_data = [create_dict(val)]
        else:
            self.arr_data = [val]

class DatasetGroupItem(HydraComplexModel):
    _type_info = [
        ('group_id', Integer(default=None)),
        ('dataset_id', Integer(default=None)),
    ]

    def __init__(self, parent=None):
        super(DatasetGroupItem, self).__init__()
        if  parent is None:
            return

        self.group_id = parent.group_id
        self.dataset_id = parent.dataset_id

class DatasetGroup(HydraComplexModel):
    _type_info = [
        ('group_name', Unicode(default=None)),
        ('group_id'  , Integer(default=None)),
        ('datasetgroupitems', SpyneArray(DatasetGroupItem)),
    ]

    def __init__(self, parent=None):
        super(DatasetGroup, self).__init__()
        if  parent is None:
            return
        self.group_name = parent.group_name
        self.group_id   = parent.group_id
        self.datasetgroupitems = [DatasetGroupItem(d) for d in parent.items]

class Attr(HydraComplexModel):
    _type_info = [
        ('id', Integer(default=None)),
        ('name', Unicode(default=None)),
        ('dimen', Unicode(default=None)),
    ]

    def __init__(self, parent=None):
        super(Attr, self).__init__()
        if  parent is None:
            return
        self.id = parent.attr_id
        self.name = parent.attr_name
        self.dimen = parent.attr_dimen

class ResourceAttr(HydraComplexModel):
    _type_info = [
        ('id',      Integer(default=None)),
        ('attr_id', Integer(default=None)),
        ('ref_id',  Integer(default=None)),
        ('ref_key', Unicode(default=None)),
        ('attr_is_var',  Unicode(min_occurs=1, default='N')),
    ]

    def __init__(self, parent=None):
        super(ResourceAttr, self).__init__()
        if  parent is None:
            return
        self.id = parent.resource_attr_id
        self.attr_id = parent.attr_id
        self.ref_key  = parent.ref_key
        if parent.ref_key == 'NETWORK':
            self.ref_id = parent.network_id
        elif parent.ref_key  == 'NODE':
            self.ref_id = parent.node_id
        elif parent.ref_key == 'LINK':
            self.ref_id = parent.link_id
        elif parent.ref_key == 'GROUP':
            parent.ref_id = parent.group_id

        self.attr_is_var = parent.attr_is_var


class ResourceTypeDef(HydraComplexModel):
    _type_info = [
        ('ref_key', Unicode(default=None)),
        ('ref_id',  Integer(default=None)),
        ('type_id', Integer(default=None)),
    ]

class TypeAttr(HydraComplexModel):
    _type_info = [
        ('attr_id',   Integer(min_occurs=1, max_occurs=1)),
        ('attr_name', Unicode(default=None)),
        ('type_id',   Integer(default=None)),
        ('data_type', Unicode(default=None)),
        ('dimension', Unicode(default=None)),
        ('default_dataset_id', Integer(default=None)),
        ('is_var',    Unicode(default=None)),
    ]

    def __init__(self, parent=None):
        super(TypeAttr, self).__init__()
        if  parent is None:
            return
        for k, v in parent.__dict__.items():
            setattr(self, k, v)

class TemplateType(HydraComplexModel):
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('resource_type', Unicode(values=['GROUP', 'NODE', 'LINK', 'NETWORK'], default=None)),
        ('alias',       Unicode(default=None)),
        ('layout',      AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('template_id', Integer(min_occurs=1, default=None)),
        ('typeattrs',   SpyneArray(TypeAttr, default=[])),
    ]

    def __init__(self, parent=None):
        super(TemplateType, self).__init__()
        if parent is None:
            return

        self.id        = parent.type_id
        self.name      = parent.type_name
        self.alias     = parent.alias
        if parent.layout is not None:
            self.layout    = eval(parent.layout)
        else:
            self.layout = {}
        self.template_id  = parent.template_id

        typeattrs = []
        for typeattr in parent.typeattrs:
            typeattrs.append(TypeAttr(typeattr))

        self.typeattrs = typeattrs

class Template(HydraComplexModel):
    _type_info = [
        ('id',        Integer(default=None)),
        ('name',      Unicode(default=None)),
        ('layout',    AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('types',     SpyneArray(TemplateType, default=[])),
    ]

    def __init__(self, parent=None):
        super(Template, self).__init__()
        if parent is None:
            return

        self.name   = parent.template_name
        self.id     = parent.template_id
        if parent.layout is not None:
            self.layout    = eval(parent.layout)
        else:
            self.layout = {}

        types = []
        for templatetype in parent.templatetypes:
            types.append(TemplateType(templatetype))
        self.types = types

class TypeSummary(HydraComplexModel):
    _type_info = [
        ('name',    Unicode),
        ('id',      Integer),
        ('template_name', Unicode),
        ('template_id', Integer),
    ]

    def __init__(self, parent=None):
        super(TypeSummary, self).__init__()

        if parent is None:
            return

        self.name          = parent.type_name
        self.id            = parent.type_id
        self.template_name = parent.template.template_name
        self.template_id   = parent.template_id

class Resource(HydraComplexModel):
    pass

class Node(Resource):
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('description', Unicode(min_occurs=1, default="")),
        ('layout',      AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('x',           Decimal(min_occurs=1, default=0)),
        ('y',           Decimal(min_occurs=1, default=0)),
        ('status',      Unicode(default='A', pattern="[AX]")),
        ('attributes',  SpyneArray(ResourceAttr, default=[])),
        ('types',       SpyneArray(TypeSummary, default=[])),
    ]

    def __init__(self, parent=None, summary=False):
        super(Node, self).__init__()

        if parent is None:
            return


        self.id = parent.node_id
        self.name = parent.node_name
        self.x = parent.node_x
        self.y = parent.node_y
        if summary is False:
            self.descriptiron = parent.node_description
            if parent.node_layout is not None:
                self.layout    = eval(parent.node_layout)
            else:
                self.layout = {}
            self.status = parent.status
            self.attributes = [ResourceAttr(a) for a in parent.attributes]
            self.types = [TypeSummary(t.templatetype) for t in parent.types]



class Link(Resource):
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('description', Unicode(min_occurs=1, default="")),
        ('layout',      AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('node_1_id',   Integer(default=None)),
        ('node_2_id',   Integer(default=None)),
        ('status',      Unicode(default='A', pattern="[AX]")),
        ('attributes',  SpyneArray(ResourceAttr, default=[])),
        ('types',       SpyneArray(TypeSummary, default=[])),
    ]

    def __init__(self, parent=None, summary=False):
        super(Link, self).__init__()

        if parent is None:
            return

        
        self.id = parent.link_id
        self.name = parent.link_name
        self.node_1_id = parent.node_1_id
        self.node_2_id = parent.node_2_id
        if summary is False:
            self.descriptiron = parent.link_description
            if parent.link_layout is not None:
                self.layout    = eval(parent.link_layout)
            else:
                self.layout = {}
            self.status    = parent.status

            self.attributes = [ResourceAttr(a) for a in parent.attributes]
            self.types = [TypeSummary(t.templatetype) for t in parent.types]

class ResourceScenario(Resource):
    _type_info = [
        ('resource_attr_id', Integer(default=None)),
        ('attr_id',          Integer(default=None)),
        ('value',            Dataset),
    ]

    def __init__(self, parent=None):
        super(ResourceScenario, self).__init__()
        if parent is None:
            return
        self.resource_attr_id = parent.resource_attr_id
        self.attr_id          = parent.resourceattr.attr_id

        self.value = Dataset(parent.dataset)

class ResourceGroupItem(HydraComplexModel):
    _type_info = [
        ('id',       Integer(default=None)),
        ('ref_id',   Integer(default=None)),
        ('ref_key',  Unicode(default=None)),
        ('group_id', Integer(default=None)),
    ]

    def __init__(self, parent=None):
        super(ResourceGroupItem, self).__init__(parent)
        if parent is None:
            return
        self.id = parent.item_id
        self.group_id = parent.group_id
        self.ref_key = parent.ref_key
        if self.ref_key == 'NODE':
            self.ref_id = parent.node_id
        elif self.ref_key == 'LINK':
            self.ref_id = parent.link_id
        elif self.ref_key == 'GROUP':
            self.ref_id = parent.subgroup_id

class ResourceGroup(HydraComplexModel):
    _type_info = [
        ('id',          Integer(default=None)),
        ('network_id',  Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('description', Unicode(min_occurs=1, default="")),
        ('status',      Unicode(default='A', pattern="[AX]")),
        ('attributes',  SpyneArray(ResourceAttr, default=[])),
        ('types',       SpyneArray(TypeSummary, default=[])),
    ]

    def __init__(self, parent=None, summary=False):
        super(ResourceGroup, self).__init__(parent)

        if parent is None:
            return

        self.name        = parent.group_name
        self.id          = parent.group_id

        if summary is False:
            self.description = parent.group_description
            self.status      = parent.status
            self.network_id  = parent.network_id
            self.attributes  = [ResourceAttr(a) for a in parent.attributes]
            self.types       = [TypeSummary(t.templatetype) for t in self.types]

class Scenario(Resource):
    _type_info = [
        ('id',                   Integer(default=None)),
        ('name',                 Unicode(default=None)),
        ('description',          Unicode(min_occurs=1, default="")),
        ('network_id',           Integer(default=None)),
        ('status',               Unicode(default='A', pattern="[AX]")),
        ('locked',               Unicode(default='N', pattern="[YN]")),
        ('start_time',           Unicode(default=None)),
        ('end_time',             Unicode(default=None)),
        ('time_step',            Unicode(default=None)),
        ('resourcescenarios',    SpyneArray(ResourceScenario, default=[])),
        ('resourcegroupitems',   SpyneArray(ResourceGroupItem, default=[])),
    ]

    def __init__(self, parent=None):
        super(Scenario, self).__init__(parent)

        if parent is None:
            return
        self.id = parent.scenario_id
        self.name = parent.scenario_name
        self.description = parent.scenario_description
        self.network_id = parent.network_id
        self.status = parent.status
        self.locked = parent.locked
        self.start_time = get_timestamp(parent.start_time)
        self.end_time = get_timestamp(parent.end_time)
        self.time_step = parent.time_step

        self.resourcescenarios = [ResourceScenario(rs) for rs in parent.resourcescenarios]
        self.resourcegroupitems = [ResourceGroupItem(rgi) for rgi in parent.resourcegroupitems]

class ResourceGroupDiff(HydraComplexModel):
    _type_info = [
       ('scenario_1_items', SpyneArray(ResourceGroupItem, default=[])),
       ('scenario_2_items', SpyneArray(ResourceGroupItem, default=[]))
    ]

    def __init__(self, parent=None):
        super(ResourceGroupDiff, self).__init__()

        if parent is None:
            return

        self.scenario_1_items = [ResourceGroupItem(rs) for rs in parent['scenario_1_items']]
        self.scenario_2_items = [ResourceGroupItem(rs) for rs in parent['scenario_2_items']]

class ResourceScenarioDiff(HydraComplexModel):
    _type_info = [
        ('resource_attr_id',     Integer(default=None)),
        ('scenario_1_dataset',   Dataset),
        ('scenario_2_dataset',   Dataset),
    ]

    def __init__(self, parent=None):
        super(ResourceScenarioDiff, self).__init__()

        if parent is None:
            return

        self.resource_attr_id   = parent['resource_attr_id']

        self.scenario_1_dataset = Dataset(parent['scenario_1_dataset'])
        self.scenario_2_dataset = Dataset(parent['scenario_2_dataset'])

class ScenarioDiff(HydraComplexModel):
    _type_info = [
        ('resourcescenarios',    SpyneArray(ResourceScenarioDiff, default=[])),
        ('groups',               ResourceGroupDiff),
    ]

    def __init__(self, parent=None):
        super(ScenarioDiff, self).__init__()

        if parent is None:
            return
        
        self.resourcescenarios = [ResourceScenarioDiff(rd) for rd in parent['resourcescenarios']]
        self.groups = ResourceGroupDiff(parent['groups'])

class Network(Resource):
    _type_info = [
        ('project_id',          Integer(default=None)),
        ('id',                  Integer(default=None)),
        ('name',                Unicode(default=None)),
        ('description',         Unicode(min_occurs=1, default=None)),
        ('created_by',          Integer(default=None)),
        ('cr_date',             Unicode(default=None)),
        ('layout',              AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('status',              Unicode(default='A', pattern="[AX]")),
        ('attributes',          SpyneArray(ResourceAttr, default=[])),
        ('scenarios',           SpyneArray(Scenario, default=[])),
        ('nodes',               SpyneArray(Node, default=[])),
        ('links',               SpyneArray(Link, default=[])),
        ('resourcegroups',      SpyneArray(ResourceGroup, default=[])),
        ('types',               SpyneArray(TypeSummary, default=[])),
    ]

    def __init__(self, parent=None):
        super(Network, self).__init__(parent)

        if parent is None:
            return
        self.project_id = parent.project_id
        self.id         = parent.network_id
        self.name       = parent.network_name
        self.description = parent.network_description
        self.created_by  = parent.created_by
        self.cr_date     = str(parent.cr_date) 
        if parent.network_layout is not None:
            self.layout    = eval(parent.network_layout)
        else:
            self.layout = {}
        self.status      = parent.status
        self.attributes  = [ResourceAttr(ra) for ra in parent.attributes]
        self.scenarios   = [Scenario(s) for s in parent.scenarios]
        self.nodes       = [Node(n) for n in parent.nodes]
        self.links       = [Link(l) for l in parent.links]
        self.resourcegroups = [ResourceGroup(rg) for rg in parent.resourcegroups]
        self.types          = [TypeSummary(t.templatetype) for t in parent.types]

class NetworkSummary(ComplexModel):
    _type_info = [
        ('project_id',          Integer(default=None)),
        ('id',                  Integer(default=None)),
        ('name',                Unicode(default=None)),
        ('description',         Unicode(default=None)),
        ('scenario_ids',        SpyneArray(Integer, default=None)),
        ('links',               SpyneArray(Link, default=None)),
        ('nodes',               SpyneArray(Node, default=None)),
        ('resourcegroups',      SpyneArray(ResourceGroup, default=None)),
    ]

    def __init__(self, parent=None):
        """
            Takes a network object and creates a very simplified summary from it.
        """
        super(NetworkSummary, self).__init__()

        if parent is None:
            return

        self.project_id = parent.project_id
        self.id         = parent.network_id
        self.name       = parent.network_name
        self.description = parent.network_description
        self.scenario_ids = []
        for s in parent.scenarios:
            self.scenario_ids.append(s.scenario_id)
        self.links = [Link(l, True) for l in parent.links]
        self.nodes = [Node(n, True) for n in parent.nodes]
        self.resourcegroups = [ResourceGroup(g, True) for g in parent.resourcegroups]

class NetworkExtents(Resource):
    _type_info = [
        ('network_id', Integer(default=None)),
        ('min_x',      Decimal(default=0)),
        ('min_y',      Decimal(default=0)),
        ('max_x',      Decimal(default=0)),
        ('max_y',      Decimal(default=0)),
    ]

    def __init__(self, parent=None):
        super(NetworkExtents, self).__init__(parent)

        if parent is None:
            return

        self.network_id = parent.network_id
        self.min_x = parent.min_x
        self.min_y = parent.min_y
        self.max_x = parent.max_x
        self.max_y = parent.max_y

class Project(Resource):
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('description', Unicode(default=None)),
        ('status',      Unicode(default='A', pattern="[AX]")),
        ('cr_date',     Unicode(default=None)),
        ('created_by',  Integer(default=None)),
        ('attributes',  SpyneArray(ResourceAttr, default=[])),
        ('attribute_data', SpyneArray(ResourceScenario, default=[])),
    ]

    def __init__(self, parent=None):
        super(Project, self).__init__(parent)

        if parent is None:
            return

        self.id = parent.project_id
        self.name = parent.project_name
        self.description = parent.project_description
        self.status      = parent.status
        self.cr_date     = str(parent.cr_date)
        self.created_by  = parent.created_by
        self.attributes  = [ResourceAttr(ra) for ra in parent.attributes]
        self.attribute_data  = [ResourceScenario(rs) for rs in parent.attribute_data]

class ProjectSummary(Resource):
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('description', Unicode(default=None)),
        ('cr_date',     Unicode(default=None)),
        ('created_by',  Integer(default=None)),
    ]

    def __init__(self, parent=None):
        super(ProjectSummary, self).__init__(parent)

        if parent is None:
            return
        self.id = parent.project_id
        self.name = parent.project_name
        self.description = parent.project_description
        self.cr_date = str(parent.cr_date)
        self.created_by = parent.created_by

class User(HydraComplexModel):
    _type_info = [
        ('id',  Integer),
        ('username', Unicode(default=None)),
        ('display_name', Unicode(default=None)),
        ('password', Unicode(default=None)),
    ]

    def __init__(self, parent=None):
        super(User, self).__init__(parent)

        if parent is None:
            return

        self.id = parent.user_id
        self.username = parent.username
        self.display_name = parent.display_name
        self.password     = parent.password

class Perm(HydraComplexModel):
    _type_info = [
        ('id',   Integer),
        ('name', Unicode),
        ('code', Unicode),
    ]

    def __init__(self, parent=None):
        super(Perm, self).__init__(parent)

        if parent is None:
            return

        self.id   = parent.perm_id
        self.name = parent.perm_name
        self.code = parent.perm_code

class RoleUser(HydraComplexModel):
    _type_info = [
        ('user_id',  Integer),
    ]
    def __init__(self, parent=None):
        super(RoleUser, self).__init__(parent)

        if parent is None:
            return

        self.user_id = parent.user.user_id

class RolePerm(HydraComplexModel):
    _type_info = [
        ('perm_id',   Integer),
    ]

    def __init__(self, parent=None):
        super(RolePerm, self).__init__(parent)

        if parent is None:
            return

        self.perm_id = parent.perm_id

class Role(HydraComplexModel):
    _type_info = [
        ('id',     Integer),
        ('name',   Unicode),
        ('code',   Unicode),
        ('roleperms', SpyneArray(RolePerm, default=[])),
        ('roleusers', SpyneArray(RoleUser, default=[])),
    ]

    def __init__(self, parent=None):
        super(Role, self).__init__(parent)

        if parent is None:
            return

        self.id = parent.role_id
        self.name = parent.role_name
        self.code = parent.role_code
        self.roleperms = [RolePerm(rp) for rp in parent.roleperms]
        self.roleusers = [RoleUser(ru) for ru in parent.roleusers]

class PluginParam(HydraComplexModel):
    _type_info = [
        ('name',        Unicode),
        ('value',       Unicode),
    ]

    def __init__(self, parent=None):
        super(PluginParam, self).__init__(parent)

        if parent is None:
            return

        self.name = parent.name
        self.value = parent.value


class Plugin(HydraComplexModel):
    _type_info = [
        ('name',        Unicode),
        ('location',    Unicode),
        ('params',      SpyneArray(PluginParam, default=[])),
    ]

    def __init__(self, parent=None):
        super(Plugin, self).__init__(parent)

        if parent is None:
            return

        self.name = parent.name
        self.location = parent.location
        self.params = [PluginParam(pp) for pp in parent.params]


class ProjectOwner(HydraComplexModel):
    _type_info = [
        ('project_id',   Integer),
        ('user_id',  Integer),
        ('edit',     Unicode),
        ('view',     Unicode)
    ]
    def __init__(self, parent=None):
        super(ProjectOwner, self).__init__(parent)

        if parent is None:
            return
        self.project_id = parent.project_id
        self.user_id    = parent.user_id
        self.edit       = parent.edit
        self.view       = parent.view

class DatasetOwner(HydraComplexModel):
    _type_info = [
        ('dataset_id',   Integer),
        ('user_id',  Integer),
        ('edit',     Unicode),
        ('view',     Unicode)
    ]
    def __init__(self, parent=None):
        super(DatasetOwner, self).__init__(parent)

        if parent is None:
            return
        self.dataset_id = parent.dataset_id
        self.user_id    = parent.user_id
        self.edit       = parent.edit
        self.view       = parent.view

class NetworkOwner(HydraComplexModel):
    _type_info = [
        ('network_id',   Integer),
        ('user_id',  Integer),
        ('edit',     Unicode),
        ('view',     Unicode)
    ]
    def __init__(self, parent=None):
        super(NetworkOwner, self).__init__(parent)

        if parent is None:
            return
        self.network_id = parent.network_id
        self.user_id    = parent.user_id
        self.edit       = parent.edit
        self.view       = parent.view

class Unit(HydraComplexModel):
    _type_info = [
        ('name', Unicode),
        ('abbr', Unicode),
        ('cf', Double),
        ('lf', Double),
        ('info', Unicode),
        ('dimension', Unicode),
    ]

    def __init__(self, parent=None):
        super(Unit, self).__init__(parent)

        if parent is None:
            return
        self.name = parent.name
        self.abbr = parent.abbr
        self.cf   = parent.cf
        self.lf   = parent.lf
        self.info = parent.info
        self.dimension = parent.dimension
