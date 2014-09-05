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
from HydraLib.util import get_datetime,\
        timestamp_to_ordinal,\
        ordinal_to_timestamp,\
        check_array_struct
import pandas as pd
from pandas.tseries.index import DatetimeIndex
import logging
from HydraLib.util import create_dict

NS = "soap_server.hydra_complexmodels"
log = logging.getLogger(__name__)

def get_timestamp(ordinal):
    if ordinal is None:
        return None

    if type(ordinal) in (str, unicode):
        ordinal = Dec(ordinal)
    timestamp = str(ordinal_to_timestamp(ordinal))
    return timestamp


ref_id_map = {
    'NODE'     : 'node_id',
    'LINK'     : 'link_id',
    'GROUP'    : 'group_id',
    'NETWORK'  : 'network_id',
    'SCENARIO' : 'scenario_id',
    'PROJECT'  : 'project_id'
}

class LoginResponse(ComplexModel):
    """
    """
    __namespace__ = 'soap_server.hydra_complexmodels'
    _type_info = [
        ('session_id', Unicode(min_occurs=1)),
        ('user_id',    Integer(min_occurs=1)),
    ]

class Metadata(ComplexModel):
    """
    """
    _type_info = [
        ('name'  , Unicode(min_occurs=1, default=None)),
        ('value' , Unicode(min_occurs=1, default=None)),
    ]

    def __init__(self, parent=None):
        if parent is None:
            return
        self.name    = parent.metadata_name
        self.value   = parent.metadata_val

class ResourceData(ComplexModel):
    """
        An object which represents a resource attr, resource scenario and dataset
        all in one. 

        
        * **attr_id:** The ID of the attribute to which this data belongs
        * **scenario_id:** The ID of the scenario in which this data has been assigned
        * **resource_attr_id:** The unique ID representing the attribute and resource combination
        * **ref_key:** Indentifies the type of resource to which this dataset is attached. Can be 'NODE', 'LINK', 'GROUP', 'NETWORK' or 'PROJECT'
        * **ref_id:** The ID of the node, link, group, network, or project in question
        * **attr_is_var:** Flag to indicate whether this resource's attribute is a variable and hence should be filled in by a model
        * **dataset_id:** The ID of the dataset which has been assigned to the resource attribute
        * **dataset_type:** The type of the dataset -- can be scalar, descriptor, array or timeseries
        * **dataset_dimension:** The dimension of the dataset (This MUST match the dimension of the attribute)
        * **dataset_unit:** The unit of the dataset.
        * **dataset_name:** The name of the dataset. Most likely used for distinguishing similar datasets or searching for datasets
        * **dataset_frequency:** The frequency of the timesteps in a timeseries. Only applicable if the dataset has a type 'timeseries'
        * **dataset_locked:** Indicates whether the dataset is locked, in which case only authorised users can use the dataset.
        * **dataset_metadata:**: A dictionary of the metadata associated with the dataset. For example: {'created_by': "User 1", "source":"Import from CSV"}
        * **dataset_value:**
            Depending on what the dataset_type is, this can be a decimal value, a freeform
            string or a JSON string.
            For a timeseries for example, the datasset_value looks like:
            '{
                "H1" : {\n
                        "2014/09/04 16:46:12:00":1,\n
                        "2014/09/05 16:46:12:00":2,\n
                        "2014/09/06 16:46:12:00":3,\n
                        "2014/09/07 16:46:12:00":4},\n
    
                "H2" : {\n
                        "2014/09/04 16:46:12:00":10,\n
                        "2014/09/05 16:46:12:00":20,\n
                        "2014/09/06 16:46:12:00":30,\n
                        "2014/09/07 16:46:12:00":40},\n
    
                "H3" :  {\n
                        "2014/09/04 16:46:12:00":100,\n
                        "2014/09/05 16:46:12:00":200,\n
                        "2014/09/06 16:46:12:00":300,\n
                        "2014/09/07 16:46:12:00":400}\n
            }'

    """
    _type_info = [
        ('attr_id',            Unicode(default=None)),
        ('scenario_id',        Unicode(default=None)),
        ('resource_attr_id',   Unicode(default=None)),
        ('ref_id',             Unicode(default=None)),
        ('ref_key',            Unicode(default=None)),
        ('attr_is_var',        Unicode(default=None)),
        ('dataset_id',         Unicode(default=None)),
        ('dataset_type',       Unicode(default=None)),
        ('dataset_dimension',  Unicode(default=None)),
        ('dataset_unit',       Unicode(default=None)),
        ('dataset_name',       Unicode(default=None)),
        ('dataset_value',      Unicode(default=None)),
        ('dataset_frequency',  Unicode(default=None)),
        ('dataset_locked',     Unicode(default=None)),
        ('dataset_metadata',   AnyDict(default=None)),
    ]

    def __init__(self, resourceattr=None):

        super(ResourceData, self).__init__()
        if  resourceattr is None:
            return
       
        ra = resourceattr

        self.attr_id = ra.attr_id
        self.resource_attr_id = ra.resource_attr_id
        self.ref_key = ra.ref_key
        self.ref_id  = getattr(ra, ref_id_map[self.ref_key])

        self.resource_attr_id = ra.resource_attr_id
        self.source = ra.source
        self.scenario_id = ra.scenario_id

        self.dataset_locked    = ra.locked
        self.dataset_id        = ra.dataset_id
        self.dataset_type      = ra.data_type
        self.dataset_name      = ra.data_name

        self.dataset_dimension = ra.data_dimen
        self.dataset_unit      = ra.data_units
        self.dataset_frequency = ra.frequency
        self.dataset_value     = ra.value

        if hasattr(ra, 'metadata'):
            self.metadata = {}
            for m in ra.metadata:
                self.metadata[m.metadata_name] = m.metadata_val

class Dataset(ComplexModel):
    """
    """
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
                self.value = {'desc_val': [parent.value]}
            elif parent.data_type == 'array':
                self.value = Array(parent.value)
            elif parent.data_type == 'scalar':
                self.value = {'param_value': [parent.value]}
            elif parent.data_type == 'eqtimeseries':
                self.value = EqTimeSeries(parent.start_time, parent.frequency, parent.value)
        if parent.data_type == 'timeseries':
            if parent.value is not None:
                self.value = TimeSeries(parent.value)
                #self.value = _make_timeseries(parent.value)
            else:
                if len(parent.timeseriesdata) > 0:
                    self.value = TimeSeries(val = parent.timeseriesdata)

        if self.value and type(self.value) is not dict:
            self.value = self.value.__dict__
        elif self.value is None:
            self.value = {}
        
        metadata = []
        for m in parent.metadata:
            complex_m = Metadata(m)
            metadata.append(complex_m)
        self.metadata = metadata

    def parse_value(self):
        """
            Turn the value of an incoming dataset into a hydra-friendly value.
        """

        is_soap_req = False
        #attr_data.value is a dictionary,
        #but the keys have namespaces which must be stripped.
        data = str(self.value)
        if data.find('{%s}'%NS) >= 0:
            data = data.replace('{%s}'%NS, '')
            is_soap_req = True
        data = eval(data)
        
        if data is None:
            log.warn("Cannot parse dataset. No value specified.")
            return None
       
        data_type = self.type

        val_names = data.keys()
        value = []
        for name in val_names:
            value.append(data[name])

        if data_type == 'descriptor':
            if is_soap_req:
                return data['desc_val'][0]
            else:
                return data['desc_val']
        elif data_type == 'timeseries':

            # The brand new way to parse time series data:
            ts = []
            for ts_val in data['ts_values']:
                #The value is a list, so must get index 0
                timestamp = ts_val['ts_time']
                if is_soap_req:
                    timestamp = timestamp[0]
                # Check if we have received a seasonal time series first
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

                ts.append((timestamp, str(ts_value)))

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
            if is_soap_req:
                return data['param_value'][0]
            else:
                return data['param_value']
        elif data_type == 'array':
            arr_data = data['arr_data']
            if is_soap_req:
                arr_data = arr_data[0]
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
        arr_data = arr.get('array', None)
        if arr_data is not None:
            if len(arr_data) == 1:
                if arr_data[0].get('item'):
                    for v in arr_data[0].get('item'):
                        try:
                            ret_arr.append(eval(v))
                        except:
                            ret_arr.append(v)
                else:
                    ret_arr = self.parse_array(arr_data[0])
            else:
                for sub_val in arr_data:
                    if sub_val.get('array'):
                        ret_arr.append(self.parse_array(sub_val))
                    elif sub_val.get('item'):
                        item_arr = []
                        for v in sub_val.get('item'):
                            try:
                                item_arr.append(float(v))
                            except:
                                item_arr.append(v)
                        ret_arr.append(item_arr)
        check_array_struct(ret_arr)
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

class Descriptor(ComplexModel):
    _type_info = [
        ('desc_val', Unicode),
    ]

    def __init__(self, val=None):
        super(Descriptor, self).__init__()
        if  val is None:
            return
        self.desc_val = [val]

class TimeSeriesData(ComplexModel):
    _type_info = [
        #('ts_time', DateTime),
        ('ts_time', Unicode),
        ('ts_value', AnyDict),
    ]

    def __init__(self, val=None):
        super(TimeSeriesData, self).__init__()
        if  val is None:
            return

        self.ts_time  = [val.ts_time]
        
        try:
            ts_val = eval(val.ts_value)
        except:
            ts_val = val.ts_value

        if type(ts_val) is list:
            self.ts_value = [create_dict(ts_val)]
        else:
            self.ts_value = [ts_val]

def _make_timeseries(val):
    log.debug("Creating timeseries complexmodels")

    if  val is None or len(val) == 0:
        return {}

    ts_vals = []
    timeseries = pd.read_json(val)
    for t in timeseries.index:
        ts_val = timeseries.loc[t].values
        ts_data = {}
        try:
            ts_data['ts_time'] = [str(get_datetime(t.to_pydatetime()))]
        except AttributeError:
            try:
                ts_data['ts_time'] = [eval(t)]
            except:
                ts_data['ts_time'] = [t]
        try:
            ts_val = list(ts_val)
            ts_data['ts_value'] = [create_dict(ts_val)]
        except:
            ts_data['ts_value'] = [ts_val]
        ts_vals.append(ts_data)
    freq = None
    if type(timeseries.index) == DatetimeIndex:
        freq = [timeseries.index.inferred_freq]
        ts = {'periods'   : [len(timeseries.index)],
              'frequency' : freq,
              'ts_values' : ts_vals,
         }

    return ts


class TimeSeries(ComplexModel):
    _type_info = [
        ('ts_values', SpyneArray(TimeSeriesData)),
        ('frequency', Unicode(default=None)),
        ('periods',   Integer(default=None)),
    ]

    def __init__(self, val=None):
        super(TimeSeries, self).__init__()
        if  val is None or len(val) == 0:
            return

        ts_vals = []
        if type(val) == str:
            log.debug("Creating timeseries complexmodels")
            timeseries = pd.read_json(val)
            for ts in timeseries.index:
                ts_val = timeseries.loc[ts].values
                ts_data = {}
                try:
                    ts_data['ts_time'] = [str(get_datetime(ts.to_pydatetime()))]
                except AttributeError:
                    try:
                        ts_data['ts_time'] = [eval(ts)]
                    except:
                        ts_data['ts_time'] = [ts]
                try:
                    ts_val = list(ts_val)
                    ts_data['ts_value'] = [create_dict(ts_val)]
                except:
                    ts_data['ts_value'] = [ts_val]
                ts_vals.append(ts_data)

            if type(timeseries.index) == DatetimeIndex:
                self.frequency = [timeseries.index.inferred_freq]
            self.periods = [len(timeseries.index)]
            log.debug("Timeseries complexmodels created")
        else:
            log.info("IN HERE!")
            for ts in val:
                ts_vals.append(TimeSeriesData(ts).__dict__)
        self.ts_values = ts_vals

class EqTimeSeries(ComplexModel):

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

class Scalar(ComplexModel):
    _type_info = [
        ('param_value', Float),
    ]

    def __init__(self, val=None):
        super(Scalar, self).__init__()
        if  val is None:
            return
        self.param_value = [val]

class Array(ComplexModel):
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

class DatasetGroup(ComplexModel):
    _type_info = [
        ('group_name', Unicode(default=None)),
        ('group_id'  , Integer(default=None)),
        ('dataset_ids',   SpyneArray(Integer)),
    ]

    def __init__(self, parent=None):
        super(DatasetGroup, self).__init__()
        if  parent is None:
            return
        self.group_name = parent.group_name
        self.group_id   = parent.group_id
        self.dataset_ids = [d.dataset_id for d in parent.items]

class Attr(ComplexModel):
    """
    """
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

class ResourceScenario(ComplexModel):
    """
    """
    _type_info = [
        ('resource_attr_id', Integer(default=None)),
        ('attr_id',          Integer(default=None)),
        ('value',            Dataset),
        ('source',           Unicode),
    ]

    def __init__(self, parent=None, attr_id=None):
        super(ResourceScenario, self).__init__()
        if parent is None:
            return
        self.resource_attr_id = parent.resource_attr_id
        self.attr_id          = attr_id if attr_id is not None else parent.resourceattr.attr_id

        self.value = Dataset(parent.dataset)
        self.source = parent.source

class ResourceAttr(ComplexModel):
    """
    """
    _type_info = [
        ('id',      Integer(default=None)),
        ('attr_id', Integer(default=None)),
        ('ref_id',  Integer(default=None)),
        ('ref_key', Unicode(default=None)),
        ('attr_is_var', Unicode(min_occurs=1, default='N')),
        ('resourcescenario', ResourceScenario),
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
        #This should be set externally as it is not related to its parent.
        self.resourcescenario = None


class ResourceTypeDef(ComplexModel):
    """
    """
    _type_info = [
        ('ref_key', Unicode(default=None)),
        ('ref_id',  Integer(default=None)),
        ('type_id', Integer(default=None)),
    ]

class TypeAttr(ComplexModel):
    """
    """
    _type_info = [
        ('attr_id',            Integer(min_occurs=1, max_occurs=1)),
        ('attr_name',          Unicode(default=None)),
        ('type_id',            Integer(default=None)),
        ('data_type',          Unicode(default=None)),
        ('dimension',          Unicode(default=None)),
        ('default_dataset_id', Integer(default=None)),
        ('data_restriction',   AnyDict(default=None)),
        ('is_var',             Unicode(default=None)),
    ]

    def __init__(self, parent=None):
        super(TypeAttr, self).__init__()
        if  parent is None:
            return
        
        self.attr_id   = parent.attr_id
        if parent.attr is not None:
            self.attr_name = parent.attr.attr_name
        self.type_id   = parent.type_id
        self.data_type = parent.data_type
        self.dimension = parent.dimension
        self.default_dataset_id = self.default_dataset_id
        if parent.data_restriction is not None:
            self.data_restriction = eval(parent.data_restriction)
            for k, v in self.data_restriction.items():
                self.data_restriction[k] = [v]
        else:
            self.data_restriction = {}
        self.is_var = parent.attr_is_var

class TemplateType(ComplexModel):
    """
    """
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('resource_type', Unicode(values=['GROUP', 'NODE', 'LINK', 'NETWORK'], default=None)),
        ('alias',       Unicode(default=None)),
        ('layout',      AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('template_id', Integer(min_occurs=1, default=None)),
        ('typeattrs',   SpyneArray(TypeAttr)),
    ]

    def __init__(self, parent=None):
        super(TemplateType, self).__init__()
        if parent is None:
            return

        self.id        = parent.type_id
        self.name      = parent.type_name
        self.alias     = parent.alias
        self.resource_type = parent.resource_type
        if parent.layout is not None:
            self.layout    = eval(parent.layout)
        else:
            self.layout = {}
        self.template_id  = parent.template_id

        typeattrs = []
        for typeattr in parent.typeattrs:
            typeattrs.append(TypeAttr(typeattr))

        self.typeattrs = typeattrs

class Template(ComplexModel):
    """
    """
    _type_info = [
        ('id',        Integer(default=None)),
        ('name',      Unicode(default=None)),
        ('layout',    AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('types',     SpyneArray(TemplateType)),
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

class TypeSummary(ComplexModel):
    """
    """
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

class Resource(ComplexModel):
    """
    """

    def get_layout(self):
        if hasattr(self, 'layout') and self.layout is not None:
            return str(self.layout).replace('{%s}'%NS, '')
        else:
            return None


class ResourceSummary(ComplexModel):
    """
    """
    _type_info = [
        ('ref_key', Unicode(default=None)),
        ('id',  Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('description', Unicode(min_occurs=1, default="")),
        ('attributes',  SpyneArray(ResourceAttr)),
        ('types',       SpyneArray(TypeSummary)),
    ]

    def __init__(self, parent=None):
        super(ResourceSummary, self).__init__()

        if parent is None:
            parent
        if hasattr(parent, 'node_id'):
            self.ref_key = 'NODE'
            self.id   = parent.node_id
            self.name = parent.node_name
            self.description = parent.node_description
        elif hasattr(parent, 'link_id'):
            self.ref_key = 'LINK'
            self.id   = parent.link_id
            self.name = parent.link_name
            self.description = parent.link_description
        elif hasattr(parent, 'group_id'):
            self.ref_key = 'GROUP'
            self.id   = parent.group_id
            self.name = parent.group_name
            self.description = parent.group_description

        self.attributes = [ResourceAttr(ra) for ra in parent.attributes]
        self.types = [TypeSummary(t.templatetype) for t in parent.types]

class Node(Resource):
    """
    """
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('description', Unicode(min_occurs=1, default="")),
        ('layout',      AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('x',           Decimal(min_occurs=1, default=0)),
        ('y',           Decimal(min_occurs=1, default=0)),
        ('status',      Unicode(default='A', pattern="[AX]")),
        ('attributes',  SpyneArray(ResourceAttr)),
        ('types',       SpyneArray(TypeSummary)),
    ]

    def __init__(self, parent=None, summary=False):
        super(Node, self).__init__()

        if parent is None:
            return


        self.id = parent.node_id
        self.name = parent.node_name
        self.x = parent.node_x
        self.y = parent.node_y
        self.description = parent.node_description
        if parent.node_layout is not None:
            self.layout    = eval(parent.node_layout)
        else:
            self.layout = {}
        self.status = parent.status
        if summary is False:
            self.attributes = [ResourceAttr(a) for a in parent.attributes]
        self.types = [TypeSummary(t.templatetype) for t in parent.types]



class Link(Resource):
    """
    """
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('description', Unicode(min_occurs=1, default="")),
        ('layout',      AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('node_1_id',   Integer(default=None)),
        ('node_2_id',   Integer(default=None)),
        ('status',      Unicode(default='A', pattern="[AX]")),
        ('attributes',  SpyneArray(ResourceAttr)),
        ('types',       SpyneArray(TypeSummary)),
    ]

    def __init__(self, parent=None, summary=False):
        super(Link, self).__init__()

        if parent is None:
            return

        
        self.id = parent.link_id
        self.name = parent.link_name
        self.node_1_id = parent.node_1_id
        self.node_2_id = parent.node_2_id
        self.description = parent.link_description
        if parent.link_layout is not None:
            self.layout    = eval(parent.link_layout)
        else:
            self.layout = {}
        self.status    = parent.status
        if summary is False:


            self.attributes = [ResourceAttr(a) for a in parent.attributes]
        self.types = [TypeSummary(t.templatetype) for t in parent.types]


class AttributeData(ComplexModel):
    """
        A class which is returned by the server when a request is made
        for the data associated with an attribute.
    """
    _type_info = [
        ('resourceattrs', SpyneArray(ResourceAttr)),
        ('resourcescenarios', SpyneArray(ResourceScenario)),
    ]

class ResourceGroupItem(ComplexModel):
    """
    """
    _type_info = [
        ('id',       Integer(default=None)),
        ('ref_id',   Integer(default=None)),
        ('ref_key',  Unicode(default=None)),
        ('group_id', Integer(default=None)),
    ]

    def __init__(self, parent=None):
        super(ResourceGroupItem, self).__init__()
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

class ResourceGroup(ComplexModel):
    """
    """
    _type_info = [
        ('id',          Integer(default=None)),
        ('network_id',  Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('description', Unicode(min_occurs=1, default="")),
        ('status',      Unicode(default='A', pattern="[AX]")),
        ('attributes',  SpyneArray(ResourceAttr)),
        ('types',       SpyneArray(TypeSummary)),
    ]

    def __init__(self, parent=None, summary=False):
        super(ResourceGroup, self).__init__()

        if parent is None:
            return

        self.name        = parent.group_name
        self.id          = parent.group_id
        self.description = parent.group_description
        self.status      = parent.status
        self.network_id  = parent.network_id

        self.types       = [TypeSummary(t.templatetype) for t in parent.types]

        if summary is False:
            self.attributes  = [ResourceAttr(a) for a in parent.attributes]

class Scenario(Resource):
    """
    """
    _type_info = [
        ('id',                   Integer(default=None)),
        ('name',                 Unicode(default=None)),
        ('description',          Unicode(min_occurs=1, default="")),
        ('network_id',           Integer(default=None)),
        ('layout',               AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('status',               Unicode(default='A', pattern="[AX]")),
        ('locked',               Unicode(default='N', pattern="[YN]")),
        ('start_time',           Unicode(default=None)),
        ('end_time',             Unicode(default=None)),
        ('time_step',            Unicode(default=None)),
        ('resourcescenarios',    SpyneArray(ResourceScenario, default=None)),
        ('resourcegroupitems',   SpyneArray(ResourceGroupItem, default=None)),
    ]

    def __init__(self, parent=None, summary=False):
        super(Scenario, self).__init__()

        if parent is None:
            return
        self.id = parent.scenario_id
        self.name = parent.scenario_name
        self.description = parent.scenario_description

        if parent.scenario_layout is not None:
            self.layout    = eval(parent.scenario_layout)
        else:
            self.layout = {}

        self.network_id = parent.network_id
        self.status = parent.status
        self.locked = parent.locked
        self.start_time = get_timestamp(parent.start_time)
        self.end_time = get_timestamp(parent.end_time)
        self.time_step = parent.time_step
        if summary is False:
            self.resourcescenarios = [ResourceScenario(rs) for rs in parent.resourcescenarios]
            self.resourcegroupitems = [ResourceGroupItem(rgi) for rgi in parent.resourcegroupitems]
        else:
            self.resourcescenarios = []
            self.resourcegroupitems = []

class ResourceGroupDiff(ComplexModel):
    """
    """
    _type_info = [
       ('scenario_1_items', SpyneArray(ResourceGroupItem)),
       ('scenario_2_items', SpyneArray(ResourceGroupItem))
    ]

    def __init__(self, parent=None):
        super(ResourceGroupDiff, self).__init__()

        if parent is None:
            return

        self.scenario_1_items = [ResourceGroupItem(rs) for rs in parent['scenario_1_items']]
        self.scenario_2_items = [ResourceGroupItem(rs) for rs in parent['scenario_2_items']]

class ResourceScenarioDiff(ComplexModel):
    """
    """
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

class ScenarioDiff(ComplexModel):
    """
    """
    _type_info = [
        ('resourcescenarios',    SpyneArray(ResourceScenarioDiff)),
        ('groups',               ResourceGroupDiff),
    ]

    def __init__(self, parent=None):
        super(ScenarioDiff, self).__init__()

        if parent is None:
            return
        
        self.resourcescenarios = [ResourceScenarioDiff(rd) for rd in parent['resourcescenarios']]
        self.groups = ResourceGroupDiff(parent['groups'])

class Network(Resource):
    """
    """
    _type_info = [
        ('project_id',          Integer(default=None)),
        ('id',                  Integer(default=None)),
        ('name',                Unicode(default=None)),
        ('description',         Unicode(min_occurs=1, default=None)),
        ('created_by',          Integer(default=None)),
        ('cr_date',             Unicode(default=None)),
        ('layout',              AnyDict(min_occurs=0, max_occurs=1, default=None)),
        ('status',              Unicode(default='A', pattern="[AX]")),
        ('attributes',          SpyneArray(ResourceAttr)),
        ('scenarios',           SpyneArray(Scenario)),
        ('nodes',               SpyneArray(Node)),
        ('links',               SpyneArray(Link)),
        ('resourcegroups',      SpyneArray(ResourceGroup)),
        ('types',               SpyneArray(TypeSummary)),
    ]

    def __init__(self, parent=None, summary=False):
        super(Network, self).__init__()

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
        self.scenarios   = [Scenario(s, summary) for s in parent.scenarios]
        self.nodes       = [Node(n, summary) for n in parent.nodes]
        self.links       = [Link(l, summary) for l in parent.links]
        self.resourcegroups = [ResourceGroup(rg, summary) for rg in parent.resourcegroups]
        self.types          = [TypeSummary(t.templatetype) for t in parent.types]

        if summary is False:
            self.attributes  = [ResourceAttr(ra) for ra in parent.attributes]

class NetworkExtents(ComplexModel):
    """
    """
    _type_info = [
        ('network_id', Integer(default=None)),
        ('min_x',      Decimal(default=0)),
        ('min_y',      Decimal(default=0)),
        ('max_x',      Decimal(default=0)),
        ('max_y',      Decimal(default=0)),
    ]

    def __init__(self, parent=None):
        super(NetworkExtents, self).__init__()

        if parent is None:
            return

        self.network_id = parent.network_id
        self.min_x = parent.min_x
        self.min_y = parent.min_y
        self.max_x = parent.max_x
        self.max_y = parent.max_y

class Project(Resource):
    """
    """
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('description', Unicode(default=None)),
        ('status',      Unicode(default='A', pattern="[AX]")),
        ('cr_date',     Unicode(default=None)),
        ('created_by',  Integer(default=None)),
        ('attributes',  SpyneArray(ResourceAttr)),
        ('attribute_data', SpyneArray(ResourceScenario)),
    ]

    def __init__(self, parent=None):
        super(Project, self).__init__()

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
    """
    """
    _type_info = [
        ('id',          Integer(default=None)),
        ('name',        Unicode(default=None)),
        ('description', Unicode(default=None)),
        ('cr_date',     Unicode(default=None)),
        ('created_by',  Integer(default=None)),
    ]

    def __init__(self, parent=None):
        super(ProjectSummary, self).__init__()

        if parent is None:
            return
        self.id = parent.project_id
        self.name = parent.project_name
        self.description = parent.project_description
        self.cr_date = str(parent.cr_date)
        self.created_by = parent.created_by

class User(ComplexModel):
    """
    """
    _type_info = [
        ('id',  Integer),
        ('username', Unicode(default=None)),
        ('display_name', Unicode(default=None)),
        ('password', Unicode(default=None)),
    ]

    def __init__(self, parent=None):
        super(User, self).__init__()

        if parent is None:
            return

        self.id = parent.user_id
        self.username = parent.username
        self.display_name = parent.display_name
        self.password     = parent.password

class Perm(ComplexModel):
    """
    """
    _type_info = [
        ('id',   Integer),
        ('name', Unicode),
        ('code', Unicode),
    ]

    def __init__(self, parent=None):
        super(Perm, self).__init__()

        if parent is None:
            return

        self.id   = parent.perm_id
        self.name = parent.perm_name
        self.code = parent.perm_code

class RoleUser(ComplexModel):
    """
    """
    _type_info = [
        ('user_id',  Integer),
    ]
    def __init__(self, parent=None):
        super(RoleUser, self).__init__()

        if parent is None:
            return

        self.user_id = parent.user.user_id

class RolePerm(ComplexModel):
    """
    """
    _type_info = [
        ('perm_id',   Integer),
    ]

    def __init__(self, parent=None):
        super(RolePerm, self).__init__()

        if parent is None:
            return

        self.perm_id = parent.perm_id

class Role(ComplexModel):
    """
    """
    _type_info = [
        ('id',     Integer),
        ('name',   Unicode),
        ('code',   Unicode),
        ('roleperms', SpyneArray(RolePerm)),
        ('roleusers', SpyneArray(RoleUser)),
    ]

    def __init__(self, parent=None):
        super(Role, self).__init__()

        if parent is None:
            return

        self.id = parent.role_id
        self.name = parent.role_name
        self.code = parent.role_code
        self.roleperms = [RolePerm(rp) for rp in parent.roleperms]
        self.roleusers = [RoleUser(ru) for ru in parent.roleusers]

class PluginParam(ComplexModel):
    """
    """
    _type_info = [
        ('name',        Unicode),
        ('value',       Unicode),
    ]

    def __init__(self, parent=None):
        super(PluginParam, self).__init__()

        if parent is None:
            return

        self.name = parent.name
        self.value = parent.value


class Plugin(ComplexModel):
    """
    """
    _type_info = [
        ('name',        Unicode),
        ('location',    Unicode),
        ('params',      SpyneArray(PluginParam)),
    ]

    def __init__(self, parent=None):
        super(Plugin, self).__init__()

        if parent is None:
            return

        self.name = parent.name
        self.location = parent.location
        self.params = [PluginParam(pp) for pp in parent.params]


class ProjectOwner(ComplexModel):
    """
    """
    _type_info = [
        ('project_id',   Integer),
        ('user_id',  Integer),
        ('edit',     Unicode),
        ('view',     Unicode)
    ]
    def __init__(self, parent=None):
        super(ProjectOwner, self).__init__()

        if parent is None:
            return
        self.project_id = parent.project_id
        self.user_id    = parent.user_id
        self.edit       = parent.edit
        self.view       = parent.view

class DatasetOwner(ComplexModel):
    """
    """
    _type_info = [
        ('dataset_id',   Integer),
        ('user_id',  Integer),
        ('edit',     Unicode),
        ('view',     Unicode)
    ]
    def __init__(self, parent=None):
        super(DatasetOwner, self).__init__()

        if parent is None:
            return
        self.dataset_id = parent.dataset_id
        self.user_id    = parent.user_id
        self.edit       = parent.edit
        self.view       = parent.view

class NetworkOwner(ComplexModel):
    """
    """
    _type_info = [
        ('network_id',   Integer),
        ('user_id',  Integer),
        ('edit',     Unicode),
        ('view',     Unicode)
    ]
    def __init__(self, parent=None):
        super(NetworkOwner, self).__init__()

        if parent is None:
            return
        self.network_id = parent.network_id
        self.user_id    = parent.user_id
        self.edit       = parent.edit
        self.view       = parent.view

class Unit(ComplexModel):
    """
    """
    _type_info = [
        ('name', Unicode),
        ('abbr', Unicode),
        ('cf', Double),
        ('lf', Double),
        ('info', Unicode),
        ('dimension', Unicode),
    ]

    def __init__(self, parent=None):
        super(Unit, self).__init__()

        if parent is None:
            return
        self.name = parent.name
        self.abbr = parent.abbr
        self.cf   = parent.cf
        self.lf   = parent.lf
        self.info = parent.info
        self.dimension = parent.dimension
