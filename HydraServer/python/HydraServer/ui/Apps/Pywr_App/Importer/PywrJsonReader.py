import json
import jsonpickle
import random
from collections import namedtuple
from dateutil.parser import parse
from datetime import datetime
from datetime import timedelta
import datetime
import inspect

recourseAttributea =[]
ref_parameters={}


class Counter(object):
    def __init__(self):
        self.id=-1

class Project (object):
    def __init__(self):
        self.name="Pywer exported project at "+str(datetime.datetime.now())
        self.status = 'A'
        self.description='Create by Pywr exporter'


class Value (object):
    def __init__(self, value):
        self.value=value
        self.dimen='Dimensionless'
        self.unit='-'


class Attribute (object):
    def __init__(self,name):
        self.name=name
        self.status='A'
        self.description='-'
        self.dimen='dimensionless'
        self.id=-1

class ResourceAttr (object):
    def __init__(self, id, attr_id, type):
        self.id=id
        self.attr_id=attr_id
        if (type.lower() == 'output'):
            self.attr_is_var = 'Y'
        else:
            self.attr_is_var = 'N'

class RecourseAttribute (object):
    def __init__(self,  ref_key, resource_attr_id, attr_id, value_, dimen):
        self.resource_attr_id=resource_attr_id
        self.attr_id=attr_id
        self.source=ref_key
        self.value=value_

class Node(object):
    def __init__(self, node_, counter, nodes_attributes):
        attributes_ = {}
        self.attributes = []
        ras = []
        self.x=None
        self.y=None
        i=0
        self.id = counter.id
        self.status = "A"
        self.name = node_.name
        self.type = node_.type
        self.description = ""
        if hasattr(node_, "position"):
            if hasattr(node_.position, "position"):
                if(node_.position.schematic != None and len (node_.position.schematic)==2):
                    self.x=str(node_.position.schematic[0])
                    self.y=str(node_.position.schematic[1])

            if hasattr(node_.position, "geographic"):
                if (node_.position.geographic != None and len(node_.position.geographic) == 2):
                    #counter.id = counter.id - 1
                    #attributes_['geographic'] = \
                    get_attribute_type_and_value(node_.position.geographic, 'geographic', counter, attributes_, self.attributes)
                    #self.attributes.append(ResourceAttr(counter.id, 'geographic', self.type))
                    #recourseAttributea.append(RecourseAttribute('NODE', counter.id, 'geographic', attributes_['geographic'], 'Dimensionless'))

        if(self.x == None):
            self.x = str(random.randint(0,99))
        if(self.y==None):
            self.y = str(random.randint(0,99))

        for i in range(0, len(node_.__dict__.keys())):
            k=node_.__dict__.keys()[i]
            if(k.lower() != 'name' and k.lower() !='type' and k.lower() !='position'):
                val=node_.__dict__.values()[i]
                get_attribute_type_and_value(val, k, counter, attributes_, self.type, self.attributes)


        get_attribute_type_and_value(node_.type, 'node_type', counter, attributes_, self.type, self.attributes)
        nodes_attributes [self.name]= attributes_

class Link(object):
    def __init__(self, edge_, nodes_ids, counter, links_attributes):
        self.node_1_id = nodes_ids[edge_[0]]
        self.node_2_id = nodes_ids[edge_[1]]
        self.name = edge_[0] + '_' + edge_[1]
        self.description=""
        self.attributes = []
        self.type='edge'
        if(len(edge_)==4):
            slot_from=edge_[2]
            slot_to=edge_[3]
        else:
            slot_from = None
            slot_to = None
        self.id = counter.id
        self.status = "A"
        self.attributes=[]
        attributes_ = {}
        counter.id = counter.id - 1
        attributes_['slot_from']=AttributeData('descriptor', str(slot_from), '-', 'Dimensionless')
        self.attributes.append(ResourceAttr(counter.id, 'slot_from','Input'))
        recourseAttributea.append(RecourseAttribute('LINK', counter.id, 'slot_from', attributes_['slot_from'], 'Dimensionless'))
        counter.id = counter.id - 1
        attributes_['slot_to']=AttributeData('descriptor', str(slot_to), '-', 'Dimensionless')
        self.attributes.append(ResourceAttr(counter.id, 'slot_to', 'Input'))
        recourseAttributea.append(RecourseAttribute('LINK', counter.id, 'slot_to', attributes_['slot_to'], 'Dimensionless'))
        links_attributes[self.name]=attributes_

class Network (object):
    def __init__(self, name, solver_name, project_id, counter, domains, recorders, network_attributes, timestepper,  metadata=None):
        attributes_={}
        self.project_id=project_id
        author=None
        if(metadata !=None):
            self.name=metadata.title
            self.description =metadata.description
            author=metadata.author
        else:
            self.name = name + '_' + str(datetime.datetime.now())
            self.description = "Pywr network"

        self.id=-1
        self.attributes = []
        self.nodes=[]
        self.links=[]
        self.scenarios=[Scenario()]

        if author is not None:
            counter.id = counter.id - 1
            attributes_['author'] = AttributeData('descriptor', author, '-', 'Dimensionless')
            self.attributes.append(ResourceAttr(counter.id, 'author', 'Input'))
            recourseAttributea.append(RecourseAttribute('NETWORK', counter.id, 'author', attributes_['author'], 'Dimensionless'))

        if(solver_name != None):
            counter.id = counter.id - 1
            attributes_['solver'] = AttributeData('descriptor', solver_name, '-', 'Dimensionless')
            self.attributes.append(ResourceAttr(counter.id, 'solver', 'Input'))
            recourseAttributea.append( RecourseAttribute('NETWORK', counter.id, 'solver', attributes_['solver'], 'Dimensionless'))

        counter.id = counter.id - 1
        attributes_['timestep'] = AttributeData('descriptor', str(timestepper.timestep), '-', 'Dimensionless')
        self.attributes.append(ResourceAttr(counter.id, 'timestep', 'Input'))
        recourseAttributea.append(
            RecourseAttribute('NETWORK', counter.id, 'timestep', attributes_['timestep'], 'Dimensionless'))

        counter.id = counter.id - 1
        attributes_['start_time'] = AttributeData('descriptor', str(timestepper.start), '-', 'Dimensionless')
        self.attributes.append(ResourceAttr(counter.id, 'start_time', 'Input'))
        recourseAttributea.append(
            RecourseAttribute('NETWORK', counter.id, 'start_time', attributes_['start_time'], 'Dimensionless'))

        counter.id = counter.id - 1
        attributes_['end_time'] = AttributeData('descriptor', str(timestepper.end), '-', 'Dimensionless')
        self.attributes.append(ResourceAttr(counter.id, 'end_time', 'Input'))
        recourseAttributea.append(
            RecourseAttribute('NETWORK', counter.id, 'end_time', attributes_['end_time'], 'Dimensionless'))


        domain_list=[]
        for domain in domains:
            domain_list.append([domain.name, domain.color])


        counter.id = counter.id - 1
        attributes_['domains'] = AttributeData('array', json.dumps(domain_list), '-', 'Dimensionless')
        self.attributes.append(ResourceAttr(counter.id, 'domains', 'Input'))
        recourseAttributea.append(
                RecourseAttribute('NETWORK', counter.id, 'domains', attributes_['domains'], 'Dimensionless'))

        recorders_list=[]
        for recorder in recorders:
            r_list=[]
            recorders_list.append(r_list)
            r_list.append(recorder.type)
            if hasattr(recorder, "recorders") and hasattr(recorder, "agg_func"):
                r_list.append(json.dumps(recorder.recorders))
                r_list.append(recorder.agg_func)
            else:
                r_list.append(recorder.node)

        counter.id = counter.id - 1
        attributes_['recorders'] = AttributeData('array', json.dumps(recorders_list), '-', 'Dimensionless')
        self.attributes.append(ResourceAttr(counter.id, 'recorders', 'Input'))
        recourseAttributea.append(
            RecourseAttribute('NETWORK', counter.id, 'recorders', attributes_['recorders'], 'Dimensionless'))
        network_attributes[self.name] = attributes_

class Scenario(object):
    def __init__(self):
        self.description="Created by PywrApp"
        self.name="scenarion_" + str(datetime.datetime.now())
        self.resourcescenarios=[]

class Resourcescenario(object):
    def __init__(self, source, resource_attr_id, attr_id, value):
        self.source=source
        self.resource_attr_id=resource_attr_id
        self.attr_id=attr_id
        self.value=value

class AttributeData (object):
    def __init__(self,  type, value, unit, dimen, metadata=None):
        self.hidden='N'
        self.type=type
        self.name = 'pywerApp'
        self.unit=unit
        self.dimension=dimen
        if(metadata is None):
            self.metadata='{}'
        else:
            self.metadata =json.dumps(metadata)
        self.value=value


class Recorder(object):
    def __init__(self, record_):
        self.type = record_.type
        if hasattr(record_, "recorders"):
            self.recorders=record_.recorders
            self.agg_func=record_.agg_func
        else:
            self.node=record_.node

class Domain (object):
    def __init__(self, domain_):
        self.name=domain_.name
        self.color=domain_.color

def get_timeseriesdates(timestepper):
    start=parse(timestepper.start)
    end=parse(timestepper.end)
    timestep=(timestepper.timestep)
    timeseries=[]
    while (end>=start):
        timeseries.append(start)

        start = start + timedelta(days=timestep)

def read_data_file(url, column=None):
    with open(url) as f:
        content = f.read().splitlines()
    head = content[0].split(',')
    if column is not None:
        for i in range(0, len(head)):
            if (head[i].strip().lower() == 'date' or head[i].strip().lower() == 'index'):
                date_index = i
            elif (head[i].strip().lower() == column.lower()):
                value_index = i
    else:
        if (head[0].lower() == 'date' or head[0].lower() == 'Index'):
            date_index = 0
            value_index = 1
        else:
            date_index = 1
            value_index = 0
    return content, date_index, value_index

def read_timeseries(url, column=None):
    content, date_index, value_index=read_data_file(url, column)
    values={}
    for i in range (1, len(content)):
        lin=content[i].split(',')
        values[lin[date_index]]=lin[value_index]
    return json.dumps({'0' :values})

def read_seasonall(values_):
    from datetime import date
    values = {}
    year = 9999
    month = 1
    day = 1
    #check if the data is saved on file
    if hasattr(values_, 'url'):
        if hasattr(values_, 'column'):
            content, date_index, value_index = read_data_file(values_.url, values_.column)
        else:
            content, date_index, value_index = read_data_file(values_.url)
        for i in range(1, len(content)):
            lin = content[i].split(',')
            ss=int (lin[date_index])
            if (values_.type.lower == 'monthlyprofile'):
                month = ss
                dat = datetime.datetime(year, ss, day)
            else:
                # account for 356 days (leap year) which Hydra 9999 seasonal data does not support
                if(ss>365):
                    continue
                dat=date.fromordinal(date(year, month, day).toordinal() + ss - 1)

            values[str(dat)] = lin[value_index]
    else:
        for v in values_.values:
            dat=datetime.datetime(year, month, day)
            values[str(dat)]=v
            if(values_.type.lower=='monthlyprofile'):
                month+=1
            else:
                day+=1
    return json.dumps({'0': values})

def get_attribute_type_and_value(value_, name, counter, attributes_, _type, res_attributes=None):
    metadata = {}
    metadata['single'] = 'yes'
    try:
        float(value_)
        value = str(value_)
        type = 'scalar'
    except:
        if isinstance(value_, list):
            type = "array"
            value = json.dumps(value_)
        elif isinstance(value_, basestring):
            if(value_ in ref_parameters.keys()):
                attr = ref_parameters[value_]
                type = attr.type
                value = attr.value
                metadata=json.loads(attr.metadata)
                if( 'type' in metadata.keys()):
                    if(metadata['type']=='aggregated'):
                        get_aggregated(json.loads(value), counter, attributes_, _type, res_attributes)
                    elif(metadata['type']=='indexedarray'):
                        get_aggregated(json.loads(value), counter, attributes_, _type, res_attributes)
                        if ('index_parameter' in metadata.keys()):
                            index_parameter = metadata['index_parameter']
                            get_attribute_type_and_value(index_parameter, index_parameter, counter, attributes_, _type, res_attributes)
                    elif (metadata['type'] == 'controlcurveindex'):
                        get_aggregated(json.loads(value), counter, attributes_, _type, res_attributes)
            else:
                value=value_
                type='descriptor'
        elif hasattr(value_, "type"):
            metadata['type'] = value_.type.lower()
            if value_.type.lower() =='constant':
                value = str(value_.values)
                type = 'scalar'
            elif value_.type.lower() == "arrayindexed":
                type = 'timeseries'
                metadata['type'] = 'arrayindexed'
                if hasattr(value_, "column"):
                    value = read_timeseries(value_.url, value_.column)
                    metadata['column']=value_.column
                else:
                    value = read_timeseries(value_.url)
            elif value_.type.lower()== "monthlyprofile" or value_.type.lower()== "dailyprofile":
                type = 'timeseries'
                metadata['type']=value_.type
                value = read_seasonall(value_)
                if hasattr(value_, "column"):
                    metadata['column'] = value_.column
                if hasattr(value_, "parse_dates"):
                    metadata['parse_dates'] = str(value_.parse_dates)
            elif value_.type.lower() == 'aggregated':
                get_aggregated_attribute(value_, name, counter, attributes_, _type, res_attributes)
                return
            elif value_.type.lower() == 'indexedarray':
                get_indexedarray_attributes(value_, name, counter, attributes_, _type,  res_attributes)

                return
            elif value_.type.lower() == 'controlcurveindex':
                get_controlcurveindex_attributes(value_, name, counter, attributes_, _type, res_attributes)
                return
            elif value_.type.lower() =='monthlyprofilecontrolcurve':
                get_monthlyprofilecontrolcurve_attributes(value_, name, counter, attributes_, _type, res_attributes)
                return
            elif value_.type.lower() == 'controlcurveinterpolated':
                get_controlcurveinterpolated_attributes(value_, name, counter, attributes_, _type, res_attributes)
                return


    if hasattr(value_, "comment"):
        metadata['comment']=value_.comment
    counter.id = counter.id - 1
    attributes_[name] = AttributeData(type, value, '-', 'Dimensionless', metadata)
    if(res_attributes!=None):
        res_attributes.append(ResourceAttr(counter.id, name, _type))
        recourseAttributea.append(RecourseAttribute('NODE', counter.id, name , attributes_[name],'Dimensionless'))

def get_aggregated_attribute(value_, name, counter, attributes_, _type, res_attributes=None):
    metadata={}
    type = 'array'
    value = json.dumps(value_.parameters)
    metadata['single'] = 'no'
    metadata['agg_func'] = value_.agg_func
    metadata['type'] = 'aggregated'
    if hasattr(value_, "comment"):
        metadata['comment'] = value_.comment

    counter.id = counter.id - 1
    attr= AttributeData(type, value, '-', 'Dimensionless', metadata)
    attributes_[name] =attr
    if(res_attributes != None):
        res_attributes.append(ResourceAttr(counter.id, name, _type))
        recourseAttributea.append(RecourseAttribute('NODE', counter.id, name, attributes_[name], 'Dimensionless'))
    get_aggregated (json.loads(attributes_[name].value), counter, attributes_,_type,  res_attributes)

def get_aggregated(paras, counter, attributes_, _type, res_attributes):
    for para in paras:
        get_attribute_type_and_value(para, para, counter, attributes_, _type, res_attributes)

def get_indexedarray_attributes(value_, name, counter, attributes_, _type, res_attributes=None):
    metadata={}
    type = 'array'
    metadata['single'] = 'no'
    metadata['type'] = 'indexedarray'
    if hasattr(value_, "comment"):
        metadata['comment'] = value_.comment
    index_list = get_indexedarray(value_.params, name, counter, attributes_, _type, res_attributes=None)
    value = json.dumps(index_list)

    if hasattr(value_, "index_parameter"):
        index_parameter=value_.index_parameter
        metadata['index_parameter']=index_parameter
    attr = AttributeData(type, value, '-', 'Dimensionless', metadata)
    counter.id = counter.id - 1
    attributes_[name] = attr
    if (res_attributes != None):
        res_attributes.append(ResourceAttr(counter.id, name, _type))
        recourseAttributea.append(RecourseAttribute('NODE', counter.id, name, attributes_[name], 'Dimensionless'))

def get_indexedarray(params, name, counter, attributes_, _type, res_attributes=None):
    index_list=[]
    ind=1
    for item in params:
        ind += 1
        name_ = name + "_" + str(ind)
        get_attribute_type_and_value(item, name_, counter, attributes_, _type, res_attributes)
        index_list.append(name_)
    return index_list


def get_controlcurveindex_attributes(value_, name, counter, attributes_, _type, res_attributes):
    metadata = {}
    type='array'
    metadata ['type'] = 'controlcurveindex'
    metadata['single'] = 'no'
    if hasattr(value_, "storage_node"):
        metadata['storage_node']=value_.storage_node
    if hasattr(value_, "comment"):
        metadata['comment'] = value_.comment
    value=value_.control_curves
    for item in value:
        get_attribute_type_and_value(item, item, counter, attributes_, _type, res_attributes)
    attr = AttributeData(type, json.dumps(value), '-', 'Dimensionless', metadata)
    counter.id = counter.id - 1
    attributes_[name] = attr
    if (res_attributes != None):
        res_attributes.append(ResourceAttr(counter.id, name, _type))
        recourseAttributea.append(RecourseAttribute('NODE', counter.id, name, attributes_[name], 'Dimensionless'))

def get_monthlyprofilecontrolcurve_attributes(value_, name, counter, attributes_, _type, res_attributes):
    metadata = {}
    type = 'array'
    metadata['type'] = 'monthlyprofilecontrolcurve'
    metadata['single'] = 'no'
    if hasattr(value_, "storage_node"):
        metadata['storage_node'] = value_.storage_node
    if hasattr(value_, "comment"):
        metadata['comment'] = value_.comment
    if hasattr(value_, "scale"):
        metadata['scale'] = str(value_.scale)
    values_list=[name+'_profile', name+'_control_curve', name+'_values']

    get_attribute_type_and_value(value_.profile, name + '_profile', counter, attributes_, _type, res_attributes)
    get_attribute_type_and_value(value_.control_curve, name + '_control_curve', counter, attributes_, _type, res_attributes)
    get_attribute_type_and_value(value_.values, name + '_values', counter, attributes_, _type, res_attributes)
    counter.id = counter.id - 1
    attr = AttributeData(type, json.dumps(values_list), '-', 'Dimensionless', metadata)

    attributes_[name] = attr
    if (res_attributes != None):
        res_attributes.append(ResourceAttr(counter.id, name, _type))
        recourseAttributea.append(RecourseAttribute('NODE', counter.id, name, attributes_[name], 'Dimensionless'))

def get_controlcurveinterpolated_attributes(value_, name, counter, attributes_, _type, res_attributes):
    type = 'array'
    metadata = {}
    metadata['single'] = 'no'
    metadata['type']='ControlCurveInterpolated'
    if hasattr(value_, "comment"):
        metadata['comment'] = value_.comment
    control_curve=value_.control_curve
    get_attribute_type_and_value(control_curve, name + 'control_curve', counter, attributes_, _type, res_attributes)
    counter.id = counter.id - 1

    val=[name + 'control_curve']
    attr = AttributeData(type, json.dumps(val), '-', 'Dimensionless', metadata)

    attributes_[name] = attr
    if (res_attributes != None):
        res_attributes.append(ResourceAttr(counter.id, name, _type))
        recourseAttributea.append(RecourseAttribute('NODE', counter.id, name, attributes_[name], 'Dimensionless'))

def get_new_attributes(attrlist, c_attrlist):
    new_attr=[]
    for attr in attrlist:
        for eattr in c_attrlist:
            if(attr.name.lower() == eattr.name.lower() and attr.dimen.lower() == eattr.dimen.lower):
                break
        new_attr.append(get_dict(attr))
    return new_attr

def get_dict(obj):
    if not  hasattr(obj,"__dict__"):
        return obj
    result = {}
    for key, val in obj.__dict__.items():

        if key.startswith("_"):
            continue
        if isinstance(val, list):
            element = []
            for item in val:
                element.append(get_dict(item))
        else:
            element = get_dict(obj.__dict__[key])
        result[key] = element
    return result

def import_net (filename, connector):
        counter=Counter()
        recorders=[]
        domains=[]
        project_attributes = {}
        pp= dict(
        name="CSV import at %s" ,
        description= \
            "Project created by the %s plug-in, %s." ,
        status='A',
        )
        c_attrlist = connector.call('get_all_attributes', {})
        nodes_attributes={}
        links_attributes = {}
        network_attributes={}
        f = open(filename,'r')
        json_string = ""
        while 1:
            line = f.readline()
            if not line:break
            json_string += line
        f.close()
        x = json.loads(json_string, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        timeseries=get_timeseriesdates(x.timestepper)
        project=Project()

        request=jsonpickle.dumps(project).replace("/","_").replace("\"", '\'')

        Proj = connector.call('add_project', {'project': project.__dict__})

        if hasattr(x, "recorders"):
            for recorder_ in x.recorders:
                recorders.append(Recorder(recorder_))
        if hasattr(x, "domains"):
            for domain_ in x.domains:
                domains.append(Domain(domain_))

        if hasattr(x, "metadata") and hasattr(x, "solver"):
            network = Network('Pywr', x.solver.name, Proj.id, counter, domains, recorders, network_attributes,
                              x.timestepper, x.metadata)
        elif hasattr(x, "solver"):
            network = Network('Pywr', x.solver.name, Proj.id, counter, domains, recorders, network_attributes,
                              x.timestepper)
        else:
            network = Network('Pywr', None, Proj.id, counter, domains, recorders, network_attributes,
                              x.timestepper)

        for attr_name in network_attributes[network.name].keys():
            if (attr_name in project_attributes):
                pass
            else:
                attr = Attribute(attr_name)
                project_attributes[attr_name] = attr

        counter.id=counter.id-1;
        nodes_ids={}

        i = 0
        if hasattr(x, "parameters"):
            for i in range(0, len(x.parameters.__dict__.keys())):
                k = x.parameters.__dict__.keys()[i]
                counter.id = counter.id - 1
                get_attribute_type_and_value(x.parameters.__dict__.values()[i], k, counter, ref_parameters, 'default')

        for node_ in x.nodes:
            node = Node(node_, counter, nodes_attributes)
            nodes_ids[node_.name] = node.id
            counter.id = counter.id - 1
            network.nodes.append(node)
            for attr_name in nodes_attributes[node.name].keys():
                if( attr_name in project_attributes):
                    pass
                else:
                    attr=Attribute(attr_name)
                    project_attributes[attr_name]=attr


        for edge_ in x.edges:
           link=Link(edge_, nodes_ids, counter, links_attributes)
           counter.id = counter.id - 1
           network.links.append(link)
           for attr_name in links_attributes[link.name].keys():
               if (attr_name in project_attributes):
                   pass
               else:
                   attr = Attribute(attr_name)
                   project_attributes[attr_name] = attr

        new_attr = get_new_attributes(project_attributes.values(), c_attrlist)
        attributes = connector.call('add_attributes', {'attrs': new_attr})
        attrs_names_ids={}
        for tt in attributes:
            attrs_names_ids[tt.name]=tt.id
        for rs in recourseAttributea:
            try:
                rs.attr_id=attrs_names_ids[rs.attr_id]
            except:
                print "Error-------------->", rs.attr_id
        network.scenarios[0].resourcescenarios=recourseAttributea

        for rs in network.attributes:
            rs.attr_id = attrs_names_ids[rs.attr_id]
        for node in network.nodes:
            for rs in node.attributes:
                rs.attr_id=attrs_names_ids[rs.attr_id]
        for link in network.links:
            for rs in link.attributes:
                rs.attr_id = attrs_names_ids[rs.attr_id]

        NetworkSummary = connector.call('add_network', {'net': get_dict(network)})
        return NetworkSummary


