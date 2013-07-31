#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''This piece of code is written in order to test the ability of HydraDB to
represent the data of a node/link network, create attributes and connect them
to data through a scenario.

The data used stems from the Gatinau River optimisation project and is
originally organised in an OpenOffice spread-sheet.'''

__copyright__ = "(C) 2013, University College London"
__author__ = 'Philipp Meier <p.meier@ucl.ac.uk>'
__license__ = 'LGPL'
__date__ = '17-07-2013'
__version__ = ""
__status__ = ""

from hydrolopy.optim import importReservoirData
#from hydrolopy.optim import reservoirData
from hydrolopy.TS import TimeSeries
from hydrolopy.data import importCSV

from datetime import datetime

from db import HydraIface
from HydraLib import hydra_logging
from HydraLib import hdb


def extract_attributes(res_data, res_id):
    '''Extract a complete list of attributes from an object of type
    ``reservoirData`` for reservoir ``res_id``. In a variable of type
    ``reservoirData`` all parameters and time series are stored in an ordered
    manner, according to the order of the reservoir ids.  Therefore the index
    of the reservoir ``res_id`` is extracted first and a list of attributes is
    pulled from the variable accordingly. Geometry data is stored in a special
    table and will be used as test case for a two dimensional array. This is
    the reason why an attribute ``Geometry`` is added.

    The function returns a dictionary containing all attributes that are
    parameters (key: 'param', time series (key: 'time_series') or arrays (key:
    'array').'''

    attributes = dict()
    attributes['param'] = []
    attributes['time_series'] = []
    attributes['array'] = []

    res_index = res_data.id.index(res_id)
    # Add parameters
    for attr_name in res_data.parameter[res_index].keys():
        attributes['param'].append((attr_name, None))

    # Add time series
    for ts_name in res_data.TimeSeries[res_index].keys():
        attributes['time_series'].append((ts_name, None))

    # Add geometry information
    attributes['array'].append(('Geometry', None))

    return attributes


def extract_parameters(res_data, attributes):
    '''Extract scalars and descriptors from a given reservoir dataset.'''

    descriptors = []
    scalars = []
    res_ids = res_data.getid()

    for attr in attributes['param']:
        for i in res_ids:
            tmp_param = {}
            tmp_param['value'] = res_data.getParameter(i, attr[0])
            tmp_param['name'] = attr[0] + '_' + res_data.getname(i)
            tmp_param['units'] = '-'
            tmp_param['dimen'] = '-'
            try:
                float(tmp_param['value'])
                scalars.append(tmp_param)
            except ValueError:
                descriptors.append(tmp_param)

    return scalars, descriptors


def extract_time_series_data(res_data, res_id, attribute_name):
    '''Extract a time series for a given reservoir and a given attribute from
    an object of type ``reservoirData``.'''

    res_index = res_data.id.index(res_id)
    res_ts = TimeSeries()
    for t in res_data.TimeSeries[res_index][attribute_name].keys():
        res_ts.add(t, res_data.TimeSeries[res_index][attribute_name][t])

    return res_ts


def convert_ts_to_equally_spaced(time_series):
    '''Convert a time series extracted from a reservoir dataset or a CSV file
    to an equally spaced time series as defined in the HydraDB database
    schema. A start date and a frequency will be defined.'''

    t_axis = time_series.getTime()
    t_axis.sort()
    start_d = t_axis[0]
    next_d = t_axis[1]
    start_time = datetime(start_d.year()[0],
                          start_d.month()[0],
                          start_d.day()[0])
    next_time = datetime(next_d.year()[0],
                         next_d.month()[0],
                         next_d.day()[0])
    frequency = (next_time - start_time).total_seconds()

    array = []
    for t in t_axis:
        array.append(time_series.getData(t)[0])

    return start_time, frequency, array


def convert_ts_to_hydra_ts(time_series):
    '''Convert a time series extracted from a reservoir dataset or a CSV file
    to a time series with time stamp and number for each entry.'''

    hydra_ts = []

    # Convert all the TSdate timestamps (proprietary to hydrolopy) to datetime
    for ts_date in time_series.getTime():
        # check if time series is repeated and monthly
        if ts_date.year()[0] == 'rep' and ts_date.day()[0] == 'mon':
            timestamp = datetime(1, ts_date.month()[0], 1)
        # check if it's monthly but not repeated
        elif ts_date.day()[0] == 'mon':
            timestamp = datetime(ts_date.year()[0], ts_date.month()[0], 1)
        # check if it's repeated and daily
        elif ts_date.year()[0] == 'rep':
            timestamp = datetime(1, ts_date.month()[0], ts_date.day()[0])
        # otherwise it's an ordinary daily time series
        else:
            timestamp = datetime(ts_date.year()[0], ts_date.month()[0],
                                 ts_date.day()[0])

        hydra_ts.append((timestamp, time_series.getData(ts_date)))

    return hydra_ts


def create_example_ts(res_data, attributes):
    ex_ts = []
    for attr in attributes['time_series']:
        for i in res_ids:
            tmp_ts = {}
            tmp_ts['name'] = attr[0] + '_' + res_data.getname(i)
            tmp_ts['units'] = '-'
            tmp_ts['dimen'] = '-'
            tmp_ts['time_series'] = convert_ts_to_hydra_ts(
                extract_time_series_data(res_data, 1, attr[0]))
            ex_ts.append(tmp_ts)

    return ex_ts


def create_project(name, description):
    '''Create a new project '''
    project = HydraIface.Project()
    project.db.project_name = name
    project.db.project_description = description
    project.save()
    project.commit()

    return project


def create_network(name, description, project):
    network = HydraIface.Network()
    network.db.network_name = name
    network.db.network_description = description
    network.db.project_id = project.db.project_id

    network.save()
    network.commit()

    return network


def create_node(name, x, y, node_type):
    node = HydraIface.Node()
    node.db.node_name = name
    node.db.node_x = x
    node.db.node_y = y
    node.db.node_type = node_type

    node.save()
    node.commit()

    return node


def create_link(name, from_node_id, to_node_id, link_type, network):
    link = HydraIface.Link()
    link.db.link_name = name
    link.db.node_1_id = from_node_id
    link.db.node_2_id = to_node_id
    link.db.link_type = link_type
    link.db.network_id = network.db.network_id

    link.save()
    link.commit()

    return link


def create_attribute(name, dimen):
    'Add attribute to DB.'
    attr = HydraIface.Attr()
    attr.db.attr_name = name
    attr.db.attr_dimen = dimen
    attr.save()
    attr.commit()

    return attr


def create_template_group(name):
    'Create a resource template group.'
    tgroup = HydraIface.ResourceTemplateGroup()
    tgroup.db.group_name = name
    tgroup.save()
    tgroup.commit()

    return tgroup


def create_node_template(name, group):
    'Create a node template.'
    template = HydraIface.ResourceTemplate()
    template.db.template_name = name
    template.db.group_id = group.db.group_id
    template.save()
    template.commit()

    return template


def add_attribute_to_template(attr, template):
    'Add an attribute to a template.'
    templ_item = HydraIface.ResourceTemplateItem()
    templ_item.db.attr_id = attr.db.attr_id
    templ_item.db.template_id = template.db.template_id
    templ_item.save()
    templ_item.commit()

    return templ_item


def add_attr_from_template(node, template):
    'Add all attributed defined by `template` to a node.'
    template.load()
    node.load()

    for t in template.resourcetemplateitems:
        node.add_attribute(t.db.attr_id)

    node.save()
    node.commit()


def create_scalar(s):
    dataset = HydraIface.ScenarioData()

    dataset.set_val('scalar', s['value'])
    dataset.db.data_units = s['units']
    dataset.db.data_name = s['name']
    dataset.db.data_dimen = s['dimen']
    dataset.save()
    dataset.commit()

    return dataset


def create_descriptor(d):
    dataset = HydraIface.ScenarioData()

    dataset.set_val('descriptor', d['value'])
    dataset.db.data_units = d['units']
    dataset.db.data_name = d['name']
    dataset.db.data_dimen = d['dimen']
    dataset.save()
    dataset.commit()

    return dataset


def create_ts(ts):
    dataset = HydraIface.ScenarioData()


def create_eq_ts(ts):
    dataset = HydraIface.ScenarioData()

    dataset.set_val('eqtimeseries', )


if __name__ == '__main__':

    hydra_logging.init(level='DEBUG')
    # Load the structure file
    filename = '../../data/gatineau/Gatineau_system.ods'
    gatineau_res_data = importReservoirData(filename)
    res_ids = gatineau_res_data.getid()

    # Load additional time series (daily time series).
    inflow_filenames = ['../../data/gatineau/1008.csv',
                        '../../data/gatineau/1009.csv',
                        '../../data/gatineau/1056.csv',
                        '../../data/gatineau/1077.csv',
                        '../../data/gatineau/1078.csv',
                        '../../data/gatineau/7030.csv']

    daily_ts_data = dict()
    n = 0
    for fname in inflow_filenames:
        daily_ts_data[n] = TimeSeries()
        daily_ts_data[n].importTS(importCSV(fname), 'DD/MM/YYYY')
        n += 1

    # Extract all attributes from the reservoir data (they happen to  be
    # exactly the same for each reservoir)
    attributes = extract_attributes(gatineau_res_data, 1)

    # Create a data structure with all scalars or descriptors (depending on the
    # data type of the attribute)

    scalars, descriptors = extract_parameters(gatineau_res_data, attributes)

    # Convert monthly data to time series with time stamp
    monthly_ts = create_example_ts(gatineau_res_data, attributes)

    # Convert daily data to equally spaced data
    inflow_ts_names = ['1008', '1009', '1056', '1077', '1078', '7030']
    eq_ts = []
    for n in daily_ts_data.keys():
        tmp_ts = {}
        tmp_ts['name'] = inflow_ts_names[n]
        tmp_ts['units'] = None
        tmp_ts['dime'] = None
        tmp_ts['start_time'], tmp_ts['frequency'], tmp_ts['array'] = \
            convert_ts_to_equally_spaced(daily_ts_data[n])
        eq_ts.append(tmp_ts)

    # Create a list of nodes
    # Add the real coordinates to each reservoir:
    res_x = {1: -76.475, 2: -75.984, 3: -75.926, 4: -75.774, 5: -75.756}
    res_y = {1: 47.315, 2: 46.725, 3: 45.815, 4: 45.512, 5: 45.500}

    nodes = []
    for i in res_ids:
        tmp_node = {}
        tmp_node['name'] = gatineau_res_data.getname(i)
        tmp_node['type'] = 'reservoir'
        tmp_node['x'] = res_x[i]
        tmp_node['y'] = res_y[i]
        nodes.append(tmp_node)

    # Write the data to the database
    hdb.connect()

    project = create_project('Test example',
                             'Example project to test database schema.')

    # Create a node template 'Reservoir node'
    template_group = create_template_group('Gatineau system')
    node_template = create_node_template('Reservoir node', template_group)

    # Add attributes
    attribute_list = []
    for n in nodes:
        for attr_type in attributes.keys():
            for attr in attributes[attr_type]:
                attribute = create_attribute(attr[0], attr[1])
                attribute_list.append(attribute)

    # Add attributes to node template
    for attr in attribute_list:
        add_attribute_to_template(attr, node_template)

    # Write network data
    network = create_network('Gatineau River',
                             'Gatineau river basin network', project)

    node_list = []
    for n in nodes:
        node = create_node(n['name'], n['x'], n['y'], n['type'])
        node_list.append(node)
        # Add attributes
        #for attr in attribute_list:
        #    node.add_attribute(attr.db.attr_id)
        add_attr_from_template(node, node_template)

    # Map local node ids to the node_ids in the DB
    id_map = {}
    for n in node_list:
        local_id = gatineau_res_data.id[
            gatineau_res_data.name.index(n.db.node_name)]
        id_map.update({local_id: n.db.node_id})

    # Create links
    links_list = []
    link_type = 'river'
    for i in res_ids:
        spillsto = gatineau_res_data.getParameter(i, 'Spill to')
        if spillsto != 0:
            linkname = gatineau_res_data.getname(i) + ' - ' + \
                gatineau_res_data.getname(spillsto)
            link = create_link(linkname, id_map[i], id_map[spillsto],
                               link_type, network)
            links_list.append(link)

    # Write data
    # Write scalars

    dataset_list = []
    for s in scalars:
        scalar = create_scalar(s)
        dataset_list.append(scalar)

    # Write descriptors

    for d in descriptors:
        if d['value'] is not None:
            descriptor = create_descriptor(d)
            dataset_list.append(descriptor)

    # Write equally spaced time series
    for ts in eq_ts:
        eq_timeseries = create_eq_ts(ts)
        dataset_list.append(eq_timeseries)

    # Write ordinary (=unequally spaced) time series
    #print monthly_ts

    # Write arrays

    # Create a scenario

    # Disconnect from db
    hdb.disconnect()
