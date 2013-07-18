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
from hydrolopy.optim import reservoirData
from hydrolopy.TS import TimeSeries
from hydrolopy.data import importCSV

from datetime import datetime

from db import HydraIface
from HydraLib import hydra_logging


class HydraReservoirData(reservoirData):

    def __init__(self, reservoir_data):
        reservoirData.__init__(self)
        self = reservoir_data

    def get_attributes(self):
        attributes = []
        for par in self.parameter:
            attributes.append(par.keys())
        return attributes


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
            tmp_param['units'] = None
            tmp_param['dimen'] = None
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
            tmp_ts['units'] = None
            tmp_ts['dimen'] = None
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


def create_dataset(data, name, type, units, dimension):
    pass

if __name__ == '__main__':

    hydra_logging.init(level='DEBUG')
    # Load the structure file
    filename = '../../data/Gatineau_system.ods'
    gatineau_res_data = importReservoirData(filename)
    res_ids = gatineau_res_data.getid()

    # Load additional time series (daily time series).
    inflow_filenames = ['../../data/1008.csv',
                        '../../data/1009.csv',
                        '../../data/1056.csv',
                        '../../data/1077.csv',
                        '../../data/1078.csv',
                        '../../data/7030.csv']

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

    # Crate links (with start node and end node)
    #TODO: Finish this stuff
    #links = []
    #for i in res_ids:
    #    tmp_link = {}
    #    spills_to = gatineau_res_data.getParameter(i, 'Spill to')
    #    tmp_link['name'] = gatineau_res_data.getname(i) + ' - ' + \
    #        gatineau_res_data.getname(spills_to)
    #    tmp_link['startnode']

    # Write the data to the database

    project = create_project('Test example',
                             'Example project to test database schema.')

    # Write network data

    network = create_network('Gatineau River',
                             'Gatineau river basin network', project)

    node_list = []
    for n in nodes:
        node = create_node(n['name'], n['x'], n['y'], n['type'])
        node_list.append(node)

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

    # Write descriptors

    # Write equally spaced time series

    # Write ordinary (=unequally spaced) time series

    # Write arrays
