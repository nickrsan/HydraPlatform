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

#from __future__ import hydra_db_connection


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
    the reason why an attribute ``Geometry`` is added.'''

    attributes = dict()
    attributes['scalar'] = []
    attributes['time_series'] = []
    attributes['array'] = []

    res_index = res_data.id.index(res_id)
    # Add scalars (parameters)
    for attr_name in res_data.parameter[res_index].keys():
        attributes['scalar'].append((attr_name, None))

    # Add time series
    for ts_name in res_data.TimeSeries[res_index].keys():
        attributes['time_series'].append((ts_name, None))

    # Add geometry information
    attributes['array'].append(('Geometry', None))

    return attributes


def extract_time_series_data(res_data, res_id, attribute_name):
    '''Extract a time series for a given reservoir and a given attribute from
    an object of type ``reservoirData``.'''

    res_index = res_data.id.index(res_id)
    return res_data.TimeSeries[res_index][attribute_name]


def convert_ts_to_equally_spaced(time_series):
    '''Convert a time series extracted from a reservoir dataset or a CSV file
    to an equally spaced time series as defined in the HydraDB database
    schema. A start date and a frequency will be defined.'''
    pass


def convert_ts_to_hydra_ts(time_series):
    '''Convert a time series extracted from a reservoir dataset or a CSV file
    to a time series with time stamp and number for each entry.'''

    hydra_ts = []

    # Convert all the TSdate timestamps (proprietary to hydrolopy) to datetime
    for ts_date in time_series.keys():
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

        hydra_ts.append((timestamp, time_series[ts_date]))

    return hydra_ts

if __name__ == '__main__':

    # Load the structure file
    filename = '../../data/Gatineau_system.ods'
    gatineau_res_data = importReservoirData(filename)

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

    # Extract all attributes from the reservoir data

    # Convert monthly data to time series with time stamp

    # Convert daily data to equally spaced data
    res_ids = gatineau_res_data.getid()
    ts_attributes = extract_attributes(gatineau_res_data, 1)['time_series']
    test_ts = extract_time_series_data(gatineau_res_data, 1,
                                       ts_attributes[0][0])
    print convert_ts_to_hydra_ts(test_ts)

    # Create a list of nodes

    # Crate links (with start node and end node)
