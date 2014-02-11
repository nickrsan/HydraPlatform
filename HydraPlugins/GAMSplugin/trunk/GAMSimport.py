#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A Hydra plug-in to export a network and a scenario to a set of files, which
can be imported into a GAMS model.

Basics
~~~~~~

The GAMS import and export plug-in provides pre- and post-processing facilities
for GAMS models. The basic idea is that this plug-in exports data and
constraints from Hydra to a text file which can be imported into an existing
GAMS model using the ``$ import`` statement.

Options
~~~~~~~

====================== ====== ========== ======================================
Option                 Short  Parameter  Description
====================== ====== ========== ======================================
--gams-path            -G     GAMS_PATH  File path of the GAMS installation.
--network              -t     NETWORK    ID of the network that will be
                                         exported.
--scenario             -s     SCENARIO   ID of the scenario that will be
                                         exported.
--gdx-file             -f     GDX_FILE   GDX file containing GAMS results
--gams-model           -m     GMS_FILE   Full path to the GAMS model (*.gms)
                                         used for the simulation.
====================== ====== ========== ======================================


API docs
~~~~~~~~
"""

import os
import logging
import argparse

from operator import mul

from HydraLib.HydraException import HydraPluginError
from HydraLib.util import convert_ordinal_to_datetime
from HydraLib import hydra_logging
from HydraLib import PluginLib

from gamsAPI import gdxcc

from GAMSplugin import import_gms_data


class GDXvariable(object):

    def __init__(self):
        self.name = None
        self.dim = 0
        self.records = 0
        self.description = None
        self.datatype = None
        self.data = []
        self.index = []

    def set_info(self, info, extinfo):
        self.name = info[1]
        self.dim = info[2]
        self.records = extinfo[1]
        self.description = extinfo[3]


class GAMSimport(object):

    def __init__(self):
        hydra_logging.init(level='INFO')
        self.gdx_handle = gdxcc.new_gdxHandle_tp()
        rc = gdxcc.gdxCreate(self.gdx_handle, gdxcc.GMS_SSSIZE)
        if rc[0] == 0:
            raise HydraPluginError('Could not find GAMS installation.')
        self.symbol_count = 0
        self.element_count = 0
        self.gdx_variables = dict()
        self.gdx_ts_vars = dict()
        self.network = None
        self.res_scenario = None
        self.attrs = dict()
        self.time_axis = dict()
        self.gms_data = []

        self.cli = PluginLib.connect()

    def load_network(self, network_id, scenario_id):
        """Load network and scenario from the server.
        """
        self.network = self.cli.service.get_network(network_id, 'Y', scenario_id)
        #self.res_scenario = self.cli.factory.create('hyd:ResourceScenario')
        self.res_scenario = \
                self.network.scenarios.Scenario[0].resourcescenarios.ResourceScenario
        attrslist = self.cli.service.get_attributes()
        for attr in attrslist.Attr:
            self.attrs.update({attr.id: attr.name})

    def open_gdx_file(self, filename):
        """Open the GDX file and read some basic information.
        """
        filename = os.path.abspath(filename)
        gdxcc.gdxOpenRead(self.gdx_handle, filename)
        x, self.symbol_count, self.element_count = \
            gdxcc.gdxSystemInfo(self.gdx_handle)
        if x != 1:
            raise HydraPluginError('GDX file could not be opened.')
        logging.info('Importing %s symbols and %s elements.' %
                     (self.symbol_count, self.element_count))

    def read_gdx_data(self):
        """Read variables and data from GDX file.
        """
        for i in range(self.symbol_count):
            gdx_variable = GDXvariable()
            info = gdxcc.gdxSymbolInfo(self.gdx_handle, i + 1)
            extinfo = gdxcc.gdxSymbolInfoX(self.gdx_handle, i + 1)
            gdx_variable.set_info(info, extinfo)
            gdxcc.gdxDataReadStrStart(self.gdx_handle, i + 1)
            for n in range(gdx_variable.records):
                x, idx, data, y = gdxcc.gdxDataReadStr(self.gdx_handle)
                gdx_variable.index.append(idx)
                gdx_variable.data.append(data[0])
            self.gdx_variables.update({gdx_variable.name: gdx_variable})

    def load_gams_file(self, gms_file):
        """Read in the .gms file.
        """
        gms_file = os.path.abspath(gms_file)
        gms_data = import_gms_data(gms_file)
        self.gms_data = gms_data.split('\n')

    def parse_time_index(self):
        """Read the time index of the GAMS model used. This only works for
        models where data is exported from Hydra using GAMSexport.
        """
        for i, line in enumerate(self.gms_data):
            if line[0:24] == 'Parameter timestamp(t) ;':
                break
        i += 2
        line = self.gms_data[i]
        while line.split('(', 1)[0].strip() == 'timestamp':
            idx = int(line.split('"')[1])
            timestamp = convert_ordinal_to_datetime(float(line.split()[2]))
            timestamp = PluginLib.date_to_string(timestamp)
            self.time_axis.update({idx: timestamp})
            i += 1
            line = self.gms_data[i]

    def parse_variables(self):
        """For all variables stored in the gdx file, check if these are time
        time series or not.
        """
        for i, line in enumerate(self.gms_data):
            if line.strip().lower() == 'variables':
                break

        i += 1
        line = self.gms_data[i]
        while line.strip() != ';':
            var = line.split()[0]
            splitvar = var.split('(', 1)
            varname = splitvar[0]
            if len(splitvar) == 1:
                params = []
            else:
                params = splitvar[1][0:-1].split(',')
            if 't' in params:
                self.gdx_ts_vars.update({varname: params.index('t')})

            i += 1
            line = self.gms_data[i]

    def assign_attr_data(self):
        """Assign data to all variable attributes in the network.
        """
        # Network attributes
        for attr in self.network.attributes.ResourceAttr:
            if attr.attr_is_var == 'Y':
                if self.attrs[attr.attr_id] in self.gdx_variables.keys():
                    gdxvar = self.gdx_variables[self.attrs[attr.attr_id]]
                    dataset = self.cli.factory.create('hyd:Dataset')
                    dataset.name = 'GAMS import - ' + gdxvar.name
                    if gdxvar.name in self.gdx_ts_vars.keys():
                        dataset.type = 'timeseries'
                        index = []
                        for idx in gdxvar.index:
                            index.append(idx[self.gdx_ts_vars[gdxvar.name]])
                        data = gdxvar.data
                        dataset.value = self.create_timeseries(index, data)
                    elif gdxvar.dim == 0:
                        data = gdxvar.data[0]
                        try:
                            data = float(data)
                            dataset.type = 'scalar'
                            dataset.value = self.create_scalar(data)
                        except ValueError:
                            dataset.type = 'descriptor'
                            dataset.value = self.create_descriptor(data)
                    elif gdxvar.dim > 0:
                        dataset.type = 'array'
                        dataset.value = self.create_array(gdxvar.index,
                                                          gdxvar.data)

                    # Add data
                    res_scen = self.cli.factory.create('hyd:ResourceScenario')
                    res_scen.resource_attr_id = attr.id
                    res_scen.attr_id = attr.attr_id
                    res_scen.value = dataset
                    self.res_scenario.append(res_scen)

        # Node attributes
        nodes = dict()
        for node in self.network.nodes.Node:
            nodes.update({node.id: node.name})
            for attr in node.attributes.ResourceAttr:
                if attr.attr_is_var == 'Y':
                    if self.attrs[attr.attr_id] in self.gdx_variables.keys():
                        gdxvar = self.gdx_variables[self.attrs[attr.attr_id]]
                        dataset = self.cli.factory.create('hyd:Dataset')
                        dataset.name = 'GAMS import - ' + node.name + ' ' \
                            + gdxvar.name
                        if gdxvar.name in self.gdx_ts_vars.keys():
                            dataset.type = 'timeseries'
                            index = []
                            data = []
                            for i, idx in enumerate(gdxvar.index):
                                if node.name in idx:
                                    index.append(
                                        idx[self.gdx_ts_vars[gdxvar.name]])
                                    data.append(gdxvar.data[i])
                            dataset.value = self.create_timeseries(index, data)
                        elif gdxvar.dim == 1:
                            for i, idx in enumerate(gdxvar.index):
                                if node.name in idx:
                                    data = gdxvar.data[i]
                                    try:
                                        data = float(data)
                                        dataset.type = 'scalar'
                                        dataset.value = \
                                            self.create_scalar(data)
                                    except ValueError:
                                        dataset.type = 'descriptor'
                                        dataset.value = \
                                            self.create_descriptor(data)
                                    break
                        elif gdxvar.dim > 1:
                            dataset.type = 'array'
                            index = []
                            data = []
                            for i, idx in enumerate(gdxvar.index):
                                if node.name in idx:
                                    idx.pop(idx.index(node.name))
                                    index.append(idx)
                                    data.append(gdxvar.data[i])
                            dataset.value = self.create_array(gdxvar.index,
                                                              gdxvar.data)

                        res_scen = \
                            self.cli.factory.create('hyd:ResourceScenario')
                        res_scen.resource_attr_id = attr.id
                        res_scen.attr_id = attr.attr_id
                        res_scen.value = dataset
                        self.res_scenario.append(res_scen)

        # Link attributes
        for link in self.network.links.Link:
            for attr in link.attributes.ResourceAttr:
                if attr.attr_is_var == 'Y':
                    fromnode = nodes[link.node_1_id]
                    tonode = nodes[link.node_2_id]
                    if self.attrs[attr.attr_id] in self.gdx_variables.keys():
                        gdxvar = self.gdx_variables[self.attrs[attr.attr_id]]
                        dataset = self.cli.factory.create('hyd:Dataset')
                        dataset.name = 'GAMS import - ' + link.name + ' ' \
                            + gdxvar.name
                        if gdxvar.name in self.gdx_ts_vars.keys():
                            dataset.type = 'timeseries'
                            index = []
                            data = []
                            for i, idx in enumerate(gdxvar.index):
                                if fromnode in idx and tonode in idx and \
                                   idx.index(fromnode) < idx.index(tonode):
                                    index.append(
                                        idx[self.gdx_ts_vars[gdxvar.name]])
                                    data.append(gdxvar.data[i])
                            dataset.value = self.create_timeseries(index, data)
                        elif gdxvar.dim == 2:
                            for i, idx in enumerate(gdxvar.index):
                                if fromnode in idx and tonode in idx and \
                                   idx.index(fromnode) < idx.index(tonode):
                                    data = gdxvar.data[i]
                                    try:
                                        data = float(data)
                                        dataset.type = 'scalar'
                                        dataset.value = \
                                            self.create_scalar(data)
                                    except ValueError:
                                        dataset.type = 'descriptor'
                                        dataset.value = \
                                            self.create_descriptor(data)
                                    break
                        elif gdxvar.dim > 2:
                            dataset.type = 'array'
                            index = []
                            data = []
                            for i, idx in enumerate(gdxvar.index):
                                if fromnode in idx and tonode in idx and \
                                   idx.index(fromnode) < idx.index(tonode):
                                    idx.pop(idx.index(fromnode))
                                    idx.pop(idx.index(tonode))
                                    index.append(idx)
                                    data.append(gdxvar.data[i])
                            dataset.value = self.create_array(gdxvar.index,
                                                              gdxvar.data)

                        res_scen = \
                            self.cli.factory.create('hyd:ResourceScenario')
                        res_scen.resource_attr_id = attr.id
                        res_scen.attr_id = attr.attr_id
                        res_scen.value = dataset
                        self.res_scenario.append(res_scen)

    def create_timeseries(self, index, data):
        #timeseries = self.cli.factory.create('hyd:TimeSeries')
        #for i, idx in enumerate(index):
        #    tsdata = self.cli.factory.create('hyd:TimeSeriesData')
        #    tsdata.ts_time = self.time_axis[int(idx)]
        #    tsdata.ts_value = [float(data[i])]
        #    timeseries.ts_values.TimeSeriesData.append(tsdata)

        timeseries = {'ts_values': []}
        for i, idx in enumerate(index):
            timeseries['ts_values'].append({'ts_time':
                                            self.time_axis[int(idx)],
                                            'ts_value':
                                            [float(data[i])]})

        return timeseries

    def create_scalar(self, value):
        scalar = self.cli.factory.create('hyd:Scalar')
        scalar.param_value = value

        return scalar

    def create_array(self, index, data):
        hydra_array = self.cli.factory.create('hyd:Array')
        dimension = len(index[0])
        extent = []
        for n in range(dimension):
            n_idx = []
            for idx in index:
                n_idx.append(int(idx[n]))
            extent.append(max(n_idx))

        array = 0
        for e in extent:
            new_array = [array for i in range(e)]
            array = new_array

        array = data
        while len(extent) > 1:
            i = 0
            outer_array = []
            for m in range(reduce(mul, extent[0:-1])):
                inner_array = []
                for n in range(extent[-1]):
                    inner_array.append(array[i])
                    i += 1
                outer_array.append(inner_array)
            array = outer_array
            extent = extent[0:-1]

        hydra_array.arr_data = str(array)

        return hydra_array

    def create_descriptor(self, value):
        descriptor = self.cli.factory.create('hyd:Descriptor')
        descriptor.dexc_val = value

        return descriptor

    def save(self):
        self.network.scenarios.Scenario[0].resourcescenarios.ResourceScenario \
            = self.res_scenario
        self.cli.service.update_network(self.network)


def commandline_parser():
    parser = argparse.ArgumentParser(
        description="""Import results of a GAMS simulation from a GDX file into
a specific network and scenario.

Written by Philipp Meier <philipp@diemeiers.ch>
(c) Copyright 2013, University College London.
        """, epilog="For more information visit www.hydra-network.com",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    # Mandatory arguments
    parser.add_argument('-G', '--gams-path',
                        help='Path of the GAMS installation.')
    parser.add_argument('-t', '--network',
                        help='ID of the network the data will be imported to.')
    parser.add_argument('-s', '--scenario',
                        help='ID of the scenario the data will be imported to.')
    parser.add_argument('-f', '--gdx-file',
                        help='GDX file containing GAMS results.')
    parser.add_argument('-m', '--gms-file',
                        help='''Full path to the GAMS model (*.gms) used for
                        the simulation.''')

    return parser


if __name__ == '__main__':
    parser = commandline_parser()
    args = parser.parse_args()

    gdximport = GAMSimport()
    gdximport.load_network(args.network, args.scenario)
    gdximport.load_gams_file(args.gms_file)
    gdximport.parse_time_index()
    gdximport.open_gdx_file(args.gdx_file)
    gdximport.read_gdx_data()
    gdximport.parse_variables()
    gdximport.assign_attr_data()

    gdximport.save()
