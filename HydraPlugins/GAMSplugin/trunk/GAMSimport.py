#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A Hydra plug-in to export a network and a scenario to a set of files, which
can be imported into a GAMS model.

Basics
~~~~~~

The GAMS import and export plug-in provides pre- and post-processing facilities
for GAMS models. The basic idea is that this plug-in exports data and
constraints from Hydra to a text file which can be imported into an existing
GAMS model using the ``$ import`` statement. It should also provide a GAMS
script handling the output of data from GAMS to a text file. That way we can
guarantee that results from GAMS can be imported back into Hydra in a
onsistent way.

Constraints
-----------

Output data
-----------


Options
~~~~~~~

====================== ====== ========== ======================================
Option                 Short  Parameter  Description
====================== ====== ========== ======================================
--gams-path            -g     GAMS_PATH  File path of the GAMS installation.
--network              -t     NETWORK    ID of the network that will be
                                         exported.
--scenario             -s     SCENARIO   ID of the scenario that will be
                                         exported.
--gdx-file             -f     GDX_FILE   GDX file containing GAMS results
====================== ====== ========== ======================================


API docs
~~~~~~~~
"""

import os
import logging
import argparse

from HydraLib.HydraException import HydraPluginError
from HydraLib.util import convert_ordinal_to_datetime
from HydraLib import hydra_logging
from HydraLib import PluginLib

from gamsAPI import gdxcc


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
        self.network = None
        self.attrs = dict()
        self.time_axis = dict()

        self.cli = PluginLib.connect()

    def load_network(self, network_id, scenario_id):
        self.network = self.cli.service.get_network(network_id, scenario_id)
        attrslist = self.cli.service.get_attributes()
        for attr in attrslist.Attr:
            self.attrs.update({attr.id: attr.name})

    def open_gdx_file(self, filename):
        filename = os.path.abspath(filename)
        gdxcc.gdxOpenRead(self.gdx_handle, filename)
        x, self.symbol_count, self.element_count = \
            gdxcc.gdxSystemInfo(self.gdx_handle)
        if x != 1:
            raise HydraPluginError('GDX file could not be opened.')
        logging.info('Import %s symbols and %s elements.' %
                     (self.symbol_count, self.element_count))

    def read_gdx_data(self):
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

    def parse_time_index(self, data_file):
        data_file = os.path.abspath(data_file)
        line = ''
        with open(data_file) as f:
            while line[0:24] != 'Parameter timestamp(t) ;':
                line = f.readline()
            line = f.readline()
            line = f.readline()
            while line.split('(', 1)[0].strip() == 'timestamp':
                idx = int(line.split('"')[1])
                timestamp = convert_ordinal_to_datetime(float(line.split()[2]))
                timestamp = PluginLib.date_to_string(timestamp)
                self.time_axis.update({idx: timestamp})
                line = f.readline()

    def assign_net_attrs(self):
        net_vars = dict()
        for attr in self.network.attributes.ResourceAttribute:
            if attr.attr_is_var == 'Y':
                net_vars.update({self.attrs[attr.attr_id]:
                                 (attr.id, attr.attr_id)})
        for var in self.gdx_variables.keys():
            if var in net_vars.keys():
                attr_id = net_vars[var][0]
                res_attr_id = net_vars[var][1]
                datatype = guess_data_type(self.gdx_variables[var], 'network')
                dataset = create_dataset(self.gdx_variables[var], datatype)

    def assign_link_attrs(self):
        pass

    def assign_node_attrs(self):
        pass

    def create_dataset(self, gdxvar):
        if datatype == 'timeseries':
            return self.create_timeseries(gdxvar)
        elif datatype == 'array':
            return self.create_array(gdxvar)
        elif datatype == 'scalar':
            return self.create_scalar(gdxvar)
        elif datatype == 'descriptor':
            return self.create_descriptor(gdxvar)
        else:
            logging.info('Could not assign data for variable "%s".'
                         % gdxvar.name)

    def create_timeseries(self, gdxvar):
        pass

    def create_scalar(self, gdxvar):
        pass

    def create_array(self, gdxvar):
        pass

    def create_descriptor(self, gdxvar):
        pass


def guess_data_type(gdxvar, restype):
    if restype == 'network':
        resdim = 0
    elif restype == 'node':
        resdim = 1
    elif restype == 'link':
        resdim = 2


def commandline_parser():
    parser = argparse.ArgumentParser(
        description="""Import results of a GAMS simulation from a GDX file into
a specific network and scenario.

Written by Philipp Meier <philipp@diemeiers.ch>
(c) Copyright 2013, University College London.
        """, epilog="For more information visit www.hydra-network.com",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    # Mandatory arguments
    parser.add_argument('-g', '--gams-path',
                        help='Path of the GAMS installation.')
    parser.add_argument('-t', '--network',
                        help='ID of the network the data will be imported to.')
    parser.add_argument('-s', '--scenario',
                        help='ID of the scenario the data will be imported to.')
    parser.add_argument('-f', '--gdx-file',
                        help='GDX file containing GAMS results.')
    parser.add_argument('-d', '--gams-data',
                        help='''Full path to the Hydra generated GAMS data file
                        (*.txt) used for the simulation.''')

    return parser


if __name__ == '__main__':
    parser = commandline_parser()
    args = parser.parse_args()

    gdximport = GAMSimport()
    gdximport.load_network(args.network, args.scenario)
    gdximport.parse_time_index(args.gams_data)
    gdximport.open_gdx_file(args.gdx_file)
    gdximport.read_gdx_data()

    for i, idx in enumerate(gdximport.gdx_variables['urTotalBenefits'].index):
        print idx, gdximport.gdx_variables['urTotalBenefits'].data[i]
