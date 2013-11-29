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

Input data for GAMS
-------------------

There are four types of parameters that can be exported: scalars, descriptors,
time series and arrays.

Constraints
-----------

Output data
-----------


Options
~~~~~~~

====================== ====== ========== ======================================
Option                 Short  Parameter  Description
====================== ====== ========== ======================================
--network              -n     NETWORK    ID of the network that will be
                                         exported.
--scenario             -s     SCENARIO   ID of the scenario that will be
                                         exported.
--output               -o     OUTPUT     Filename of the output file.
--start-date           -st    START_DATE Start date of the time period used for
                                         simulation.
--end-date             -en    END_DATE   End date of the time period used for
                                         simulation.
--time-step            -dt    TIME_STEP  Time step used for simulation.
--node-type-attr       -nt    NODE_TYPE_ATTR The name of the attribute
                                         specifying the node type.
--link-type-attr       -lt    LINK_TYPE_ATTR The name of the attribute
                                         specifying the link type.
--group-nodes-by       -gn    GROUP_ATTR Group nodes by this attribute(s).
--group_links-by       -gl    GROUP_ATTR Group links by this attribute(s).
====================== ====== ========== ======================================


API docs
~~~~~~~~
"""

import re
import argparse as ap
from datetime import datetime
from datetime import timedelta
from string import ascii_lowercase

from HydraLib import PluginLib
from HydraLib import units

from GAMSplugin import GAMSnetwork
from GAMSplugin import GAMSlink
from GAMSplugin import array_dim
from GAMSplugin import create_arr_index
from GAMSplugin import arr_to_matrix
from GAMSplugin import convert_date_to_timeindex


class GAMSexport(object):

    def __init__(self, network_id, scenario_id, filename):
        self.filename = filename
        self.time_index = []
        self.unit_conversion = units.Units()

        self.cli = PluginLib.connect()
        net = self.cli.service.get_network(network_id, scenario_id)
        attrs = self.cli.service.get_attributes()

        self.network = GAMSnetwork()
        self.network.load(net, attrs)

        self.output = """* Data exported from Hydra using GAMSplugin.
* (c) Copyright 2013, University College London
* (c) Copyright 2013, University of Manchester
*
* %s: %s
* Network-ID:  %s
* Scenario-ID: %s
*******************************************************************************

""" % (self.network.name, self.network.description,
            self.network.ID, self.network.scenario_id)

    def export_network(self):
        self.output += '* Network definition\n\n'
        self.export_nodes()
        self.export_links()
        self.connectivity_matrix()

    def export_nodes(self):
        self.output += 'SETS\n\n'
        # Write all nodes ...
        self.output += 'i vector of all nodes /\n'
        for node in self.network.nodes:
            self.output += node.name + '\n'
        self.output += '    /\n\n'
        # ... and create an alias for the index i called j:
        self.output += 'Alias(i,j)\n\n'
        # After an 'Alias; command another 'SETS' command is needed
        self.output += '* Node types\n\n'
        self.output += 'SETS\n\n'
        # Group nodes by type
        for object_type in self.network.node_types:
            self.output += object_type + '(i) /\n'
            for node in self.network.get_node(node_type=object_type):
                self.output += node.name + '\n'
            self.output += '/\n\n'
        # Define other groups
        self.output += '* Node groups\n\n'
        for group in self.network.node_groups:
            self.output += group + '(i) /\n'
            for node in self.network.get_node(group=group):
                self.output += node.name + '\n'
            self.output += '/\n\n'

    def export_links(self):
        self.output += 'SETS\n\n'
        # Write all links ...
        self.output += 'links(i,j) vector of all links /\n'
        for link in self.network.links:
            self.output += link.gams_name + '\n'
        self.output += '    /\n\n'
        # Group links by type
        self.output += '* Link types\n\n'
        for object_type in self.network.link_types:
            self.output += object_type + '(i,j) /\n'
            for link in self.network.get_link(link_type=object_type):
                self.output += link.gams_name + '\n'
            self.output += '/\n\n'
        # Define other groups
        for group in self.network.link_groups:
            self.output += group + '(i,j) /\n'
            for link in self.network.get_link(group=group):
                self.output += link.gams_name + '\n'
            self.output += '/\n\n'

    def connectivity_matrix(self):
        self.output += '* Connectivity matrix.\n'
        self.output += 'Table Connect(i,j)\n          '
        node_names = [node.name for node in self.network.nodes]
        for name in node_names:
            self.output += '%10s' % name
        self.output += '\n'
        conn = [[0 for node in node_names] for node in node_names]
        for link in self.network.links:
            conn[node_names.index(link.from_node)]\
                [node_names.index(link.to_node)] = 1

        for i in range(len(conn)):
            self.output += '%10s' % node_names[i]
            for j in range(len(conn[0])):
                self.output += '%10d' % conn[i][j]
            self.output += '\n\n'

    def export_data(self):
        # Export node data for each node type
        self.output += '* Node data\n\n'
        for node_type in self.network.node_types:
            self.output += '* Data for node type %s\n\n' % node_type
            nodes = self.network.get_node(node_type=node_type)
            self.export_parameters(nodes, 'scalar')
            self.export_parameters(nodes, 'descriptor')
            self.export_timeseries(nodes)
            self.export_arrays(nodes)

        # Export link data for each node type
        self.output += '* Link data\n\n'
        for link_type in self.network.link_types:
            self.output += '* Data for link type %s\n\n' % link_type
            links = self.network.get_link(link_type=link_type)
            self.export_parameters(links, 'scalar')
            self.export_parameters(links, 'descriptor')
            self.export_timeseries(links)
            self.export_arrays(links)

    def export_parameters(self, resources, datatype):
        """Export scalars or descriptors.
        """
        islink = isinstance(resources[0], GAMSlink)
        attributes = []
        obj_type = resources[0].object_type
        for attr in resources[0].attributes:
            if attr.dataset_type == datatype:
                attributes.append(attr)
        if len(attributes) > 0:
            self.output += 'SETS\n\n'  # Needed before sets are defined
            self.output += obj_type + '_' + datatype + 's /\n'
            for attribute in attributes:
                self.output += attribute.name + '\n'
            self.output += '/\n\n'
            if islink:
                self.output += 'Table ' + obj_type + '_' + datatype + \
                    '_data(i,j,' + obj_type + '_' + datatype + \
                    's) \n\n'
            else:
                self.output += 'Table ' + obj_type + '_' + datatype + \
                    '_data(i,' + obj_type + '_' + datatype + 's) \n\n'

            self.output += '                '
            for attribute in attributes:
                self.output += ' %14s' % attribute.name
            self.output += '\n'
            for resource in resources:
                if islink:
                    self.output += '{0:16}'.format(resource.gams_name)
                else:
                    self.output += '{0:16}'.format(resource.name)
                for attribute in attributes:
                    attr = resource.get_attribute(attr_name=attribute.name)
                    self.output += ' %14s' % attr.value.__getitem__(0)
                self.output += '\n'
            self.output += '\n\n'

    def export_timeseries(self, resources):
        """Export time series.
        """
        islink = isinstance(resources[0], GAMSlink)
        attributes = []
        obj_type = resources[0].object_type
        for attr in resources[0].attributes:
            if attr.dataset_type == 'timeseries':
                attributes.append(attr)
        if len(attributes) > 0:
            self.output += 'SETS\n\n'  # Needed before sets are defined
            self.output += obj_type + '_timeseries /\n'
            for attribute in attributes:
                self.output += attribute.name + '\n'
            self.output += '/\n\n'
            if islink:
                self.output += 'Table ' + obj_type + \
                    '_timeseries_data(t,i,j,' + obj_type + \
                    '_timeseries) \n\n       '
            else:
                self.output += 'Table ' + obj_type + \
                    '_timeseries_data(t,i,' + obj_type + \
                    '_timeseries) \n\n       '
            for attribute in attributes:
                for resource in resources:
                    if islink:
                        self.output += ' %14s' % (resource.gams_name + '.' +
                                                  attribute.name)
                    else:
                        self.output += ' %14s' % (resource.name + '.' +
                                                  attribute.name)
            self.output += '\n'

            for t, timestamp in enumerate(self.time_index):
                self.output += '{0:<7}'.format(t)
                for attribute in attributes:
                    for resource in resources:
                        attr = resource.get_attribute(attr_name=attribute.name)
                        soap_time = self.cli.factory.create('ns0:stringArray')
                        soap_time.string.append(PluginLib.date_to_string(timestamp))
                        data = self.cli.service.get_val_at_time(
                            attr.dataset_id, soap_time)
                        data = eval(data.data)
                        self.output += ' %14s' % data[0]
                self.output += '\n'
            self.output += '\n'

    def export_arrays(self, resources):
        """Export arrays.
        """
        attributes = []
        for attr in resources[0].attributes:
            if attr.dataset_type == 'array':
                attributes.append(attr)
        if len(attributes) > 0:
            # We have to write the complete array information for every single
            # node, because they might have different sizes.
            for resource in resources:
                #self.output += 'SETS\n\n'  # Needed before sets are defined
                #self.output += resource.name + '_' + '_arrays /\n'
                #for attribute in attributes:
                #    self.output += attribute.name + '\n'
                #self.output += '/\n\n'
                # This exporter only supports 'rectangular' arrays
                for attribute in attributes:
                    attr = resource.get_attribute(attr_name=attribute.name)
                    if attr.value is not None:
                        raw_array = attr.value.__getitem__(0)
                        array = []
                        for a in raw_array:
                            array.append(eval(a))
                        dim = array_dim(array)
                        self.output += '* Array %s for node %s, ' % \
                            (attr.name, resource.name)
                        self.output += 'dimensions are %s\n\n' % dim
                        # Generate array indices
                        self.output += 'SETS\n\n'
                        indexvars = list(ascii_lowercase)
                        for i, n in enumerate(dim):
                            self.output += indexvars[i] + '_' + \
                                resource.name + '_' + attr.name + \
                                ' array index /\n'
                            for idx in range(n):
                                self.output += str(idx) + '\n'
                            self.output += '/\n\n'

                        self.output += 'Table ' + resource.name + '_' + \
                            attr.name + '('
                        for i, n in enumerate(dim):
                            self.output += indexvars[i] + '_' + resource.name \
                                + '_' + attr.name
                            if i < (len(dim) - 1):
                                self.output += ','
                        self.output += ') \n\n'
                        ydim = dim[-1]
                        #self.output += ' '.join(['{0:10}'.format(y)
                        #                        for y in range(ydim)])
                        for y in range(ydim):
                            self.output += '{0:20}'.format(y)
                        self.output += '\n'
                        arr_index = create_arr_index(dim[0:-1])
                        matr_array = arr_to_matrix(array, dim)
                        for i, idx in enumerate(arr_index):
                            for n in range(ydim):
                                self.output += '{0:<10}'.format(
                                    ' . '.join([str(k) for k in idx]))
                                self.output += '{0:10}'.format(matr_array[i][n])
                            self.output += '\n'
                        self.output += '\n\n'

    def write_time_index(self, start_time, end_time, time_step):
        start_time = ' '.join(start_time)
        end_time = ' '.join(end_time)
        start_date = self.parse_date(start_time)
        end_date = self.parse_date(end_time)
        delta_t = self.parse_time_step(time_step)

        self.output += 'SETS\n\n'
        self.output += '* Time index\n'
        self.output += 't time index /\n'
        t = 0
        while start_date < end_date:

            self.output += '%s\n' % t
            self.time_index.append(start_date)
            start_date += timedelta(delta_t)
            t += 1

        self.output += '/\n\n'

        self.output += '* define time steps dependent on time index (t)\n\n'
        self.output += 'Parameter timestamp(t) ;\n\n'
        for t, date in enumerate(self.time_index):
            self.output += '    timestamp("%s") = %s ;\n' % \
                (t, convert_date_to_timeindex(date))
        self.output += '\n\n'

    def parse_time_step(self, time_step):
        """Read in the time step and convert it to days.
        """
        if len(time_step) == 2:
            value = float(time_step[0])
            units = time_step[1]
        elif len(time_step) == 1:
            # export numerical value from string using regex
            value = re.findall(r'\+d', time_step[0])
            valuelen = len(value)
            value = float(value)
            units = time_step[valuelen:].strip()

        return self.unit_conversion.convert(value, units, 'day')

    def parse_date(self, date):
        """Parse date string supplied from the user. All formats supported by
        HydraLib.PluginLib.guess_timefmt can be used.
        """
        # Guess format of the string
        FORMAT = PluginLib.guess_timefmt(date)
        return datetime.strptime(date, FORMAT)

    def write_file(self):
        with open(self.filename, 'w') as f:
            f.write(self.output)


def commandline_parser():
    parser = ap.ArgumentParser(
        description="""Export a network and a scenrio to a set of files, which
can be imported into a GAMS model.

Written by Philipp Meier <philipp@diemeiers.ch>
(c) Copyright 2013, University College London.
        """, epilog="For more information visit www.hydra-network.com",
        formatter_class=ap.RawDescriptionHelpFormatter)
    # Mandatory arguments
    #parser.add_argument('-p', '--project',
    #                    help='''ID of the project that will be exported.''')
    parser.add_argument('-n', '--network',
                        help='''ID of the network that will be exported.''')
    parser.add_argument('-s', '--scenario',
                        help='''ID of the scenario that will be exported.''')
    parser.add_argument('-o', '--output',
                        help='''Filename of the output file.''')
    parser.add_argument('-st', '--start-date', nargs='+',
                        help='''Start date of the time period used for
                        simulation.''')
    parser.add_argument('-en', '--end-date', nargs='+',
                        help='''End date of the time period used for
                        simulation.''')
    parser.add_argument('-dt', '--time-step', nargs='+',
                        help='''Time step used for simulation.''')
    parser.add_argument('-nt', '--node-type-attr',
                        help='''The name of the attribute specifying the node
                        type.''')
    parser.add_argument('-lt', '--link-type-attr',
                        help='''The name of the attribute specifying the link
                        type.''')

    # Optional arguments
    parser.add_argument('-gn', '--group-nodes-by', nargs='+',
                        help='''Group nodes by this attribute(s).''')
    parser.add_argument('-gl', '--group-links-by', nargs='+',
                        help='''Group links by this attribute(s).''')

    return parser


if __name__ == '__main__':
    parser = commandline_parser()
    args = parser.parse_args()

    exporter = GAMSexport(int(args.network), int(args.scenario), args.output)

    exporter.network.set_node_type(args.node_type_attr)
    exporter.network.set_link_type(args.link_type_attr)

    if args.group_nodes_by is not None:
        for ngroup in args.group_nodes_by:
            exporter.network.create_node_groups(ngroup)

    if args.group_links_by is not None:
        for lgroup in args.group_links_by:
            exporter.network.create_link_groups(lgroup)

    exporter.export_network()
    exporter.write_time_index(args.start_date, args.end_date, args.time_step)
    exporter.export_data()

    #exporter = GAMSexport(int(args.network),
    #                      int(args.scenario),
    #                      args.output)
    #exporter.export_network(args.node_type_attr,
    #                        args.link_type_attr,
    #                        args.group_nodes_by,
    #                        args.group_links_by)
    #exporter.export_data(args.start_date,
    #                     args.end_date,
    #                     args.time_step)

    exporter.write_file()
