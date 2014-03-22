#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A Hydra plug-in to export a network and a scenario to a set of files, which
can be imported into a GAMS model.

The GAMS import plug-in provides an easy to use tool for exporting data from
HydraPlatform to custom GAMS models. The basic idea is that this plug-in
exports a network and associated data from HydraPlatform to a text file which
can be imported into an existing GAMS model using the ``$ import`` statement.

Using the commandline tool
--------------------------

**Mandatory arguments:**

====================== ======= ========== ======================================
Option                 Short   Parameter  Description
====================== ======= ========== ======================================
``--network``          ``-t``  NETWORK    ID of the network that will be
                                          exported.
``--scenario``         ``-s``  SCENARIO   ID of the scenario that will be
                                          exported.
``--template-id``      ``-tp`` TEMPLATE   ID of the template used for exporting
                                          resources. Attributes that don't
                                          belong to this template are ignored.
``--output``           ``-o``  OUTPUT     Filename of the output file.
====================== ======= ========== ======================================

**Optional arguments:**

====================== ======= ========== ======================================
``--group-nodes-by``   ``-gn`` GROUP_ATTR Group nodes by this attribute(s).
``--group_links-by``   ``-gl`` GROUP_ATTR Group links by this attribute(s).
====================== ======= ========== ======================================


Specifying the time axis
~~~~~~~~~~~~~~~~~~~~~~~~

One of the following two options for specifying the time domain of the model is
mandatory:

**Option 1:**

====================== ======= ========== ======================================
``--start-date``       ``-st`` START_DATE Start date of the time period used for
                                          simulation.
``--end-date``         ``-en`` END_DATE   End date of the time period used for
                                          simulation.
``--time-step``        ``-dt`` TIME_STEP  Time step used for simulation. The
                                          time step needs to be specified as a
                                          valid time length as supported by
                                          Hydra's unit conversion function (e.g.
                                          1 s, 3 min, 2 h, 4 day, 1 mon, 1 yr)
====================== ======= ========== ======================================

**Option 2:**

====================== ======= ========== ======================================
``--time-axis``        ``-tx`` TIME_AXIS  Time axis for the modelling period (a
                                          list of comma separated time stamps).
====================== ======= ========== ======================================


Input data for GAMS
-------------------

.. note::

    The main goal of this plug-in is to provide a *generic* tool for exporting
    network topologies and data to a file readable by GAMS. In most cases it
    will be necessary to adapt existing GAMS models to the naming conventions
    used by this plug-in.

Network topology
~~~~~~~~~~~~~~~~

Nodes are exported to GAMS by name and referenced by index ``i``::

    SETS

    i vector of all nodes /
    NodeA
    NodeB
    NodeC
    /

The representation of links based on node names. The set of links therefore
refers to the list of nodes. Because there are always two nodes that are
connected by a link, the list of link refers to the index of nodes::

    Alias(i,j)

    SETS

    links(i,j) vector of all links /
    NodeA . NodeB
    NodeB . NodeC
    /

In addition to links, GAMSexport provides a connectivity matrx::

    * Connectivity matrix.
    Table Connect(i,j)
                    NodeA     NodeB     NodeC
    NodeA               0         1         0
    NodeB               0         0         1
    NodeC               0         0         0


Nodes and links are also grouped by node type::

    * Node groups

    Ntype1(i) /
    NodeA
    NodeB
    /

    Ntype2(i) /
    NodeC
    /

    * Link groups

    Ltype1(i,j) /
    NodeA . NodeB
    NodeB . NodeC
    /

If you want to learn more about node and link types, please refer to the Hydra
documentation.


Datasets
~~~~~~~~

There are four types of parameters that can be exported: scalars, descriptors,
time series and arrays. Because of the way datasets are translated to GAMS
code, data used for the same attribute of different nodes and links need to be
of the same type (scalar, descriptor, time series, array). This restriction
applies for nodes and links that are of the same type. For example, ``NodeA``
and ``NodeB`` have node type ``Ntype1``, both have an attribute ``atttr_a``.
Then both values for ``attr_a`` need to be a scalar (or both need to be a
descriptor, ...). It is also possible that one node does not have a value for
one specific attribute, while other nodes of the same type do. In this case,
make sure that the GAMS mode code supports this.

Scalars and Descriptors:
    Scalars and descriptors are exported based on node and link types. All
    scalar datasets of each node (within one node type) are exported into one
    table::

        SETS

        Ntype1_scalars /
        attr_a
        attr_c
        /

        Table Ntype1_scalar_data(i, Ntype1_scalars)

                        attr_a      attr_c
        NodeA              1.0         2.0
        NodeB           3.1415         0.0

    Descriptors are handled in exactly the same way.

Time series:
    For all time series exported, a common time index is defined::

        SETS

        * Time index
        t time index /
        0
        1
        2
        /

    In case the length of each time step is not uniform and it is used in the
    model, timestamps corresponding to each time index are stored in the
    ``timestamp`` parameter::

        Parameter timestamp(t) ;

            timestamp("0") = 730851.0 ;
            timestamp("1") = 730882.0 ;
            timestamp("2") = 730910.0 ;

    Timestamps correspond to the Gregorian ordinal of the date, where the value
    of 1 corresponds to January 1, year 1.

    Similar to scalars and descriptors, time series for one node or link type
    are summarised in one table::

        SETS

        Ntype1_timeseries /
        attr_b
        attr_d
        /

        Table Ntype1_timeseries_data(t,i,Ntype1_timeseries)

                NodeA.attr_b    NodeB.attr_b    NodeA.attr_d    NodeB.attr_b
        0                1.0            21.1          1001.2          1011.4
        1                2.0            21.0          1003.1          1109.0
        2                3.0            20.9          1005.7          1213.2



Arrays:
    Due to their nature, arrays can not be summarised by node type. For every
    array that is exported a complete structure needs to be defined. It is best
    to show this structure based on an example::

        * Array attr_e for node NodeC, dimensions are [2, 2]

        SETS

        a_NodeC_attr_e array index /
        0
        1
        /

        b_NodeC_attr_e array index /
        0
        1
        /

        Table NodeC_attr_e(a_NodeC_attr_e,b_NodeC_attr_e)

                    0       1
        0         5.0     6.0
        1         7.0     8.0

    For every additional dimension a new index is created based on letters (a
    to z). This also restricts the maximum dimensions of an array to 26.  We
    are willing to increase this restriction to 676 or more as soon as somebody
    presents us with a real-world problem that needs arrays with more than 26
    dimensions.

API docs
--------
"""

import re
import argparse as ap
from datetime import datetime
from datetime import timedelta
from string import ascii_lowercase

from HydraLib import PluginLib
from HydraLib.HydraException import HydraPluginError
from HydraLib.util import array_dim

from GAMSplugin import GAMSnetwork
from GAMSplugin import GAMSlink
from GAMSplugin import create_arr_index
from GAMSplugin import arr_to_matrix
from GAMSplugin import convert_date_to_timeindex


class GAMSexport(object):

    def __init__(self, network_id, scenario_id, filename):
        self.filename = filename
        self.time_index = []

        self.cli = PluginLib.connect()
        net = self.cli.service.get_network(network_id,
                                           'Y',
                                           scenario_ids=[scenario_id])
        attrs = self.cli.service.get_attributes()

        self.network = GAMSnetwork()
        self.network.load(net, attrs)

        self.template_id = None

        self.output = """* Data exported from Hydra using GAMSplugin.
* (c) Copyright 2013, University College London
* (c) Copyright 2013, 2014, University of Manchester
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
        for object_type in self.network.get_node_types(template_id=self.template_id):
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
        for object_type in self.network.get_link_types(template_id=self.template_id):
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
        for node_type in self.network.get_node_types(template_id=self.template_id):
            self.output += '* Data for node type %s\n\n' % node_type
            nodes = self.network.get_node(node_type=node_type)
            self.export_parameters(nodes, node_type, 'scalar')
            self.export_parameters(nodes, node_type, 'descriptor')
            self.export_timeseries(nodes, node_type)
            self.export_arrays(nodes)

        # Export link data for each node type
        self.output += '* Link data\n\n'
        for link_type in self.network.get_link_types(template_id=self.template_id):
            self.output += '* Data for link type %s\n\n' % link_type
            links = self.network.get_link(link_type=link_type)
            self.export_parameters(links, link_type, 'scalar')
            self.export_parameters(links, link_type,'descriptor')
            self.export_timeseries(links, link_type)
            self.export_arrays(links)

    def export_parameters(self, resources, obj_type, datatype):
        """Export scalars or descriptors.
        """
        islink = isinstance(resources[0], GAMSlink)
        attributes = []
        for attr in resources[0].attributes:
            if attr.dataset_type == datatype and attr.is_var is False:
                attr.name = translate_attr_name(attr.name)
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

    def export_timeseries(self, resources, obj_type):
        """Export time series.
        """
        islink = isinstance(resources[0], GAMSlink)
        attributes = []
        for attr in resources[0].attributes:
            if attr.dataset_type == 'timeseries' and attr.is_var is False:
                attr.name = translate_attr_name(attr.name)
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
                    attr = resource.get_attribute(attr_name=attribute.name)
                    if attr.dataset_id is not None:
                        if islink:
                            self.output += ' %14s' % (resource.gams_name + '.'
                                                      + attribute.name)
                        else:
                            self.output += ' %14s' % (resource.name + '.'
                                                      + attribute.name)
            self.output += '\n'

            for t, timestamp in enumerate(self.time_index):
                self.output += '{0:<7}'.format(t)
                for attribute in attributes:
                    for resource in resources:
                        attr = resource.get_attribute(attr_name=attribute.name)
                        if attr.dataset_id is not None:
                            soap_time = self.cli.factory.create('ns0:stringArray')
                            soap_time.string.append(PluginLib.date_to_string(timestamp))
                            data = self.cli.service.get_val_at_time(
                                attr.dataset_id, soap_time)
                            data = eval(data.data)
                            self.output += ' %14f' % data[0]
                self.output += '\n'
            self.output += '\n'

    def export_arrays(self, resources):
        """Export arrays.
        """
        attributes = []
        for attr in resources[0].attributes:
            if attr.dataset_type == 'array' and attr.is_var is False:
                attr.name = translate_attr_name(attr.name)
                attributes.append(attr)
        if len(attributes) > 0:
            # We have to write the complete array information for every single
            # node, because they might have different sizes.
            for resource in resources:
                # This exporter only supports 'rectangular' arrays
                for attribute in attributes:
                    attr = resource.get_attribute(attr_name=attribute.name)
                    if attr.value is not None:
                        array = eval(attr.value.__getitem__(0))
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

    def write_time_index(self, start_time=None, end_time=None, time_step=None,
                         time_axis=None):

        self.output += 'SETS\n\n'
        self.output += '* Time index\n'
        self.output += 't time index /\n'

        if time_axis is None:
            start_time = ' '.join(start_time)
            end_time = ' '.join(end_time)
            start_date = self.parse_date(start_time)
            end_date = self.parse_date(end_time)
            delta_t = self.parse_time_step(time_step)

            t = 0
            while start_date < end_date:

                self.output += '%s\n' % t
                self.time_index.append(start_date)
                start_date += timedelta(delta_t)
                t += 1

            self.output += '/\n\n'

        else:
            time_axis = ' '.join(time_axis).split(',')
            t = 0
            for timestamp in time_axis:
                date = self.parse_date(timestamp.strip())
                self.time_index.append(date)
                self.output += '%s\n' % t
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

        return self.cli.service.convert_units(value, units, 'day')[0]

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


def translate_attr_name(name):
    """Replace non alphanumeric characters with '_'. This function throws an
    error, if the first letter of an attribute name is not an alphabetic
    character.
    """
    if isinstance(name, str):
        translator = ''.join(chr(c) if chr(c).isalnum()
                             else '_' for c in range(256))
    elif isinstance(name, unicode):
        translator = UnicodeTranslate()

    name = name.translate(translator)

    return name


class UnicodeTranslate(dict):
    """Translate a unicode attribute name to a valid GAMS variable.
    """
    def __missing__(self, item):
        char = unichr(item)
        repl = u'_'
        if item < 256 and char.isalnum():
            repl = char
        self[item] = repl
        return repl


def commandline_parser():
    parser = ap.ArgumentParser(
        description="""Export a network and a scenrio to a set of files, which
can be imported into a GAMS model.

Written by Philipp Meier <philipp@diemeiers.ch>
(c) Copyright 2014, Univeristy of Manchester.
        """, epilog="For more information visit www.hydraplatform.com",
        formatter_class=ap.RawDescriptionHelpFormatter)
    # Mandatory arguments
    #parser.add_argument('-p', '--project',
    #                    help='''ID of the project that will be exported.''')
    parser.add_argument('-t', '--network',
                        help='''ID of the network that will be exported.''')
    parser.add_argument('-s', '--scenario',
                        help='''ID of the scenario that will be exported.''')
    parser.add_argument('-tp', '--template-id',
                        help='''ID of the template to be used.''')
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
    parser.add_argument('-tx', '--time-axis', nargs='+',
                        help='''Time axis for the modelling period (a list of
                        comma separated time stamps).''')
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

    if args.template_id is not None:
        exporter.template_id = int(args.template_id)

    if args.group_nodes_by is not None:
        for ngroup in args.group_nodes_by:
            exporter.network.create_node_groups(ngroup)

    if args.group_links_by is not None:
        for lgroup in args.group_links_by:
            exporter.network.create_link_groups(lgroup)

    exporter.export_network()

    if args.start_date is not None and args.end_date is not None \
            and args.time_step is not None:
        exporter.write_time_index(start_time=args.start_date,
                                  end_time=args.end_date,
                                  time_step=args.time_step)
    elif args.time_axis is not None:
        exporter.write_time_index(time_axis=args.time_axis)
    else:
        raise HydraPluginError('Time axis not specified.')
    exporter.export_data()

    exporter.write_file()
