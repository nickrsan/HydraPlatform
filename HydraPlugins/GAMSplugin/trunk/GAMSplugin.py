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

Input data
----------

Constraints
-----------

Output data
-----------


Options
~~~~~~~

--start-time
--end-time

--group-by-attribute (to create subsets)

--export-results-to-excel

TODO
----

- Allow user to supply a list of time stamps instead of start time, end time,
  time step.

API docs
~~~~~~~~
"""

import argparse as ap
import re
from datetime import datetime
from datetime import timedelta

from HydraLib import PluginLib
from HydraLib import units
from string import ascii_lowercase


# A set of classes to facilitate import and export from and to GAMS
class GAMSresource(object):
    """
    """
    def __init__(self):
        self.name = None
        self.ID = None
        self.attributes = []
        self.groups = []
        self.object_type = ''

    def add_attribute(self, attr, res_attr, res_scen):
        attribute = GAMSattribute(attr, res_attr, res_scen)

        self.attributes.append(attribute)

    def delete_attribute(self, attribute):
        idx = self.attributes.index(attribute)
        del self.attributes[idx]

    def get_attribute(self, attr_name=None, attr_id=None):

        if attr_name is not None:
            return self._get_attr_by_name(attr_name)
        elif attr_id is not None:
            return self._get_attr_by_id(attr_id)

    def group(self, group_attr):
        attr = self._get_attr_by_name(group_attr)
        if attr is not None:
            group = attr.value.__getitem__(0)
            self.groups.append(group)
            # The attribute is used for grouping and will not be exported
            self.delete_attribute(attr)
            return group

    def set_object_type(self, type_attr):
        attr = self._get_attr_by_name(type_attr)
        self.object_type = attr.value.__getitem__(0)
        return self.object_type

    def _get_attr_by_name(self, attr_name):
        for attr in self.attributes:
            if attr.name == attr_name:
                return attr

    def _get_attr_by_id(self, attr_id):
        for attr in self.attributes:
            if attr.attr_id == attr_id:
                return attr


class GAMSnetwork(GAMSresource):
    """
    """

    description = None
    scenario_id = None
    nodes = []
    links = []
    node_types = []
    node_groups = []
    link_types = []
    link_groups = []

    def load(self, soap_net, soap_attrs):

        # load network
        resource_scenarios = dict()
        for res_scen in \
                soap_net.scenarios.Scenario[0].resourcescenarios.ResourceScenario:
            resource_scenarios.update({res_scen.resource_attr_id: res_scen})
        attributes = dict()
        for attr in soap_attrs.Attr:
            attributes.update({attr.id: attr})

        self.name = soap_net.name
        self.ID = soap_net.id
        self.description = soap_net.description
        self.scenario_id = soap_net.scenarios.Scenario[0].id

        if soap_net.attributes is not None:
            for res_attr in soap_net.attributes.Attr:
                self.add_attribute(attributes[res_attr.attr_id],
                                   res_attr,
                                   resource_scenarios[res_attr.id])

        # load nodes
        for node in soap_net.nodes.Node:
            new_node = GAMSnode()
            new_node.ID = node.id
            new_node.name = node.name
            new_node.gams_name = node.name
            if node.attributes is not None:
                for res_attr in node.attributes.ResourceAttr:
                    if res_attr.id in resource_scenarios.keys():
                        new_node.add_attribute(attributes[res_attr.attr_id],
                                               res_attr,
                                               resource_scenarios[res_attr.id])
                    else:
                        new_node.add_attribute(attributes[res_attr.attr_id],
                                               res_attr,
                                               None)

            self.add_node(new_node)
            del new_node

        # load links
        for link in soap_net.links.Link:
            new_link = GAMSlink()
            new_link.ID = link.id
            new_link.name = link.name
            new_link.from_node = self.get_node(node_id=link.node_1_id).name
            new_link.to_node = self.get_node(node_id=link.node_2_id).name
            new_link.gams_name = new_link.from_node + ' . ' + new_link.to_node
            if link.attributes is not None:
                for res_attr in link.attributes.ResourceAttr:
                    new_link.add_attribute(attributes[res_attr.attr_id],
                                           res_attr,
                                           resource_scenarios[res_attr.id])
            self.add_link(new_link)

    def add_node(self, node):
        self.nodes.append(node)

    def delete_node(self, node):
        pass

    def get_node(self, node_name=None, node_id=None, node_type=None, group=None):
        if node_name is not None:
            return self._get_node_by_name(node_name)
        elif node_id is not None:
            return self._get_node_by_id(node_id)
        elif node_type is not None:
            return self._get_node_by_type(node_type)
        elif group is not None:
            return self._get_node_by_group(group)

    def add_link(self, link):
        self.links.append(link)

    def delete_link(self, link):
        pass

    def get_link(self, link_name=None, link_id=None, link_type=None, group=None):
        if link_name is not None:
            return self._get_link_by_name(link_name)
        elif link_id is not None:
            return self._get_link_by_id(link_id)
        elif link_type is not None:
            return self._get_link_by_type(link_type)
        elif group is not None:
            return self._get_link_by_group(group)

    def set_node_type(self, attr_name):
        for i, node in enumerate(self.nodes):
            object_type = self.nodes[i].set_object_type(attr_name)
            attr = self.nodes[i].get_attribute(attr_name=attr_name)
            self.nodes[i].delete_attribute(attr)
            if object_type not in self.node_types:
                self.node_types.append(object_type)

    def set_link_type(self, attr_name):
        for i, link in enumerate(self.links):
            object_type = self.links[i].set_object_type(attr_name)
            attr = self.links[i].get_attribute(attr_name=attr_name)
            self.links[i].delete_attribute(attr)
            if object_type not in self.link_types:
                self.link_types.append(object_type)

    def create_node_groups(self, group_attr):
        for i, node in enumerate(self.nodes):
            group = self.nodes[i].group(group_attr)
            if group is not None and group not in self.node_groups:
                self.node_groups.append(group)

    def create_link_groups(self, group_attr):
        for i, link in enumerate(self.links):
            group = self.links[i].group(group_attr)
            if group is not None and group not in self.link_groups:
                self.link_groups.append(group)

    def _get_node_by_name(self, name):
        for node in self.nodes:
            if node.name == name:
                return node

    def _get_node_by_id(self, ID):
        for node in self.nodes:
            if node.ID == ID:
                return node

    def _get_node_by_type(self, node_type):
        nodes = []
        for node in self.nodes:
            if node.object_type == node_type:
                nodes.append(node)
        return nodes

    def _get_node_by_group(self, node_group):
        nodes = []
        for node in self.nodes:
            if node_group not in node.groups:
                nodes.append(node)
        return nodes

    def _get_link_by_name(self, name):
        for link in self.links:
            if link.name == name:
                return link

    def _get_link_by_id(self, ID):
        for link in self.links:
            if link.ID == ID:
                return link

    def _get_link_by_type(self, link_type):
        links = []
        for link in self.links:
            if link.object_type == link_type:
                links.append(link)
        return links

    def _get_link_by_group(self, link_group):
        links = []
        for link in self.links:
            if link_group not in link.groups:
                links.append(link)
        return links


class GAMSnode(GAMSresource):
    pass


class GAMSlink(GAMSresource):

    gams_name = None
    from_node = None
    to_node = None


class GAMSattribute(object):

    name = None

    attr_id = None
    resource_attr_id = None
    is_var = False

    dataset_id = None
    dataset_type = ''

    value = None

    def __init__(self, attr, res_attr, res_scen):
        self.name = attr.name
        self.attr_id = attr.id
        self.resource_attr_id = res_attr.id
        if res_scen is None:
            self.is_var = True
        else:
            self.dataset_id = res_scen.value.id
            self.dataset_type = res_scen.value.type
            self.value = res_scen.value.value


class GAMSimport(object):
    pass


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
                    's) /\n          '
            else:
                self.output += 'Table ' + obj_type + '_' + datatype + \
                    '_data(i,' + obj_type + '_' + datatype + 's) /\n          '

            for attribute in attributes:
                self.output += '%16s' % attribute.name
            self.output += '\n'
            for resource in resources:
                if islink:
                    self.output += '%14s' % resource.gams_name
                else:
                    self.output += '%14s' % resource.name
                for attribute in attributes:
                    attr = resource.get_attribute(attr_name=attribute.name)
                    self.output += '%14s' % attr.value.__getitem__(0)
                self.output += '\n'
            self.output += '/\n\n'

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
                    '_timeseries) /\n          '
            else:
                self.output += 'Table ' + obj_type + \
                    '_timeseries_data(t,i,' + obj_type + \
                    '_timeseries) /\n          '
            for attribute in attributes:
                for resource in resources:
                    if islink:
                        self.output += '%10s' % (resource.gams_name + '.' +
                                                 attribute.name)
                    else:
                        self.output += '%10s' % (resource.name + '.' +
                                                 attribute.name)
            self.output += '\n'

            for t in self.time_index:
                self.output += '%10s' % self.convert_date_to_timeindex(t)
                for attribute in attributes:
                    for resource in resources:
                        attr = resource.get_attribute(attr_name=attribute.name)
                        #data = self.cli.service.get_val_at_time(
                        #    attr.dataset_id, PluginLib.date_to_string(t))
                        data = 1.0
                        self.output += '%10s' % data
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
                        self.output += ') /\n'
                        arr_index = create_arr_index(dim)
                        vec_array = arr_to_vector(array, dim)
                        for n, idx in enumerate(arr_index):
                            self.output += ' . '.join([str(i) for i in idx])
                            self.output += '   %s\n' % vec_array[n]
                        self.output += '/\n\n'

    def write_time_index(self, start_time, end_time, time_step):
        start_time = ' '.join(start_time)
        end_time = ' '.join(end_time)
        start_date = self.parse_date(start_time)
        end_date = self.parse_date(end_time)
        delta_t = self.parse_time_step(time_step)

        self.output += 'SETS\n\n'
        self.output += '* Time steps\n'
        self.output += 't time /\n'
        while start_date < end_date:

            self.output += str(self.convert_date_to_timeindex(start_date)) \
                + '\n'

            self.time_index.append(start_date)

            start_date += timedelta(delta_t)

        self.output += '/\n\n'

    def convert_date_to_timeindex(self, date):
        totalseconds = date.hour * 3600 + date.minute * 60 + date.second
        return date.toordinal() + float(totalseconds) / 86400

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


def array_dim(arr):
    dim = []
    while True:
        try:
            dim.append(len(arr))
            arr = arr[0]
        except TypeError:
            return dim


def arr_to_vector(arr, dim):
    tmp_arr = []
    for n in range(len(dim) - 1):
        for inner in arr:
            for i in inner:
                tmp_arr.append(i)
        arr = tmp_arr
        tmp_arr = []
    return arr


def create_arr_index(dim):
    arr_idx = []
    L = 1
    for d in dim:
        L *= d

    for l in range(L):
        arr_idx.append(())

    K = 1
    for d in dim:
        L = L / d
        n = 0
        for k in range(K):
            for i in range(d):
                for l in range(L):
                    arr_idx[n] += (i,)
                    n += 1
        K = K * d

    return arr_idx


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
