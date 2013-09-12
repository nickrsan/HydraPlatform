#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A Hydra plug-in for importing CSV files.

Basics
~~~~~~

The plug-in for importing CSV files assumes that you have a collection of files
ready. You need one or several files for nodes (usually one file per node type)
and files for links (usually on file per link type). One node file and one link
file are mandatory, additional files are optional. The plug-in also allows you
to import network attributes.

   Basic usage::

      usage: ImportCSV.py [-h] [-p PROJECT] [-s SCENARIO] [-t NETWORK]
                          -n NODES [NODES ...] -l LINKS [LINKS ...]
                          [-r RULES [RULES ...]] [-x]

Options
~~~~~~~

====================== ====== ========== =========================================
Option                 Short  Parameter  Description
====================== ====== ========== =========================================
``--help``             ``-h``            show help message and exit.
``--project``          ``-p`` PROJECT    The ID of an existing project. If no
                                         project is specified or if the ID
                                         provided does not belong to an existing
                                         project, a new one will be created.
``--scenario``         ``-s`` SCENARIO   Specify the name of the scenario created
                                         by the import function. Every import
                                         creates a new scenario. If no name is
                                         provided a default name will be assigned.
``--network``          ``-t`` NETWORK    Specify the file containing network
                                         information. If no file is specified, a
                                         new network will be created using default
                                         values.
``--nodes``            ``-n`` NODES      One or multiple files containing nodes
                                         and attributes.
``--links``            ``-l`` LINKS      One or multiple files containing
                                         information on links.
``--rules``            ``-l`` RULES      File(s) containing rules or constraints
                                         as mathematical expressions.
``--expand-filenames`` ``-x``            If the import function encounters
                                         something that looks like a filename, it
                                         tries to read the file.
====================== ====== ========== =========================================


File structure
~~~~~~~~~~~~~~

In the node and link file a minimum of information has to be provided in order
to be able to import a complete network. Optionally the files can define any
number of attributes for nodes and links.

For nodes a valid file looks like this::

    Name , x, y, attribute_1, attribute_2, ..., attribute_n, description
    Units,  ,  ,           m,    m^3 s^-1, ...,           -,
    node1, 2, 1,         4.0,      3421.9, ...,  Crop: corn, Irrigation node 1
    node2, 2, 3,         2.4,       988.4, ...,  Crop: rice, Irrigation node 2

For links, the following is a valid file::

    Name ,       from,       to, attribute_1, ..., attribute_n, description
    Units,           ,         ,           m, ...,    m^2 s^-1,
    link1,      node1,    node2,         453, ...,        0.34, Water transfer

.. note::

   If you specify a header line using the keywords ``name``, ``x``, ``y`` and
   ``description`` (``name``, ``from``, ``to`` and ``description`` for links)
   the order of the columns does not matter. If you don't specify these
   keywords, the plug-in will assume that the first column specifies the name,
   the second X, the third Y and the last the description (name, from, to and
   description for links).

Please also consider the following:

- A link references to start and end node by name. This also implies that the
  node names defined in the file need to be unique.

- The description should be in the last column. This will lead to more readable
  files, since the description is usually free form text.

- If you specify more than one file containing nodes, common attributes (i.e.
  with the same name) will be considered the same. This results in a unique
  attribute set for nodes. The same applies for links.


TODO
----

- Implement rules and constraints

- Implement file name expansion and import of time series and arrays"""

import argparse as ap
import logging
from datetime import datetime

import PluginLib

from suds import WebFault


class ImportCSV(object):
    """"""

    Project = None
    Network = None
    Scenario = None
    Nodes = dict()
    Links = dict()
    NetworkAttributes = dict()
    NodeAttributes = dict()
    LinkAttributes = dict()

    def __init__(self):
        self.cli = PluginLib.connect()
        self.node_id = self.temp_ids()
        self.link_id = self.temp_ids()
        self.attr_id = self.temp_ids()

    def create_project(self, ID=None):
        if ID is not None:
            try:
                ID = int(ID)
                try:
                    self.Project = self.cli.service.get_project(ID)
                except WebFault:
                    logging.info('Project ID not found. Creating new project')

            except ValueError:
                logging.info('Project ID not valid. Creating new project')

        self.Project = self.cli.factory.create('hyd:Project')
        self.Project.name = "CSV import"
        self.Project.description = \
            "Project created by the %s plug-in, %s." % \
            (self.__class__.__name__, datetime.now())
        self.Project = self.cli.service.add_project(self.Project)

    def create_scenario(self, name=None):
        self.Scenario = self.cli.factory.create('hyd:Scenario')
        if name is not None:
            self.Scenario.name = name
        else:
            self.Scenario.name = 'CSV import'

        self.Scenario.description = \
            'Default scenario created by the CSV import plug-in.'
        self.Scenario.id = -1

    def create_network(self, file=None):

        self.Network = self.cli.factory.create('hyd:Network')
        self.Network.project_id = self.Project.id
        self.Network.nodes = self.cli.factory.create('hyd:NodeArray')
        self.Network.links = self.cli.factory.create('hyd:LinkArray')
        self.Network.scenarios = self.cli.factory.create('hyd:ScenarioArray')

        if file is not None:
            with open(file, mode='r') as csv_file:
                self.net_data = csv_file.read()
            keys = self.net_data.split('\n', 1)[0].split(',')
            # A network file should only have one line of data
            data = self.net_data.split('\n')[1].split(',')

            # We assume a standard order of the network information (Name,
            # Description, attributes,...).
            name_idx = 0
            desc_idx = 1
            # If the file does not follow the standard, we can at least try to
            # guess what is stored where.
            for i, key in enumerate(keys):
                if key.lower().strip() == 'name':
                    name_idx = i
                elif key.lower().strip() == 'description':
                    desc_idx = i

            # Assign name and description
            self.Network.name = data[name_idx].strip()
            self.Network.description = data[desc_idx].strip()

            # Everything that is not name or description is an attribute
            attrs = dict()

            for i, key in enumerate(keys):
                if i != name_idx and i != desc_idx:
                    attrs.update({i: key.strip()})

            self.Network = self.add_data(self.Network, attrs, data)

        else:
            self.Network.name = "CSV import"
            self.Network.description = \
                "Network created by the %s plug-in, %s." % \
                (self.__class__.__name__, datetime.now())

    def read_nodes(self, file):
        with open(file, mode='r') as csv_file:
            self.node_data = csv_file.read()
        self.create_nodes()

    def create_nodes(self):
        keys = self.node_data.split('\n', 1)[0].split(',')
        units = self.node_data.split('\n')[1].split(',')
        data = self.node_data.split('\n')[2:-1]

        name_idx = 0
        desc_idx = -1  # Should be the last one
        x_idx = 1
        y_idx = 2
        # Guess parameter position:
        for i, key in enumerate(keys):
            if key.lower().strip() == 'name':
                name_idx = i
            elif key.lower().strip() == 'description':
                desc_idx = i
            elif key.lower().strip() == 'x':
                x_idx = i
            elif key.lower().strip() == 'y':
                y_idx = i

        attrs = dict()

        for i, key in enumerate(keys):
            if i != name_idx and i != desc_idx and i != x_idx and i != y_idx:
                attrs.update({i: key.strip()})

        for line in data:
            linedata = line.split(',')
            node = self.cli.factory.create('hyd:Node')
            node.id = self.node_id.next()
            node.name = linedata[name_idx].strip()
            node.description = linedata[desc_idx].strip()
            try:
                node.x = float(linedata[x_idx])
            except ValueError:
                node.x = 0
                logging.info('X coordinate of node %s is not a number.'
                             % node.name)
            try:
                node.y = float(linedata[y_idx])
            except ValueError:
                node.y = 0
                logging.info('Y coordinate of node %s is not a number.'
                             % node.name)

            node = self.add_data(node, attrs, linedata, units=units)

            self.Nodes.update({node.name: node})
            self.Network.nodes.Node.append(node)

    def read_links(self, file):
        with open(file, mode='r') as csv_file:
            self.link_data = csv_file.read()
        self.create_links()

    def create_links(self):
        keys = self.link_data.split('\n', 1)[0].split(',')
        units = self.link_data.split('\n')[1].split(',')
        data = self.link_data.split('\n')[2:-1]

        name_idx = 0
        desc_idx = -1  # Should be the last one
        from_idx = 1
        to_idx = 2
        # Guess parameter position:
        for i, key in enumerate(keys):
            if key.lower().strip() == 'name':
                name_idx = i
            elif key.lower().strip() == 'description':
                desc_idx = i
            elif key.lower().strip() == 'from':
                from_idx = i
            elif key.lower().strip() == 'to':
                to_idx = i

        attrs = dict()

        for i, key in enumerate(keys):
            if i != name_idx and i != desc_idx and \
                    i != from_idx and i != to_idx:
                attrs.update({i: key.strip()})

        for line in data:
            linedata = line.split(',')
            link = self.cli.factory.create('hyd:Link')
            link.id = self.link_id.next()
            link.name = linedata[name_idx].strip()
            link.description = linedata[desc_idx].strip()

            try:
                fromnode = self.Nodes[linedata[from_idx].strip()]
                tonode = self.Nodes[linedata[to_idx].strip()]
                link.node_1_id = fromnode.id
                link.node_2_id = tonode.id

                link = self.add_data(link, attrs, linedata, units=units)
                self.Network.links.Link.append(link)

            except KeyError:
                logging.info(('Start or end node not found (%s -- %s).' +
                             ' No link created.') %
                             (linedata[from_idx].strip(),
                              linedata[to_idx].strip()))

    def create_attribute(self, name, dimension=None):
        attribute = self.cli.factory.create('hyd:Attr')
        attribute.name = name
        attribute.dimen = dimension
        attribute = self.cli.service.add_attribute(attribute)

        return attribute

    def add_data(self, resource, attrs, data, units=None):
        '''Add the data read for each resource to the resource. This requires
        creating the attributes, resource attributes and a scenario which holds
        the data.'''

        # Guess resource type
        res_type = str(resource).split('{', 1)[0]

        # Create resource attributes and data coollection
        res_attr_array = \
            self.cli.factory.create('hyd:ResourceAttrArray')

        for i in attrs.keys():
            # Create the attributes if the do not exist already
            if res_type == '(Network)':
                if attrs[i] in self.NetworkAttributes.keys():
                    attribute = self.NetworkAttributes[attrs[i]]
                else:
                    attribute = self.create_attribute(attrs[i])
                    self.NetworkAttributes.update({attrs[i]: attribute})

            elif res_type == '(Node)':
                if attrs[i] in self.NodeAttributes.keys():
                    attribute = self.NodeAttributes[attrs[i]]
                else:
                    attribute = self.create_attribute(attrs[i])
                    self.NodeAttributes.update({attrs[i]: attribute})

            elif res_type == '(Link)':
                if attrs[i] in self.LinkAttributes.keys():
                    attribute = self.LinkAttributes[attrs[i]]
                else:
                    attribute = self.create_attribute(attrs[i])
                    self.LinkAttributes.update({attrs[i]: attribute})

            else:
                logging.critical('Unknown resource type.')

            res_attr = self.cli.factory.create('hyd:ResourceAttr')
            res_attr.id = self.attr_id.next()
            res_attr.attr_id = attribute.id
            #self.Scenario.attributes.ResourceAttr.append(res_attr)
            resource.attributes.ResourceAttr.append(res_attr)

            # create dataset and assign to attribute
            if len(data[i]) > 0:
                dataset = self.create_dataset(data[i], res_attr)
                self.Scenario.resourcescenarios.ResourceScenario.append(dataset)

        #resource.attributes = res_attr_array

        return resource

    def read_constraints(self):
        pass

    def create_dataset(self, value, resource_attr):
        dataset = self.cli.factory.create('hyd:ResourceScenario')

        dataset.attr_id = resource_attr.attr_id
        dataset.resource_attr_id = resource_attr.id

        try:
            float(value)
            dataset.type = 'scalar'
            scal = self.create_scalar(value)
            dataset.value = scal
        except ValueError:
            dataset.type = 'descriptor'
            desc = self.create_descriptor(value)
            dataset.value = desc

        return dataset

    def create_scalar(self, value):
        scalar = self.cli.factory.create('hyd:Scalar')
        scalar.param_value = value
        return scalar

    def create_descriptor(self, value):
        descriptor = self.cli.factory.create('hyd:Descriptor')
        descriptor.desc_val = value
        return descriptor

    def commit(self):
        self.Network.scenarios.Scenario.append(self.Scenario)
        self.Network = self.cli.service.add_network(self.Network)

    def temp_ids(self, n=-1):
        while True:
            yield n
            n -= 1


def commandline_parser():
    parser = ap.ArgumentParser()
    parser.add_argument('-p', '--project',
                        help='''The ID of an existing project. If no project is
                        specified or if the ID provided does not belong to an
                        existing project, a new one will be created.''')
    parser.add_argument('-s', '--scenario',
                        help='''Specify the name of the scenario created by the
                        import function. Every import creates a new scenario.
                        If no name is provided a default name will be assigned.
                        ''')
    parser.add_argument('-t', '--network',
                        help='''Specify the file containing network
                        information. If no file is specified, a new network
                        will be created using default values.''')
    parser.add_argument('-n', '--nodes', nargs='+',
                        help='''One or multiple files containing nodes and
                        attributes.''')
    parser.add_argument('-l', '--links', nargs='+',
                        help='''One or multiple files containing information
                        on links.''')
    parser.add_argument('-r', '--rules', nargs='+',
                        help='''File(s) containing rules or constraints as
                        mathematical expressions.''')
    parser.add_argument('-x', '--expand-filenames', action='store_true',
                        help='''If the import function encounters something
                        that looks like a filename, it tries to read the file.
                        It also tries to guess if it contains a number, a
                        descriptor, an array or a time series.''')
    return parser


if __name__ == '__main__':
    parser = commandline_parser()
    args = parser.parse_args()
    csv = ImportCSV()

    if args.nodes is not None:
        # Create project and network only when there is actual data to import.
        csv.create_project(ID=args.project)
        csv.create_scenario(name=args.scenario)
        csv.create_network(file=args.network)

        for nodefile in args.nodes:
            csv.read_nodes(nodefile)

        if args.links is not None:
            for linkfile in args.links:
                csv.read_links(linkfile)

        csv.commit()

    else:
        logging.info('No nodes found. Nothing imported.')
