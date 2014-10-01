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

       SamplePlugin.py [-h] -n NODES [NODES ...] -l LINKS [LINKS ...]
                    [-u SERVER URL] [-c SESSION ID]

Options
~~~~~~~

====================== ====== ============ =======================================
Option                 Short  Parameter    Description
====================== ====== ============ =======================================
``--help``             ``-h``              show help message and exit.
``--network-name``     ``-t`` NETWORK NAME The name of the network
``--nodes``            ``-n`` NODES        One or multiple files containing nodes
                                           and attributes.
``--links``            ``-l`` LINKS        One or multiple files containing
                                           information on links.
``--server-url``       ``-u`` SERVER URL   URL of the server. If not specified,
                                           a default from config is used
``--session-id``       ``-c`` SESSION ID   If user has already logged in, avoid
                                           doing it again by providing a valid
                                           session ID.
====================== ====== ============ =======================================


File structure
~~~~~~~~~~~~~~

In the node and link file a minimum of information has to be provided in order
to be able to import a complete network. Optionally the files can define any
number of attributes for nodes and links.

For nodes a valid file looks like this::

    Name , x, y,  attribute_1, attribute_2, ..., attribute_n, description
    Units,  ,  ,            m,    m^3 s^-1, ...,           -,
    node1, 2, 1,          4.0,      3421.9, ...,  Crop: corn, Irrigation 1
    node2, 2, 3,          2.4,       988.4, ...,  Crop: rice, Irrigation 2

For links, the following is a valid file::

    Name ,       from,    to,  attre_1, ...,  attre_n, description
    Units,           ,      ,        m, ..., m^2 s^-1,
    link1,      node1, node2,      453, ...,     0.34, Water transfer

It is optional to supply a network file. If you decide to do so, it needs to
follow this structure::

    # A test network created as a set of CSV files
    ID, Name            , attribute_1, ..., Description
    Units,              ,            ,    ,
    1 , My first network, test       ,    , Network created from CSV files


Lines starting with the ``#`` character are ignored.

.. note::

   If you specify a header line using the keywords ``name``, ``x``, ``y`` and
   ``description`` (``name``, ``from``, ``to`` and ``description`` for links)
   the order of the columns does not matter. If you don't specify these
   keywords, the plug-in will assume that the first column specifies the name,
   the second X, the third Y and the last the description (name, from, to and
   description for links).

.. note::

Please also consider the following:

- A link references to start and end node by name. This also implies that the
  node names defined in the file need to be unique.

- The description should be in the last column. This will lead to more readable
  files, since the description is usually free form text.

- If you specify more than one file containing nodes, common attributes (i.e.
  with the same name) will be considered the same. This results in a unique
  attribute set for nodes. The same applies for links.

- An attribute to a node or link is only added if there is a value for that
  specific attribute in the line specifying a node (or link). If an attribute
  should be added as a variable, you need to enter ``NULL``.

API docs
~~~~~~~~
"""

import argparse as ap
import logging
import os, sys
from datetime import datetime

from HydraLib import PluginLib
from HydraLib.PluginLib import JsonConnection, write_progress, write_output, validate_plugin_xml

from HydraLib.HydraException import HydraPluginError

log = logging.getLogger(__name__)

__location__ = os.path.split(sys.argv[0])[0]

class SamplePlugin(object):
    """
    """

    def __init__(self, url=None, session_id=None):

        self.Attributes = dict()

        #These are used to keep track of whether
        #duplicate names have been specified in the files.
        self.link_names = []
        self.node_names = []

        self.basepath = ''

        self.message = ''
        self.files = []
        self.warnings = []

        self.num_steps = 4

        self.connection = JsonConnection(url)
        if session_id is not None:
            log.info("Using existing session %s", session_id)
            self.connection.session_id=session_id
        else:
            self.connection.login()

        self.node_id = PluginLib.temp_ids()
        self.link_id = PluginLib.temp_ids()
        self.attr_id = PluginLib.temp_ids()

    def get_file_data(self, file):
        """
            Taking a csv file as an argument,
            return an array where each element is a line in the csv.
        """
        file_data=None
        if file == None:
            log.warn("No file specified")
            return None
        self.basepath = os.path.dirname(os.path.realpath(file))
        with open(file, mode='r') as csv_file:
            file_data = csv_file.read().split('\n')
            if len(file_data) == 0:
                log.warn("File contains no data")
        # Ignore comments

        bad_lines = []
        for i, line in enumerate(file_data):
            try:
                line = ''.join([x if ord(x) < 128 else ' ' for x in line])
                line.decode('ascii')
            except UnicodeDecodeError, e:
                #If there are unknown characters in this line, save the line
                #and the column in the line where the bad character has occurred.
                bad_lines.append((i+1, e.start))

            if len(line) > 0 and line.strip()[0] == '#':
                file_data.pop(i)

        #Complain about the lines that the bad characters are on.
        if len(bad_lines) > 0:
            lines = [a[0] for a in bad_lines]
            raise HydraPluginError("Lines %s, in %s contain non ascii characters"%(lines, file))

        return file_data

    def create_project(self):
        project_dict = dict(
            name = "CSV import at %s" % (datetime.now()),
            description = "Project created by the Sample plug-in, %s." % (datetime.now()),
            status = 'A',
        )
        project = self.connection.call('add_project', {'project':project_dict})
        return project

    def read_nodes(self, file):
        node_file_data = self.get_file_data(file)

        nodes = []
        node_data = []
        
        #The first row is the headings (attribute names and node parameters)
        headings  = node_file_data[0].split(',')
        #The second row is units
        units = node_file_data[1].split(',')
        #The third row is data
        data = node_file_data[2:-1]

        for i, unit in enumerate(units):
            units[i] = unit.strip()

        #If the heading is not in the reserved list, it must be an
        #attribute
        reserved_headings = ['name', 'description', 'x', 'y', 'type']
        # Guess parameter position:
        attrs = dict()
        #Records which are the reserved heading columns
        heading_idx = dict()
        for i, key in enumerate(headings):

            if key.lower().strip() in reserved_headings:
                heading_idx[key.lower().strip()] = i
            else:
                attrs[i] = key.strip()

        self.add_attributes(attrs, units)
        
        for line in data:
            linedata = line.split(',')
            nodename = linedata[heading_idx['name']].strip()

            if nodename in self.node_names:
                raise HydraPluginError("Duplicate Node name: %s"%(nodename))
            else:
                self.node_names.append(nodename)

            node = dict(
                id = self.node_id.next(),
                name = nodename,
                description = linedata[heading_idx['description']].strip(),
                attributes = [],
            )
            try:
                node['x'] = str(linedata[heading_idx['x']])
            except ValueError:
                node['x'] = 0
                log.info('X coordinate of node %s is not a number.'
                             % node['name'])
                self.warnings.append('X coordinate of node %s is not a number.'
                                     % node['name'])
            try:
                node['y'] = str(linedata[heading_idx['y']])
            except ValueError:
                node['y'] = 0
                log.info('Y coordinate of node %s is not a number.'
                             % node['name'])
                self.warnings.append('Y coordinate of node %s is not a number.'
                                     % node['name'])

            if len(attrs) > 0:
                attributes, data = self.add_data(nodename, attrs, linedata, units=units)
                node['attributes'] = attributes

            nodes.append(node)
            node_data.extend(data)

        return nodes, node_data

    def read_links(self, file, nodes):
        link_file_data = self.get_file_data(file)
       
        links = []
        link_data = []

        #Make a dictionary, keyed on the node name
        #to make accessing the correct node a bit easier.
        node_dict = {}
        for n in nodes:
            node_dict[n['name']] = n
        
        #The first row is the headings
        headings = link_file_data[0].split(',')
        #The second row is units
        units = link_file_data[1].split(',')
        #The third row until the end is actual data.
        data = link_file_data[2:-1]

        for i, unit in enumerate(units):
            units[i] = unit.strip()

        reserved_headings = ['name', 'description', 'from', 'to', 'type']
        
        # Guess parameter position:
        attrs = dict()
        #Records which are the reserved heading columns
        heading_idx = dict()
        for i, key in enumerate(headings):

            if key.lower().strip() in reserved_headings:
                heading_idx[key.lower().strip()] = i
            else:
                attrs[i] = key.strip()

        self.add_attributes(attrs, units)

        for line in data:
            linedata = line.split(',')
            linkname = linedata[heading_idx['name']].strip()

            if linkname in self.link_names:
                raise HydraPluginError("Duplicate Link name: %s"%(linkname))
            else:
                self.link_names.append(linkname)

            link = dict(
                id = self.link_id.next(),
                name = linkname,
                description = linedata[heading_idx['description']].strip(),
                attributes = [], 
            )

            try:
                fromnode = node_dict[linedata[heading_idx['from']].strip()]
                tonode = node_dict[linedata[heading_idx['to']].strip()]
                link['node_1_id'] = fromnode['id']
                link['node_2_id'] = tonode['id']

            except KeyError:
                log.info(('Start or end node not found (%s -- %s).' +
                              ' No link created.') %
                             (linedata[heading_idx['from']].strip(),
                              linedata[heading_idx['to']].strip()))
                self.warnings.append(('Start or end node not found (%s -- %s).' +
                              ' No link created.') %
                             (linedata[heading_idx['from']].strip(),
                              linedata[heading_idx['to']].strip()))

            if len(attrs) > 0:
                attributes, data = self.add_data(linkname, attrs, linedata, units=units)
                link['attributes'] = attributes

            links.append(link)
            link_data.extend(data)
            
        return links, link_data

    def create_attribute(self, name, unit=None):
        """
            Create attribute locally. It will get added in bulk later.
        """
        try:
            attribute = dict(
                name = name.strip(),
            )
            if unit is not None and len(unit.strip()) > 0:
                attribute['dimen'] = self.connection.call('get_dimension', {'unit1':unit.strip()})
        except Exception,e:
            raise HydraPluginError("Invalid attribute %s %s: error was: %s"%(name,unit,e))

        return attribute

    def add_attributes(self, attrs, units):
        attributes = []
        for i in attrs.keys():
            if attrs[i] in self.Attributes.keys():
                attribute = self.Attributes[attrs[i]]
            else:
                if units is not None:
                    attribute = self.create_attribute(attrs[i], units[i])
                else:
                    attribute = self.create_attribute(attrs[i])
                self.Attributes.update({attrs[i]: attribute})
            attributes.append(attribute)

        attributes = self.connection.call('add_attributes', {'attrs':attributes})
        for attr in attributes:
            self.Attributes.update({attr['name']: attr})

    def add_data(self, resource_name, attrs, data, units=None):
        '''Add the data read for each resource to the resource. This requires
        creating the attributes, resource attributes and a scenario which holds
        the data.'''

        attributes = []
        resource_data       = []

        # Add data to each attribute
        for i in attrs.keys():
            attr = self.Attributes[attrs[i]]
            res_attr = dict( 
                id = self.attr_id.next(),
                attr_id = attr['id'],
                attr_is_var = 'N',
            )
            # create dataset and assign to attribute (if not empty)
            if len(data[i].strip()) > 0:

                attributes.append(res_attr)

                if data[i].strip() in ('NULL',
                                       'I AM NOT A NUMBER! I AM A FREE MAN!'):

                    res_attr['attr_is_var'] = 'Y'

                else:
                    unit      = None
                    #Are there units in the csv file?
                    if units is not None:
                        unit = units[i]
                        #Is there a unit specified for this attribute?
                        if unit is not None:
                            if unit.strip() == '':
                                unit = None

                    dataset = self.create_dataset(data[i],
                                                    res_attr,
                                                    unit,
                                                    attr.get('dimen'),
                                                    resource_name,
                                                    )
                    if dataset is not None:
                        resource_data.append(dataset)

        return attributes, resource_data

    def create_dataset(self, value, resource_attr, unit, dimension, resource_name):
 
        resourcescenario = dict()
        
        dataset          = dict(
            id=None,
            type=None,
            unit=unit,
            dimension=dimension,
            name='Import CSV data',
            value=None,
            hidden='N',
            metadata=None,
        )

        resourcescenario['attr_id'] = resource_attr['attr_id']
        resourcescenario['resource_attr_id'] = resource_attr['id']

        try:
            float(value)
            dataset['type'] = 'scalar'
            dataset['value'] = dict(param_value = str(value)) 
        except ValueError:
            dataset['type'] = 'descriptor'
            dataset['value'] = dict(desc_val = str(value))

        resourcescenario['value'] = dataset

        return resourcescenario

def commandline_parser():
    parser = ap.ArgumentParser(
        description="""Import a network saved in a set of CSV files into Hydra.

        Written by Philipp Meier <philipp@diemeiers.ch> and Stephen Knox <stephen.knox@manchester.ac.uk>
        (c) Copyright 2013, University of Manchester.

        """, epilog="For more information visit www.hydra-network.com",
        formatter_class=ap.RawDescriptionHelpFormatter)
    parser.add_argument('-t', '--network_name',
                        help='''The name of the network''')
    parser.add_argument('-n', '--nodes', nargs='+',
                        help='''One or multiple files containing nodes and
                        attributes.''')
    parser.add_argument('-l', '--links', nargs='+',
                        help='''One or multiple files containing information
                        on links.''')
    parser.add_argument('-u', '--server-url',
                        help='''Specify the URL of the server to which this
                        plug-in connects.''')
    parser.add_argument('-c', '--session_id',
                        help='''Session ID. If this does not exist, a login will be
                        attempted based on details in config.''')
    return parser


if __name__ == '__main__':
    parser = commandline_parser()
    args = parser.parse_args()
    example = SamplePlugin(url=args.server_url, session_id=args.session_id)

    scen_ids = []
    errors = []
    network_id=None
    try:
        
        validate_plugin_xml(os.path.join(__location__, 'plugin.xml'))

        if args.nodes is not None:
            # Create project and network only when there is actual data to
            # import.
            project = example.create_project()

            write_progress(1,example.num_steps) 
            for nodefile in args.nodes:
                write_output("Reading Node file %s" % nodefile)
                nodes, node_data = example.read_nodes(nodefile)

            write_progress(2,example.num_steps) 
            if args.links is not None:
                for linkfile in args.links:
                    write_output("Reading Link file %s" % linkfile)
                    links, link_data = example.read_links(linkfile, nodes)


            write_progress(3,example.num_steps) 
            scenario = dict( 
                name = 'Sample scenario',
                description='Default scenario created by the Sample CSV import plug-in.',
                resourcescenarios = node_data + link_data,
            )
            
            # Create a new network
            network = dict(
                project_id = project['id'],
                name = args.network_name,
                description = "Network created by the example plug-in.",
                nodes = nodes,
                links = links,
                scenarios = [scenario],
                resourcegroups = [],
                attributes = [],
            )
            
            write_output("Saving network")
            network = example.connection.call('add_network', {'net':network})
            
            if network['scenarios']:
                scen_ids = [s['id'] for s in network['scenarios']]

            network_id = network['id']
            
            scen_ids = [s['id'] for s in network['scenarios']]

            write_progress(4,example.num_steps)

        else:
            log.info('No nodes found. Nothing imported.')

	errors = []
    except HydraPluginError as e:
        errors = [e.message]

    xml_response = PluginLib.create_xml_response('SamplePlugin',
                                                 network_id,
                                                 scen_ids,
                                                 errors,
                                                 example.warnings,
                                                 example.message,
                                                 example.files)

    print xml_response
