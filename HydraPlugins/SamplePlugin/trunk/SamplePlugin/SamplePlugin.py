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

       ImportCSV.py [-h] -n NODES [NODES ...] -l LINKS [LINKS ...]
                    [-z TIMEZONE]
                    [-x]

Options
~~~~~~~

====================== ====== ============ =======================================
Option                 Short  Parameter    Description
====================== ====== ============ =======================================
``--help``             ``-h``              show help message and exit.
``--project``          ``-p`` PROJECT      The ID of an existing project. If no
                                           project is specified or if the ID
                                           provided does not belong to an existing
                                           project, a new one will be created.
``--nodes``            ``-n`` NODES        One or multiple files containing nodes
                                           and attributes.
``--links``            ``-l`` LINKS        One or multiple files containing
                                           information on links.
``--timezone``         ``-z`` TIMEZONE     Specify a timezone as a string
                                           following the Area/Loctation pattern
                                           (e.g.  Europe/London). This timezone
                                           will be used for all timeseries data
                                           that is imported. If you don't specify
                                           a timezone, it defaults to UTC.
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
import pytz

from HydraLib import PluginLib
from HydraLib.PluginLib import write_progress, write_output, validate_plugin_xml
from HydraLib import config, util

from HydraLib.HydraException import HydraPluginError,HydraError

import requests

import json
log = logging.getLogger(__name__)

__location__ = os.path.split(sys.argv[0])[0]

class ImportCSV(object):
    """
    """

    def __init__(self, url=None, session_id=None):

        self.url = url

        self.Attributes = dict()

        #These are used to keep track of whether
        #duplicate names have been specified in the files.
        self.link_names = []
        self.node_names = []

        self.timezone = pytz.utc
        self.basepath = ''

        self.add_attrs = True
        
        self.session_id=None
        if session_id is not None:
            log.info("Using existing session %s", session_id)
            self.session_id=session_id
        else:
            self.login()

        self.warnings = []
        self.message = ''
        self.files = []

        self.num_steps = 6

    def validate_value(self, value, restriction_dict):
        if restriction_dict is None or restriction_dict == {}:
            return

        try:
            util.validate_value(restriction_dict, value)
        except HydraError, e:
            raise HydraPluginError(e.message)

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

    def create_project(self, ID=None):
        project_dict = dict(
            name = "CSV import at %s" % (datetime.now()),
            description = "Project created by the Sample plug-in, %s." % (datetime.now()),
            status = 'A',
        )
        project = self.call('add_project', {'project':project_dict})
        return project

    def login(self):
        user = config.get('hydra_client', 'user')
        passwd = config.get('hydra_client', 'password')
        login_params = {'username':user, 'password':passwd}

        resp = self.call('login', login_params)
        #set variables for use in request headers
        self.session_id = resp['session_id']
        log.info("Session ID=%s", self.session_id)

    def call(self, func, args):
        log.info("Calling: %s"%(func))
        if self.url is None:
            port = config.getint('hydra_server', 'port', 8080)
            domain = config.get('hydra_server', 'domain', '127.0.0.1')
            self.url = "http://%s:%s/json"%(domain, port)
            log.info("Setting URL %s", self.url)
        call = {func:args}
        headers = {
                    'Content-Type': 'application/json',       
                    'session_id':self.session_id,
                    'app_name' : 'Import CSV'
                  }
        r = requests.post(self.url, data=json.dumps(call), headers=headers)
        if not r.ok:
            try:
                resp = json.loads(r.content)
                err = "%s:%s"%(resp['faultcode'], resp['faultstring'])
            except:
                err = r.content
            raise HydraPluginError(err)

        return json.loads(r.content) 

    def read_nodes(self, file):
        node_data = self.get_file_data(file)

        nodes = []
        
        self.add_attrs = True

        keys  = node_data[0].split(',')
        units = node_data[1].split(',')
        data = node_data[2:-1]

        for i, unit in enumerate(units):
            units[i] = unit.strip()

        field_idx = {'name': 0,
                     'description': -1,
                     'x': 1,
                     'y': 2,
                     'type': None,
                     }
        # Guess parameter position:
        attrs = dict()
        for i, key in enumerate(keys):

            if key.lower().strip() in field_idx.keys():
                field_idx[key.lower().strip()] = i
            else:
                attrs.update({i: key.strip()})

        node_names = []

        for line in data:
            linedata = line.split(',')
            nodename = linedata[field_idx['name']].strip()

            if nodename in self.node_names:
                raise HydraPluginError("Duplicate Node name: %s"%(nodename))
            else:
                node_names.append(nodename)

            if nodename in node_names:
                log.info('Node %s exists.' % nodename)
            else:
                node = dict(
                    id = self.node_id.next(),
                    name = nodename,
                    description = linedata[field_idx['description']].strip(),
                    attributes = [],
                )
                try:
                    node['x'] = str(linedata[field_idx['x']])
                except ValueError:
                    node['x'] = 0
                    log.info('X coordinate of node %s is not a number.'
                                 % node['name'])
                    self.warnings.append('X coordinate of node %s is not a number.'
                                         % node['name'])
                try:
                    node['y'] = str(linedata[field_idx['y']])
                except ValueError:
                    node['y'] = 0
                    log.info('Y coordinate of node %s is not a number.'
                                 % node['name'])
                    self.warnings.append('Y coordinate of node %s is not a number.'
                                         % node['name'])

            if len(attrs) > 0:
                node = self.add_data(node, attrs, linedata, units=units)

            nodes.append(node)

        return nodes

    def read_links(self, file, nodes):
        link_data = self.get_file_data(file)
        
        #Make a dictionary, keyed on the node name
        #to make accessing the correct node a bit easier.
        node_dict = {}
        for n in nodes:
            node_dict[n['name']] = n

        self.add_attrs = True

        keys = link_data[0].split(',')
        units = link_data[1].split(',')
        data = link_data[2:-1]

        for i, unit in enumerate(units):
            units[i] = unit.strip()

        field_idx = {'name': 0,
                     'description': -1,
                     'from': 1,
                     'to': 2,
                     'type': None,
                     }
        # Guess parameter position:
        attrs = dict()
        
        links = []
        
        #keep track of all the link names to make sure
        #there are no duplicates
        link_names = []
        
        for i, key in enumerate(keys):
            if key.lower().strip() in field_idx.keys():
                field_idx[key.lower().strip()] = i
            else:
                attrs.update({i: key.strip()})

        for line in data:
            linedata = line.split(',')
            linkname = linedata[field_idx['name']].strip()

            if linkname in self.link_names:
                raise HydraPluginError("Duplicate Link name: %s"%(linkname))
            else:
                link_names.append(linkname)

            if linkname in link_names:
                log.info('Link %s exists.' % linkname)
            else:
                link = dict(
                    id = self.link_id.next(),
                    name = linkname,
                    description = linedata[field_idx['description']].strip(),
                    attributes = [], 
                )

                try:
                    fromnode = node_dict[linedata[field_idx['from']].strip()]
                    tonode = node_dict[linedata[field_idx['to']].strip()]
                    link['node_1_id'] = fromnode['id']
                    link['node_2_id'] = tonode['id']

                except KeyError:
                    log.info(('Start or end node not found (%s -- %s).' +
                                  ' No link created.') %
                                 (linedata[field_idx['from']].strip(),
                                  linedata[field_idx['to']].strip()))
                    self.warnings.append(('Start or end node not found (%s -- %s).' +
                                  ' No link created.') %
                                 (linedata[field_idx['from']].strip(),
                                  linedata[field_idx['to']].strip()))

            if len(attrs) > 0:
                link = self.add_data(link, attrs, linedata, units=units)

            links.append(link)
            
        return links

    def create_attribute(self, name, unit=None):
        """
            Create attribute locally. It will get added in bulk later.
        """
        try:
            attribute = dict(
                name = name.strip(),
            )
            if unit is not None and len(unit.strip()) > 0:
                attribute['dimen'] = self.call('get_dimension', {'unit1':unit.strip()})
        except Exception,e:
            raise HydraPluginError("Invalid attribute %s %s: error was: %s"%(name,unit,e))

        return attribute

    def add_data(self, resource, attrs, data, metadata, units=None, restrictions={}):
        '''Add the data read for each resource to the resource. This requires
        creating the attributes, resource attributes and a scenario which holds
        the data.'''

        attributes = []

        # Collect existing resource attributes:
        resource_attrs = dict()

        if resource.get('attributes') is None:
            return resource

        for res_attr in resource['attributes']:
            resource_attrs.update({res_attr.attr_id: res_attr})

        for i in attrs.keys():
            if attrs[i] in self.Attributes.keys():
                attribute = self.Attributes[attrs[i]]
                #attribute = self.create_attribute(attrs[i])
            else:
                if units is not None:
                    attribute = self.create_attribute(attrs[i], units[i])
                else:
                    attribute = self.create_attribute(attrs[i])
                self.Attributes.update({attrs[i]: attribute})
            attributes.append(attribute)

        # Add all attributes. If they exist already, we retrieve the real id.
        # Also, we add the attributes only once (that's why we use the
        # add_attrs flag).
        if self.add_attrs:
            attributes = self.call('add_attributes', {'attrs':attributes})
            self.add_attrs = False
            for attr in attributes:
                self.Attributes.update({attr['name']: attr})

        # Add data to each attribute
        for i in attrs.keys():
            attr = self.Attributes[attrs[i]]
            # Attribute might already exist for resource, use it if it does
            if attr['id'] in resource_attrs.keys():
                res_attr = resource_attrs[attr.id]
            else:
                res_attr = dict( 
                    id = self.attr_id.next(),
                    attr_id = attr['id'],
                    attr_is_var = 'N',
                )
            # create dataset and assign to attribute (if not empty)
            if len(data[i].strip()) > 0:

                resource['attributes'].append(res_attr)

                if data[i].strip() in ('NULL',
                                       'I AM NOT A NUMBER! I AM A FREE MAN!'):

                    res_attr['attr_is_var'] = 'Y'

                else:
                    if units is not None:
                        if units[i] is not None and len(units[i].strip()) > 0:
                            dimension = attr['dimen']
                        else:
                            dimension = None

                        dataset = self.create_dataset(data[i],
                                                      res_attr,
                                                      units[i],
                                                      dimension,
                                                      resource['name'],
                                                      restrictions.get(attr['name'], {}).get('restrictions', {})
                                                     )
                    else:
                        dataset = self.create_dataset(data[i],
                                                      res_attr,
                                                      None,
                                                      None,
                                                      resource['name'],
                                                      restrictions.get(attr['name'], {}).get('restrictions', {})
                                                     )

                    if dataset is not None:
                        self.Scenario['resourcescenarios'].append(dataset)

        #resource.attributes = res_attr_array

        return resource

    def create_dataset(self, value, resource_attr, unit, dimension, resource_name, restriction_dict):
 
        resourcescenario = dict()
        
        dataset          = dict(
            id=None,
            type=None,
            unit=None,
            dimension=None,
            name='Import CSV data',
            value=None,
            locked='N',
            metadata=None,
        )

        resourcescenario['attr_id'] = resource_attr['attr_id']
        resourcescenario['resource_attr_id'] = resource_attr['id']

        value = value.strip()
        if unit is not None:
            unit = unit.strip()
            if len(unit) == 0:
                unit = None

        try:
            float(value)
            dataset['type'] = 'scalar'
            dataset['value'] = dict(param_value = str(value)) 
        except ValueError:
            dataset['type'] = 'descriptor'
            dataset['value'] = dict(desc_val = str(value))

        dataset['unit'] = unit
        if unit is not None:
            dataset['dimension'] = dimension

        resourcescenario['value'] = dataset

        return resourcescenario

    def create_network(self, network):
        log.info("Committing Network")
        
        log.info("Adding Network")
        log.info("Network created with %s nodes and %s links. Network ID is %s",
                 len(self.NetworkSummary['nodes']), 
                 len(self.NetworkSummary['links']),
                 self.NetworkSummary['id'])

        self.message = 'Data import was successful.'

    def return_xml(self):
        """
            This is a fist version of a possible XML output.
        """
        scen_ids = [s['id'] for s in self.NetworkSummary['scenarios']]

        xml_response = PluginLib.create_xml_response('ImportCSV',
                                                     self.Network['id'],
                                                     scen_ids)

        print xml_response

def commandline_parser():
    parser = ap.ArgumentParser(
        description="""Import a network saved in a set of CSV files into Hydra.

        Written by Philipp Meier <philipp@diemeiers.ch> and Stephen Knox <stephen.knox@manchester.ac.uk>
        (c) Copyright 2013, University of Manchester.

        """, epilog="For more information visit www.hydra-network.com",
        formatter_class=ap.RawDescriptionHelpFormatter)
    parser.add_argument('-p', '--project',
                        help='''The ID of an existing project. If no project is
                        specified or if the ID provided does not belong to an
                        existing project, a new one will be created.''')
    parser.add_argument('-n', '--nodes', nargs='+',
                        help='''One or multiple files containing nodes and
                        attributes.''')
    parser.add_argument('-l', '--links', nargs='+',
                        help='''One or multiple files containing information
                        on links.''')
    parser.add_argument('-z', '--timezone',
                        help='''Specify a timezone as a string following the
                        Area/Location pattern (e.g. Europe/London). This
                        timezone will be used for all timeseries data that is
                        imported. If you don't specify a timezone, it defaults
                        to UTC.''')
    parser.add_argument('-x', '--expand-filenames', action='store_true',
                        help='''If the import function encounters something
                        that looks like a filename, it tries to read the file.
                        It also tries to guess if it contains a number, a
                        descriptor, an array or a time series.''')
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
    csv = ImportCSV(url=args.server_url, session_id=args.session_id)

    scen_ids = []
    errors = []
    try:
        
        validate_plugin_xml(os.path.join(__location__, 'plugin.xml'))

        if args.expand_filenames:
            csv.expand_filenames = True

        if args.timezone is not None:
            csv.timezone = pytz.timezone(args.timezone)

        if args.nodes is not None:
            # Validation

            if args.template is not None:
                try:
                    csv.validate_template(args.template)
                except Exception, e:
                    raise HydraPluginError("An error has occurred with the template. (%s)"%(e))

            # Create project and network only when there is actual data to
            # import.
            project = csv.create_project(ID=args.project)

            scenario = dict( 
                name = 'Sample scenario',
                description='Default scenario created by the CSV import plug-in.',
                resourcescenarios = [],
            )

            # Create a new network
            network = dict(
                project_id = project['id'],
                name = "CSV import",
                description = "Network created by the test plug-in.",
                nodes = [],
                links = [],
                scenarios = [scenario],
                resourcegroups = [],
                attributes = [],
            )
            
            network = csv.call('add_network', {'net':network})

            write_progress(1,csv.num_steps) 
            for nodefile in args.nodes:
                write_output("Reading Node file %s" % nodefile)
                nodes = csv.read_nodes(nodefile)

            write_progress(2,csv.num_steps) 
            if args.links is not None:
                for linkfile in args.links:
                    write_output("Reading Link file %s" % linkfile)
                    links = csv.read_links(linkfile)

            write_progress(3,csv.num_steps) 
            write_progress(4,csv.num_steps) 
            write_output("Saving network")
            
            csv.create_network(network)

            if csv.NetworkSummary['scenarios']:
                scen_ids = [s['id'] for s in csv.NetworkSummary['scenarios']]

            network_id = csv.NetworkSummary['id']

            write_progress(6,csv.num_steps)

        else:
            log.info('No nodes found. Nothing imported.')

	errors = []
    except HydraPluginError as e:
        errors = [e.message]

    xml_response = PluginLib.create_xml_response('ImportCSV',
                                                 network_id,
                                                 scen_ids,
                                                 errors,
                                                 csv.warnings,
                                                 csv.message,
                                                 csv.files)

    print xml_response
