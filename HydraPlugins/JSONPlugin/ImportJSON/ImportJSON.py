#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) Copyright 2013, 2014, 2015 University of Manchester\
#\
# ImportJSON is free software: you can redistribute it and/or modify\
# it under the terms of the GNU General Public License as published by\
# the Free Software Foundation, either version 3 of the License, or\
# (at your option) any later version.\
#\
# ImportJSON is distributed in the hope that it will be useful,\
# but WITHOUT ANY WARRANTY; without even the implied warranty of\
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the\
# GNU General Public License for more details.\
# \
# You should have received a copy of the GNU General Public License\
# along with ImportJSON.  If not, see <http://www.gnu.org/licenses/>\
#

"""A Hydra plug-in for exporting a hydra network to a JSON file.

Basics
~~~~~~

The plug-in for exporting a network to a JSON file.
Basic usage::

       ImportJSON.py [-h] [-t NETWORK]

Options
~~~~~~~

====================== ====== ============ ========================================
Option                 Short  Parameter    Description
====================== ====== ============ ========================================
``--help``             ``-h``              Show help message and exit.
``--network``          ``-t`` NETWORK      The file containing network
                                           information.
``--server-url``       ``-u`` SERVER-URL   Url of the server the plugin will
                                           connect to.
                                           Defaults to localhost.
``--session-id``       ``-c`` SESSION-ID   Session ID used by the calling software.
                                           If left empty, the plugin will attempt
                                           to log in itself.
====================== ====== ============ ========================================


API docs
~~~~~~~~
"""

import argparse as ap
import logging

from HydraLib import PluginLib
from HydraLib.PluginLib import JsonConnection
from HydraLib.HydraException import HydraPluginError
from HydraLib.PluginLib import write_progress, write_output, validate_plugin_xml, RequestError

import json

import os, sys

from datetime import datetime


if "../" not in sys.path:
    sys.path.append("../")

from utils.xml2json import xml2json

log = logging.getLogger(__name__)

__location__ = os.path.split(sys.argv[0])[0]

class ImportJSON(object):
    """
    """

    Network = None

    def __init__(self, url=None, session_id=None):

        self.warnings = []
        self.files    = []

        self.connection = JsonConnection(url)
        if session_id is not None:
            log.info("Using existing session %s", session_id)
            self.connection.session_id=session_id
        else:
            self.connection.login()

        #3 steps: start, read, save 
        self.num_steps = 3

    def import_network(self, network):
        """
            Read the file containing the network data and send it to
            the server.
        """
        
        write_output("Reading Network") 
        write_progress(2, self.num_steps) 

        if network is not None:
            network_file = open(network).readlines()
            text = "".join(x for x in network_file)
            
            try:
                network_data = json.loads(text)
            except:

                try:
                    json_string = xml2json(network_file[0])
                    network_data = json.loads(json_string)
                    network_data = network_data['network']
                except Exception, e:
                    log.exception(e)
                    raise HydraPluginError("Unrecognised data format.")
            
            project = self.create_project(network_data)
            network_data['project_id'] = project['id']

            write_output("Saving Network") 
            write_progress(3, self.num_steps) 

            #The network ID can be specified to get the network...
            network = self.connection.call('add_network', {'net':network_data})
        else:
            raise HydraPluginError("A network ID must be specified!")
        return network

    def create_project(self, network):
        """
            If a project ID is not specified within the network, a new one
            must be created to hold the incoming network. 
            If an ID is specified, we must retrieve the project to make sure
            it exists. If it does not exist, then a new project is created.

            Returns the project object so that the network can access it's ID.
        """
        project_id = network.get('project_id')
        if project_id is not None:
            try:
                project = self.connection.call('get_project', {'project_id':project_id})
                log.info('Loading existing project (ID=%s)' % project_id)
                return project
            except RequestError:
                log.info('Project ID not found. Creating new project')

        #Using 'datetime.now()' in the name guarantees a unique project name.
        new_project = dict(
            name = "Project for network %s created at %s" % (network['name'], datetime.now()),
            description = \
            "Default project created by the %s plug-in." % \
                (self.__class__.__name__),
        )
        saved_project = self.connection.call('add_project', {'project':new_project})
        return saved_project 


def commandline_parser():
    parser = ap.ArgumentParser(
        description="""Import a network in JSON format.

Written by Stephen Knox <stephen.knox@manchester.ac.uk>
(c) Copyright 2015, University of Manchester.
        """, epilog="For more information visit www.hydraplatform.org",
        formatter_class=ap.RawDescriptionHelpFormatter)
    parser.add_argument('-t', '--network',
                        help='''Specify the network_id of the network to be exported.
                        If the network_id is not known, specify the network name. In
                        this case, a project ID must also be provided''')
    parser.add_argument('-u', '--server-url',
                        help='''Specify the URL of the server to which this
                        plug-in connects.''')
    parser.add_argument('-c', '--session_id',
                        help='''Session ID. If this does not exist, a login will be
                        attempted based on details in config.''')
    return parser


def run():
    parser = commandline_parser()
    args = parser.parse_args()
    json_importer = ImportJSON(url=args.server_url, session_id=args.session_id)
    network_id = None
    scenarios = []
    errors = []
    try:      
        write_progress(1, json_importer.num_steps) 
        
        validate_plugin_xml(os.path.join(__location__, 'plugin.xml'))

        net = json_importer.import_network(args.network)
        scenarios = [s.id for s in net.scenarios]
        network_id = net.id
        message = "Import complete"
    except HydraPluginError as e:
        message="An error has occurred"
        errors = [e.message]
        log.exception(e)
    except Exception, e:
        message="An error has occurred"
        log.exception(e)
        errors = [e]

    xml_response = PluginLib.create_xml_response('ImportJSON',
                                                 network_id,
                                                 scenarios,
                                                 errors,
                                                 json_importer.warnings,
                                                 message,
                                                 json_importer.files)
    print xml_response

if __name__ == '__main__':
    run()

