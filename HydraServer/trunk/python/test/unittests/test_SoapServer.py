#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest
import logging
import shutil
import os

from HydraLib import hydra_logging, util

from suds.client import Client
from tempfile import gettempdir as tmp


class SoapServerTest(unittest.TestCase):

    def setUp(self):
        hydra_logging.init(level='INFO')
        # Clear SUDS cache:
        shutil.rmtree(os.path.join(tmp(), 'suds'), True)

    def tearDown(self):
        logging.debug("Tearing down")
        hydra_logging.shutdown()

    def connect(self, login=True):
        #logging.debug("Connecting to server.")
        config = util.load_config()
        port = config.getint('hydra_server', 'port')
        url = 'http://localhost:%s?wsdl' % port
        client = Client(url)
        
        token = None
        if login == True:
            session_id = client.service.login('root', '')
        
            token = client.factory.create('RequestHeader')
            token.session_id = session_id
            token.username = 'root'
      
        client.set_options(cache=None, soapheaders=token)
        client.add_prefix('hyd', 'soap_server.hydra_complexmodels')
        return client

    def create_project(self, name):
        cli = self.connect()
        project = cli.factory.create('hyd:Project')
        project.name = 'SOAP test'
        project = cli.service.add_project(project)
        return project

    def create_network(self, project):
        cli = self.connect()
        network = cli.factory.create('hyd:Network')
        network.name = 'Test network'
        network.description = 'A test network.'
        network.project_id = project.project_id
        network = cli.service.add_network(network)
        return network

    def create_link(self, node_1_id, node_2_id):
        cli = self.connect()
        link = cli.factory.create('hyd:Link')
        link.name = 'Test'
        link.description = 'A test link between two nodes.'
        link.node_1_id = node_1_id
        link.node_2_id = node_2_id

        return link

    def create_node(self,node_id, attributes=None):
        cli = self.connect()
        node = cli.factory.create('hyd:Node')
        node.id = node_id
        node.name = "Test Node name"
        node.description = "A node representing a water resource"
        node.x = 0
        node.y = 0
        node.attributes = attributes

        return node

    def create_attr(self):
        cli = self.connect()
        attr = cli.factory.create('hyd:Attr')
        attr.name = 'Test attribute'
        attr.dimen = 'dimensionless'
        attr = cli.service.add_attribute(attr)
        return attr


def run():
    unittest.main()

if __name__ == '__main__':
    run()  # all tests
