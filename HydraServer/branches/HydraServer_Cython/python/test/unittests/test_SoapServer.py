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

    def connect(self):
        #logging.debug("Connecting to server.")
        config = util.load_config()
        port = config.getint('soap_server', 'port')
        url = 'http://localhost:%s?wsdl' % port
        client = Client(url)
        client.set_options(cache=None)
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
        network.network_name = 'Test network'
        network.network_description = 'A test network.'
        network.project_id = project.project_id
        network = cli.service.add_network(network)
        return network

    def create_node(self, attributes=None):
        cli = self.connect()
        node = cli.factory.create('hyd:Node')
        node.node_name = 'Test node'
        node.node_description = 'A test node for testing.'
        node.node_x = 1
        node.node_y = 1
        node.attributes = attributes
        node = cli.service.add_node(node)
        return node

    def create_link(self, network, node1, node2):
        cli = self.connect()
        link = cli.factory.create('hyd:Link')
        link.link_name = 'Test'
        link.link_description = 'A test link between two nodes.'
        link.node_1_id = node1.node_id
        link.node_2_id = node2.node_id
        link.network_id = network.network_id
        link = cli.service.add_link(link)
        return link

    def create_attr(self):
        cli = self.connect()
        attr = cli.factory.create('hyd:Attr')
        attr.attr_name = 'Test attribute'
        attr.attr_dimen = 'dimensionless'
        attr = cli.service.add_attribute(attr)
        return attr


def run():
    unittest.main()

if __name__ == '__main__':
    run()  # all tests
