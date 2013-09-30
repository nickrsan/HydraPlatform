#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest
import logging
import shutil
import os

from HydraLib import hydra_logging, util

from suds.client import Client
from suds.plugin import MessagePlugin
from tempfile import gettempdir as tmp

global CLIENT
CLIENT = None

class FixNamespace(MessagePlugin):
    def marshalled(self, context):
        self.fix_ns(context.envelope)

    def fix_ns(self, element):
        if element.prefix == 'xs':
            element.prefix = 'ns1'

        for e in element.getChildren():
            self.fix_ns(e)

def connect(login=True):
    #logging.debug("Connecting to server.")
    config = util.load_config()
    port = config.getint('hydra_server', 'port')
    url = 'http://localhost:%s?wsdl' % port
    client = Client(url, plugins=[FixNamespace()])
    
    token = None
    if login == True:

        session_id = client.service.login('root', '')
    
        token = client.factory.create('RequestHeader')
        token.session_id = session_id
        token.username = 'root'
  
    client.set_options(cache=None, soapheaders=token)
    client.add_prefix('hyd', 'soap_server.hydra_complexmodels')
    global CLIENT
    CLIENT = client
    return client

class SoapServerTest(unittest.TestCase):

    def setUp(self):
        hydra_logging.init(level='INFO')
        # Clear SUDS cache:
        shutil.rmtree(os.path.join(tmp(), 'suds'), True)
        global CLIENT
        if CLIENT is None:
            connect()
        
        self.client = CLIENT

    def tearDown(self):
        logging.debug("Tearing down")
        hydra_logging.shutdown()

    def create_project(self, name):
        project = self.client.factory.create('hyd:Project')
        project.name = 'SOAP test'
        project = self.client.service.add_project(project)
        return project

    def create_network(self, project):
        network = self.client.factory.create('hyd:Network')
        network.name = 'Test network'
        network.description = 'A test network.'
        network.project_id = project.id
        network.nodes = []
        network.links = []
        network.scenarios = []
        network = self.client.service.add_network(network)
        return network

    def create_link(self, node_1_id, node_2_id):
        link = self.client.factory.create('hyd:Link')
        link.name = 'Test'
        link.description = 'A test link between two nodes.'
        link.node_1_id = node_1_id
        link.node_2_id = node_2_id

        return link

    def create_node(self,node_id, attributes=None):
        node = self.client.factory.create('hyd:Node')
        node.id = node_id
        node.name = "Test Node name"
        node.description = "A node representing a water resource"
        node.x = 0
        node.y = 0
        node.attributes = attributes

        return node

    def create_attr(self):
        attr = self.client.factory.create('hyd:Attr')
        attr.name = 'Test attribute'
        attr.dimen = 'dimensionless'
        attr = self.client.service.add_attribute(attr)
        return attr


def run():
    unittest.main()

if __name__ == '__main__':
    run()  # all tests
