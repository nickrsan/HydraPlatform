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
import datetime

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

    def create_node(self,node_id, attributes=None, node_name="Test Node Name"):
        node = self.client.factory.create('hyd:Node')
        node.id = node_id
        node.name = node_name
        node.description = "A node representing a water resource"
        node.x = 0
        node.y = 0
        node.attributes = attributes

        return node

    def create_attr(self, name="Test attribute"):
        attr = self.client.service.get_attribute(name)
        if attr is None:
            attr       = self.client.factory.create('hyd:Attr')
            attr.name  = name 
            attr.dimen = 'dimensionless'
            attr = self.client.service.add_attribute(attr)
        return attr

    def create_network_with_data(self):
        """
            Test adding data to a network through a scenario.
            This test adds attributes to one node and then assignes data to them.
            It assigns a descriptor, array and timeseries to the 
            attributes node. 
        """
        start = datetime.datetime.now()
        (project) = {
            'name'        : 'New Project',
            'description' : 'New Project Description',
        }
        p =  self.client.service.add_project(project)

        print "Project creation took: %s"%(datetime.datetime.now()-start)
        start = datetime.datetime.now()

        #Create some attributes, which we can then use to put data on our nodes
        attr1 = self.create_attr("testattr_1")
        attr2 = self.create_attr("testattr_2")
        attr3 = self.create_attr("testattr_3")

        print "Attribute creation took: %s"%(datetime.datetime.now()-start)
        start = datetime.datetime.now()

        #Create 2 nodes
        node1 = self.create_node(-1, node_name="Node 1")
        node2 = self.create_node(-2, node_name="Node 2")

        node_array = self.client.factory.create('ns1:NodeArray')
        node_array.Node.append(node1)
        node_array.Node.append(node2)

        #From our attributes, create a resource attr for our node
        #We don't assign data directly to these resource attributes. This
        #is done when creating the scenario -- a scenario is just a set of
        #data for a given list of resource attributes.
        attr_array = self.client.factory.create('ns1:ResourceAttrArray')
        node_attr1  = self.client.factory.create('ns1:ResourceAttr')
        node_attr1.id = -1
        node_attr1.attr_id = attr1.id
        node_attr2  = self.client.factory.create('ns1:ResourceAttr')
        node_attr2.attr_id = attr2.id
        node_attr2.id = -2
        node_attr3  = self.client.factory.create('ns1:ResourceAttr')
        node_attr3.attr_id = attr3.id
        node_attr3.id = -3
        attr_array.ResourceAttr.append(node_attr1)
        attr_array.ResourceAttr.append(node_attr2)
        attr_array.ResourceAttr.append(node_attr3)

        node1.attributes = attr_array

        #Connect the two nodes with a link
        link = self.create_link(node1['id'], node2['id'])

        #A network must contain an array of links. In this case, the array
        #contains a single link
        link_array = self.client.factory.create('ns1:LinkArray')
        link_array.Link.append(link)

        print "Making nodes & links took: %s"%(datetime.datetime.now()-start)
        start = datetime.datetime.now()

        #Create the scenario
        scenario = self.client.factory.create('ns1:Scenario')
        scenario.id = -1
        scenario.name = 'Scenario 1'
        scenario.description = 'Scenario Description'

        #Multiple data (Called ResourceScenario) means an array.
        scenario_data = self.client.factory.create('ns1:ResourceScenarioArray')

        #Our node has several dmin'resource attributes', created earlier.
        node_attrs = node1['attributes']

        #This is an example of 3 diffent kinds of data
        #A simple string (Descriptor)
        #A time series, where the value may be a 1-D array
        #A multi-dimensional array.
        descriptor = self.create_descriptor(node_attrs.ResourceAttr[0])
        timeseries = self.create_timeseries(node_attrs.ResourceAttr[1])
        array      = self.create_array(node_attrs.ResourceAttr[2])

        scenario_data.ResourceScenario.append(descriptor)
        scenario_data.ResourceScenario.append(timeseries)
        scenario_data.ResourceScenario.append(array)


        #Set the scenario's data to the array we have just populated
        scenario.resourcescenarios = scenario_data

        #A network can have multiple scenarios, so they are contained in
        #a scenario array
        scenario_array = self.client.factory.create('ns1:ScenarioArray')
        scenario_array.Scenario.append(scenario)

        print "Scenario definition took: %s"%(datetime.datetime.now()-start)
        start = datetime.datetime.now()

        (network) = {
            'name'        : 'Network 1',
            'description' : 'Test network with 2 nodes and 1 link',
            'project_id'  : p['id'],
            'links'       : link_array,
            'nodes'       : node_array,
            'scenarios'   : scenario_array,
        }
        #print network
        network = self.client.service.add_network(network)

        print "Network Creation took: %s"%(datetime.datetime.now()-start)

        return network

    def create_descriptor(self, ResourceAttr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        scenario_attr = self.client.factory.create('ns1:ResourceScenario')

        scenario_attr.attr_id = ResourceAttr.attr_id
        scenario_attr.resource_attr_id = ResourceAttr.id

        dataset = self.client.factory.create('ns1:Dataset')
        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit = 'metres / second'
        dataset.dimension = 'number of units per time unit'
        
        descriptor = self.client.factory.create('ns1:Descriptor')
        descriptor.desc_val = 'test'

        dataset.value = descriptor

        scenario_attr.value = dataset

        return scenario_attr


    def create_timeseries(self, ResourceAttr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        scenario_attr = self.client.factory.create('ns1:ResourceScenario')

        scenario_attr.attr_id = ResourceAttr.attr_id
        scenario_attr.resource_attr_id = ResourceAttr.id

        dataset = self.client.factory.create('ns1:Dataset')
        
        dataset.type = 'timeseries'
        dataset.name = 'my time series'
        dataset.unit = 'feet cubed'
        dataset.dimension = 'cubic capacity'

        ts1 = self.client.factory.create('ns1:TimeSeriesData')
        ts1.ts_time  = datetime.datetime.now()
        ts1.ts_value = str([1, 2, 3, 4, 5])

        ts2 = self.client.factory.create('ns1:TimeSeriesData')
        ts2.ts_time  = datetime.datetime.now() + datetime.timedelta(hours=1)
        ts2.ts_value = str([2, 3, 4, 5, 6])

        ts3 = self.client.factory.create('ns1:TimeSeries')
        ts3.ts_values.TimeSeriesData.append(ts1)
        ts3.ts_values.TimeSeriesData.append(ts2)

        dataset.value = ts3
        scenario_attr.value = dataset

        return scenario_attr

    def create_array(self, ResourceAttr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        scenario_attr = self.client.factory.create('ns1:ResourceScenario')

        scenario_attr.attr_id = ResourceAttr.attr_id
        scenario_attr.resource_attr_id = ResourceAttr.id
        
        dataset = self.client.factory.create('ns1:Dataset')
        dataset.type = 'array'
        dataset.name = 'my array'
        dataset.unit = 'joules'
        dataset.dimension = 'pressure'

        arr = self.client.factory.create('ns1:Array')
        arr.arr_data = str([[[1, 2, 3], [5, 4, 6]],[[10, 20, 30], [40, 50, 60]]])
        
        dataset.value = arr

        scenario_attr.value = dataset

        return scenario_attr

def run():
    unittest.main()

if __name__ == '__main__':
    run()  # all tests
