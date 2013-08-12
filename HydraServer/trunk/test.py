import os
import datetime
import unittest
from decimal import Decimal
import shutil
from tempfile import gettempdir as tmp
shutil.rmtree(os.path.join(tmp(), 'suds'), True)
from suds.client import Client
from suds.plugin import MessagePlugin

class FixNamespace(MessagePlugin):
    def marshalled(self, context):
        scenarios = context.envelope.getChild('Body')[0].getChild('network').getChild('scenarios')
        if scenarios:
            scenario = scenarios.getChildren()[0]
            if scenario:
                data = scenario.getChild('data')
                if data:
                    scenario_attr = data.getChildren()[0]
                    if scenario_attr:
                        val = scenario_attr.getChild('value')
                        for v in val.getChildren():
                            if v.namespace()[0] == 'xs':
                                v.setPrefix('ns0')

class TestSoap(unittest.TestCase):

    def create_node(self,c,name,desc="Node Description", x=0, y=0, attributes=None):
        (node_1) = {
            'node_name' : name,
            'node_description' : desc,
            'node_x' : x,
            'node_y' : y,
            'attributes': attributes,
        }

        node_1 = c.service.add_node(node_1)
        return node_1
    
    def create_link(self,c,name,node_1_id, node_2_id):
        link = c.factory.create('ns5:Link')
        link.link_name = name
        link.link_description = 'Link from %s to %s'%(node_1_id, node_2_id)
        link.node_1_id = node_1_id 
        link.node_2_id = node_2_id
        return link

    def create_attr(self, c):
        attr = c.factory.create('ns5:Attr')
        attr.attr_name = 'Test Attr'
        attr.attr_dimen = 'very big'
        attr = c.service.add_attribute(attr)
        return attr

    def create_network(self, c, project_id, name, desc=None, links=None, scenarios=None):
        (network) = {
            'network_name'        : name,
            'network_description' : desc,
            'project_id'          : project_id,
            'links'               : links,
            'scenarios'           : scenarios,
        }
        network = c.service.add_network(network)
        return network

    def test_add_project(self):
        c = Client('http://localhost:8000/?wsdl')
        (project) = {
            'project_name' : 'New Project',
            'project_description' : 'New Project Description',
        }
        p =  c.service.add_project(project)
        print p
        p1 =  c.service.get_project(p['project_id'])
        print p1
        (project1) = {
            'project_id'   : p['project_id'],
            'project_name' : 'Updated Project',
            'project_description' : 'Updated Project Description',
        }
        p2 = c.service.update_project(project1)
        print p2


    def test_add_node(self):
        c = Client('http://localhost:8000/?wsdl')
        (Node1) = {
            'node_name' : 'Node One',
            'node_description' : 'Node One Description',
            'node_x' : Decimal('1.50'),
            'node_y' : Decimal('2.00'),
        }

        Node1 = c.service.add_node(Node1)
        assert Node1 is not None, "Node did not add correctly"

    def test_network(self):
        c = Client('http://localhost:8000/?wsdl')
        (project) = {
            'project_name' : 'New Project',
            'project_description' : 'New Project Description',
        }
        p =  c.service.add_project(project)

        node1 = self.create_node(c, "Node 1")
        node2 = self.create_node(c, "Node 2")

        link = self.create_link(c, 'link 1', node1['node_id'], node2['node_id'])

        LinkArray = c.factory.create('ns5:LinkArray')
        LinkArray.Link.append(link)

        (Network) = {
            'network_name'        : 'Network1',
            'network_description' : 'Test Network with 2 nodes and 1 link',
            'project_id'          : p['project_id'],
            'links'              : LinkArray,
        }
        Network = c.service.add_network(Network)
        assert Network is not None, "Network did not create correctly"

    def test_scenario(self):
        c = Client('http://localhost:8000/?wsdl', plugins=[FixNamespace()])
        print c

        (project) = {
            'project_name' : 'New Project',
            'project_description' : 'New Project Description',
        }
        p =  c.service.add_project(project)

        #Create some attributes, which we can then use to put data on our nodes
        attr1 = self.create_attr(c) 
        attr2 = self.create_attr(c) 
        attr3 = self.create_attr(c) 

        #From our attributes, create a resource attr for our node
        #We don't assign data directly to these resource attributes. This
        #is done when creating the scenario -- a scenario is just a set of
        #data for a given list of resource attributes.
        attr_array = c.factory.create('ns5:ResourceAttrArray')
        node_attr1  = c.factory.create('ns5:ResourceAttr')
        node_attr1.attr_id = attr1.attr_id
        node_attr2  = c.factory.create('ns5:ResourceAttr')
        node_attr2.attr_id = attr2.attr_id
        node_attr3  = c.factory.create('ns5:ResourceAttr')
        node_attr3.attr_id = attr3.attr_id
        attr_array.ResourceAttr.append(node_attr1)
        attr_array.ResourceAttr.append(node_attr2)
        attr_array.ResourceAttr.append(node_attr3)

        #Create 2 nodes
        node1 = self.create_node(c, "Node 1", attributes=attr_array)
        node2 = self.create_node(c, "Node 2")

        #Connect the two nodes with a link
        link = self.create_link(c, 'link 1', node1['node_id'], node2['node_id'])

        #A network must contain an array of links. In this case, the array
        #contains a single link
        link_array = c.factory.create('ns5:LinkArray')
        link_array.Link.append(link)
        
        #Create the scenario
        scenario = c.factory.create('ns5:Scenario')
        scenario.scenario_name = 'Scenario 1'
        scenario.scenario_description = 'Scenario Description'

        #Multiple data (Called ScenarioAttr) means an array.
        scenario_data = c.factory.create('ns5:ScenarioAttrArray')
        
        #Our node has several 'resource attributes', created earlier.
        node_attrs = node1.attributes
        
        #This is an example of 3 diffent kinds of data
        #A simple string (Descriptor)
        #A time series, where the value may be a 1-D array
        #A multi-dimensional array.
        descriptor = self.create_descriptor(c, node_attrs.ResourceAttr[0])
        timeseries = self.create_timeseries(c, node_attrs.ResourceAttr[0])
        array      = self.create_array(c, node_attrs.ResourceAttr[0])
        
        scenario_data.ScenarioAttr.append(descriptor)
        scenario_data.ScenarioAttr.append(timeseries)
        scenario_data.ScenarioAttr.append(array)


        #Set the scenario's data to the array we have just populated
        scenario.data = scenario_data

        #A network can have multiple scenarios, so they are contained in
        #a scenario array
        scenario_array = c.factory.create('ns5:ScenarioArray')
        scenario_array.Scenario.append(scenario)
        network = self.create_network(c, p['project_id'], 'Network1', 'Test Network with 2 nodes and 1 link',links=link_array,scenarios=scenario_array)
        print c.last_sent()
        print network
        print c.last_received()

    def create_descriptor(self, c, ResourceAttr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        scenario_attr = c.factory.create('ns5:ScenarioAttr')
        
        scenario_attr.attr_id = ResourceAttr.attr_id
        scenario_attr.resource_attr_id = ResourceAttr.resource_attr_id
        
        #All pieces of data must be wrapped in a 'Data' tag, in which 
        #you put the type and the value itself. The value must be one
        #of the following: [Descriptor, TimeSeries, EqTimeSeries, Scalar, Array]
        data  = c.factory.create('ns1:Data')
        data.type = 'Descriptor'
        descriptor = c.factory.create('ns1:Descriptor')
        descriptor.desc_val = "I am a value"
        data.value = descriptor
        scenario_attr.value = data 

        return scenario_attr


    def create_timeseries(self, c, ResourceAttr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        scenario_attr = c.factory.create('ns5:ScenarioAttr')
        
        scenario_attr.attr_id = ResourceAttr.attr_id
        scenario_attr.resource_attr_id = ResourceAttr.resource_attr_id
        
        #All pieces of data must be wrapped in a 'Data' tag, in which 
        #you put the type and the value itself. The value must be one
        #of the following: [Descriptor, TimeSeries, EqTimeSeries, Scalar, Array]
        data  = c.factory.create('ns1:Data')
        data.type = 'TimeSeries'
        ts = c.factory.create('ns1:TimeSeries')
        ts.ts_time = datetime.datetime.now()
        ts.ts_value = [1, 2, 3, 4, 5]
        data.value = ts
        scenario_attr.value = data 

        return scenario_attr

    def create_array(self, c, ResourceAttr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        scenario_attr = c.factory.create('ns5:ScenarioAttr')
        
        scenario_attr.attr_id = ResourceAttr.attr_id
        scenario_attr.resource_attr_id = ResourceAttr.resource_attr_id
        
        #All pieces of data must be wrapped in a 'Data' tag, in which 
        #you put the type and the value itself. The value must be one
        #of the following: [Descriptor, TimeSeries, EqTimeSeries, Scalar, Array]
        data  = c.factory.create('ns1:Data')
        data.type = 'Array'
        
        arr2 = c.factory.create('ns1:Array')
        arr2.arr_data = [1, 2, 3]
        arr3 = c.factory.create('ns1:Array')
        arr3.arr_data = [4, 5, 6]
        
        arr1 = c.factory.create('ns1:Array')
        arr1.arr_data = [arr2, arr3]

        data.value = arr1
        scenario_attr.value = data 

        return scenario_attr

def run():
    unittest.main()

if __name__ == "__main__":
    run() # run all tests
