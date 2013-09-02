import sys
if "./python" not in sys.path:
    sys.path.append("./python")
if "../../HydraLib/trunk/" not in sys.path:
    sys.path.append("../../HydraLib/trunk/")

import os
import datetime
import unittest
import shutil
from tempfile import gettempdir as tmp
shutil.rmtree(os.path.join(tmp(), 'suds'), True)
from suds.client import Client
from suds.plugin import MessagePlugin
from HydraLib import util

class FixNamespace(MessagePlugin):
    def marshalled(self, context):
        self.fix_ns(context.envelope)

    def fix_ns(self, element):
        if element.prefix == 'xs':
            element.prefix = 'ns1'

        for e in element.getChildren():
            self.fix_ns(e)

class TestSoap(unittest.TestCase):

    def create_node(self,c,node_id,name,desc="Node Description", x=0, y=0, attributes=None):
        node = c.factory.create('ns1:Node')
        node.id = node_id
        node.name = name
        node.description = desc
        node.x = x
        node.y = y

        return node
    
    def create_link(self,c,name,node_1_id, node_2_id):
        link = c.factory.create('ns1:Link')
        link.name = name
        link.description = 'Link from %s to %s'%(node_1_id, node_2_id)
        link.node_1_id = node_1_id 
        link.node_2_id = node_2_id
        print link
        return link

    def create_link_without_ids(self,c,name,node_1_name, node_1_x, node_1_y, node_2_name, node_2_x, node_2_y):
        link = c.factory.create('ns1:Link')
        link.name = name
        link.description = 'Link from node %s at %s, %s to node %s at %s, %s'%(node_1_name, node_1_x, node_2_x, node_2_name, node_2_x, node_2_y)
        link.node_1_id = "%s,%s,%s"%(node_1_name, node_1_x, node_1_y) 
        link.node_2_id = "%s,%s,%s"%(node_2_name, node_2_x, node_2_y)
        return link

    def create_attr(self, c):
        attr = c.factory.create('ns1:Attr')
        attr.name = 'Test Attr'
        attr.dimen = 'very big'
        attr = c.service.add_attribute(attr)
        #print attr
        return attr

    def create_network(self, c, project_id, name, desc=None, nodes=None, links=None, scenarios=None):
        (network) = {
            'name'        : name,
            'description' : desc,
            'project_id'  : project_id,
            'links'       : links,
            'nodes'       : nodes,
            'scenarios'   : scenarios,
        }
        print network
        network = c.service.add_network(network)
        return network

    def test_add_project(self):

        config = util.load_config()
        port = config.getint('soap_server', 'port')
        c = Client('http://localhost:%s/?wsdl'%port, plugins=[FixNamespace()])
        (project) = {
            'name' : 'New Project',
            'description' : 'New Project Description',
        }
        p =  c.service.add_project(project)
        print p
        p1 =  c.service.get_project(p['id'])
        print p1
        (project1) = {
            'id'   : p['id'],
            'name' : 'Updated Project',
            'description' : 'Updated Project Description',
        }
        p2 = c.service.update_project(project1)
        print p2

    def test_network(self):
        config = util.load_config()
        port = config.getint('soap_server', 'port')
        c = Client('http://localhost:%s/?wsdl'%port, plugins=[FixNamespace()])


        (project) = {
            'name' : 'New Project',
            'description' : 'New Project Description',
        }
        p =  c.service.add_project(project)

        NodeArray = c.factory.create('ns1:NodeArray')
        node1 = self.create_node(c, -1, "Node 1")
        node2 = self.create_node(c, -2, "Node 2")
        node3 = self.create_node(c, -3, "Node 3")
        NodeArray.Node.append(node1)
        NodeArray.Node.append(node2)
        NodeArray.Node.append(node3)

        link1 = self.create_link(c, 'link 1', node1.id, node2.id)
        link2 = self.create_link(c, 'link 1', node2.id, node3.id)

        LinkArray = c.factory.create('ns1:LinkArray')
        LinkArray.Link.append(link1)
        LinkArray.Link.append(link2)

        (Network) = {
            'name'        : 'Network1',
            'description' : 'Test Network with 2 nodes and 1 link',
            'project_id'  : p['id'],
            'links'       : LinkArray,
            'nodes'       : NodeArray,
        }
        print Network
        Network = c.service.add_network(Network)
        assert Network is not None, "Network did not create correctly"

    def test_scenario(self):
        config = util.load_config()
        port = config.getint('soap_server', 'port')
        c = Client('http://localhost:%s/?wsdl'%port, xstq=False,  plugins=[FixNamespace()])
#        c = Client('http://localhost:%s/?wsdl'%port, plugins=[FixNamespace()])

        (project) = {
            'name'        : 'New Project',
            'description' : 'New Project Description',
        }
        p =  c.service.add_project(project)

        #Create some attributes, which we can then use to put data on our nodes
        attr1 = self.create_attr(c) 
        attr2 = self.create_attr(c) 
        attr3 = self.create_attr(c) 

        #Create 2 nodes
        node1 = self.create_node(c, -1, "Node 1")
        node2 = self.create_node(c, -2, "Node 2")

        node_array = c.factory.create('ns1:NodeArray')
        node_array.Node.append(node1)
        node_array.Node.append(node2)

        #From our attributes, create a resource attr for our node
        #We don't assign data directly to these resource attributes. This
        #is done when creating the scenario -- a scenario is just a set of
        #data for a given list of resource attributes.
        attr_array = c.factory.create('ns1:ResourceAttrArray')
        node_attr1  = c.factory.create('ns1:ResourceAttr')
        node_attr1.id = -1
        node_attr1.attr_id = attr1.id
        node_attr2  = c.factory.create('ns1:ResourceAttr')
        node_attr2.attr_id = attr2.id
        node_attr2.id = -2
        node_attr3  = c.factory.create('ns1:ResourceAttr')
        node_attr3.attr_id = attr3.id
        node_attr3.id = -3
        attr_array.ResourceAttr.append(node_attr1)
        attr_array.ResourceAttr.append(node_attr2)
        attr_array.ResourceAttr.append(node_attr3)

        node1.attributes = attr_array

        #Connect the two nodes with a link
        link = self.create_link(c, 'link 1', node1['id'], node2['id'])

        #A network must contain an array of links. In this case, the array
        #contains a single link
        link_array = c.factory.create('ns1:LinkArray')
        link_array.Link.append(link)
        
        #Create the scenario
        scenario = c.factory.create('ns1:Scenario')
        scenario.id = -1
        scenario.name = 'Scenario 1'
        scenario.description = 'Scenario Description'

        #Multiple data (Called ResourceScenario) means an array.
        scenario_data = c.factory.create('ns1:ResourceScenarioArray')
        
        #Our node has several dmin'resource attributes', created earlier.
        node_attrs = node1['attributes']
        
        #This is an example of 3 diffent kinds of data
        #A simple string (Descriptor)
        #A time series, where the value may be a 1-D array
        #A multi-dimensional array.
        descriptor = self.create_descriptor(c, node_attrs.ResourceAttr[0])
        timeseries = self.create_timeseries(c, node_attrs.ResourceAttr[1])
        array      = self.create_array(c, node_attrs.ResourceAttr[2])
        
        scenario_data.ResourceScenario.append(descriptor)
        scenario_data.ResourceScenario.append(timeseries)
        scenario_data.ResourceScenario.append(array)


        #Set the scenario's data to the array we have just populated
        scenario.resourcescenarios = scenario_data

        #A network can have multiple scenarios, so they are contained in
        #a scenario array
        scenario_array = c.factory.create('ns1:ScenarioArray')
        scenario_array.Scenario.append(scenario)
        network = self.create_network(c, p['id'], 'Network1', 'Test Network with 2 nodes and 1 link',nodes=node_array,links=link_array,scenarios=scenario_array)
     #   print c.last_sent()
        print "****************************"
        print network
      #  print c.last_received()

    def create_descriptor(self, c, ResourceAttr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        scenario_attr = c.factory.create('ns1:ResourceScenario')
        
        scenario_attr.attr_id = ResourceAttr.attr_id
        scenario_attr.resource_attr_id = ResourceAttr.id
        scenario_attr.type = 'descriptor'
       
        print scenario_attr

        scenario_attr.value = {'desc_val' : 'test'} 

        return scenario_attr


    def create_timeseries(self, c, ResourceAttr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        scenario_attr = c.factory.create('ns1:ResourceScenario')
        
        scenario_attr.attr_id = ResourceAttr.attr_id
        scenario_attr.resource_attr_id = ResourceAttr.id
        scenario_attr.type = 'timeseries'
        
        ts1 = c.factory.create('ns1:TimeSeriesData')
        ts1.ts_time  = datetime.datetime.now()
        ts1.ts_value = [1, 2, 3, 4, 5]

        ts2 = c.factory.create('ns1:TimeSeriesData')
        ts2.ts_time  = datetime.datetime.now() + datetime.timedelta(hours=1)
        ts2.ts_value = [2, 3, 4, 5, 6]

        ts3 = c.factory.create('ns1:TimeSeries')
        ts3.ts_values = [ts1, ts2] 
        
        #scenario_attr.value = ts3
        scenario_attr.value = {
            'value'            : [
                {
                   'ts_time'   :  datetime.datetime.now(),
                   'ts_value' : str([1, 2, 3, 4, 5]),
                },
                {
                    'ts_time'  : datetime.datetime.now() + datetime.timedelta(hours=1),
                    'ts_value' : str([2, 3, 4, 5, 6]),
                }
            ]
        }

        return scenario_attr

    def create_array(self, c, ResourceAttr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        scenario_attr = c.factory.create('ns1:ResourceScenario')
        
        scenario_attr.attr_id = ResourceAttr.attr_id
        scenario_attr.resource_attr_id = ResourceAttr.id
        scenario_attr.type = 'array'
        
        arr1 = c.factory.create('ns1:Array')
        arr1.arr_data = [1, 2, 3]
        arr2 = c.factory.create('ns1:Array')
        arr2.arr_data = [4, 5, 6]
        
        arr = c.factory.create('ns1:Array')
        arr.arr_data = [arr1, arr2]

        arr3 = c.factory.create('ns1:Array')
        arr3.arr_data = [10, 20, 30]
        arr4 = c.factory.create('ns1:Array')
        arr4.arr_data = [40, 50, 60]
        
        arr5 = c.factory.create('ns1:Array')
        arr5.arr_data = [arr3, arr4]

        arr6 = c.factory.create('ns1:Array')
        arr6.arr_data = [arr, arr5]

        #scenario_attr.value = arr6 
        scenario_attr.value = {
           'arr_data' : str([[[1, 2, 3], [4, 5, 6]], [[10, 20, 30],[40, 50, 60]]])
        }

        return scenario_attr

def run():
    unittest.main()

if __name__ == "__main__":
    run() # run all tests
