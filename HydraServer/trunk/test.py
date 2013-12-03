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
from HydraLib import config

class TestSoap(unittest.TestCase):
    def setUp(self):
        self.seq = range(10)
        url = config.get('hydra_client', 'url')
        print "Connecting to %s"%url

        self.c = Client(url)
        session_id = self.c.service.login('root', '')

        token = self.c.factory.create('RequestHeader')
        token.session_id = session_id
        token.username = 'root'

        self.c.set_options(soapheaders=token)

    def create_node(self,node_id,name,desc="Node Description", x=0, y=0, attributes=None):
        """
            Create a node using the suds library
        """
        node = self.c.factory.create('ns1:Node')
        node.id = node_id
        node.name = name
        node.description = desc
        node.x = x
        node.y = y
        node_layout = """
            <resource_layout>
                <layout>
                    <name>color</name>
                    <value>#FFFFFF</value>
                </layout>
            </resource_layout>
        """
        node.layout = node_layout

        return node

    def create_link(self,name,node_1_id, node_2_id):
        """
            Create a link using the suds library
        """
        link = self.c.factory.create('ns1:Link')
        link.name = name
        link.description = 'Link from %s to %s'%(node_1_id, node_2_id)
        link.node_1_id = node_1_id
        link.node_2_id = node_2_id
        link_layout = """
            <resource_layout>
                <layout>
                    <name>color</name>
                    <value>#FFFFFF</value>
                </layout>
            </resource_layout>
        """
        link.layout = link_layout

        return link

    def create_attr(self, name):
        """
            Create an attribute. No two attributes can have the same name,
            so first check if the attribute exists. If it does, just return it. 
            If not, create and save a new one.
        """
        attr = self.c.service.get_attribute(name)
        if attr is None:
            attr       = self.c.factory.create('ns1:Attr')
            attr.name  = name
            attr.dimen = 'dimensionless'
            attr       = self.c.service.add_attribute(attr)
        return attr

    def create_network(self, project_id, name, desc=None, nodes=None, links=None, scenarios=None):
        """
            Create an entire suds network including nodes, links & scenarios
        """

        network_layout = """ <resource_layout>
                <layout>
                    <name>color</name>
                    <value>#FFFFFF</value>
                </layout>
            </resource_layout>"""

        (network) = {
            'name'        : "%s, %s"%(name, datetime.datetime.now()),
            'description' : desc,
            'project_id'  : project_id,
            'links'       : links,
            'nodes'       : nodes,
            'layout'      : network_layout,
            'scenarios'   : scenarios,
        }
        #print network
        network = self.c.service.add_network(network)

        return network

    def test_add_project(self):
        """
            Test adding a new project.
        """
        (project) = {
            'name' : 'New Project at %s'%datetime.datetime.now(),
            'description' : 'New Project Description',
        }
        p =  self.c.service.add_project(project)
        print p
        p1 =  self.c.service.get_project(p['id'])
        print p1
        (project1) = {
            'id'   : p['id'],
            'name' : 'Updated Project at %s'%datetime.datetime.now(),
            'description' : 'Updated Project Description',
        }

        p2 = self.c.service.update_project(project1)
        #print self.c.last_sent()
        print p2

    def test_network(self):
        """
            Test adding a new network. 
        """

        start = datetime.datetime.now()
        print "Time until project creation: %s"%(datetime.datetime.now()-start)
        project_start = datetime.datetime.now()

        (project) = {
            'name' : 'New Project at %s'%datetime.datetime.now(),
            'description' : 'New Project Description',
        }
        p =  self.c.service.add_project(project)

        print "Project created in: %s"%(datetime.datetime.now()-project_start)

        NodeArray = self.c.factory.create('ns1:NodeArray')
        node1 = self.create_node(-1, "Node 1")
        node2 = self.create_node(-2, "Node 2")
        node3 = self.create_node(-3, "Node 3")
        NodeArray.Node.append(node1)
        NodeArray.Node.append(node2)
        NodeArray.Node.append(node3)

        link1 = self.create_link('link 1', node1.id, node2.id)
        link2 = self.create_link('link 1', node2.id, node3.id)

        LinkArray = self.c.factory.create('ns1:LinkArray')
        LinkArray.Link.append(link1)
        LinkArray.Link.append(link2)
        
        attr1 = self.create_attr("testattr_1")
        network_attrs = self.c.factory.create('ns1:ResourceAttrArray')
        net_attr1  = self.c.factory.create('ns1:ResourceAttr')
        net_attr1.id = -1
        net_attr1.attr_id = attr1.id
        network_attrs.ResourceAttr.append(net_attr1)

        network_layout = """
            <resource_layout>
                <layout>
                    <name>color</name>
                    <value>#FFFFFF</value>
                </layout>
            </resource_layout>
        """

        (Network) = {
            'name'        : 'Network1',
            'description' : 'Test Network with 2 nodes and 1 link',
            'project_id'  : p['id'],
            'links'       : LinkArray,
            'nodes'       : NodeArray,
            'attributes'  : network_attrs,
            'layout'      : network_layout
 
        }
        print "Time until network creation: %s"%(datetime.datetime.now()-start)

        Network = self.c.service.add_network(Network)
        print "Total test time was: %s"%(datetime.datetime.now()-start)
        assert Network is not None, "Network did not create correctly"

    def test_scenario(self):
        """
            Test adding data to a network through a scenario.
            This test adds attributes to one node and then assignes data to them.
            It assigns a descriptor, array and timeseries to the 
            attributes node. 
        """
        start = datetime.datetime.now()
        (project) = {
            'name' : 'New Project at %s'%datetime.datetime.now(),
            'description' : 'New Project Description',
        }
        p =  self.c.service.add_project(project)

        print "Project creation took: %s"%(datetime.datetime.now()-start)
        start = datetime.datetime.now()

        #Create some attributes, which we can then use to put data on our nodes
        attr1 = self.create_attr("testattr_1")
        attr2 = self.create_attr("testattr_2")
        attr3 = self.create_attr("testattr_3")

        print "Attribute creation took: %s"%(datetime.datetime.now()-start)
        start = datetime.datetime.now()

        #Create 2 nodes
        node1 = self.create_node(-1, "Node 1")
        node2 = self.create_node(-2, "Node 2")

        node_array = self.c.factory.create('ns1:NodeArray')
        node_array.Node.append(node1)
        node_array.Node.append(node2)

        #From our attributes, create a resource attr for our node
        #We don't assign data directly to these resource attributes. This
        #is done when creating the scenario -- a scenario is just a set of
        #data for a given list of resource attributes.
        attr_array = self.c.factory.create('ns1:ResourceAttrArray')
        node_attr1  = self.c.factory.create('ns1:ResourceAttr')
        node_attr1.id = -1
        node_attr1.attr_id = attr1.id
        node_attr2  = self.c.factory.create('ns1:ResourceAttr')
        node_attr2.attr_id = attr2.id
        node_attr2.id = -2
        node_attr3  = self.c.factory.create('ns1:ResourceAttr')
        node_attr3.attr_id = attr3.id
        node_attr3.id = -3
        attr_array.ResourceAttr.append(node_attr1)
        attr_array.ResourceAttr.append(node_attr2)
        attr_array.ResourceAttr.append(node_attr3)

        node1.attributes = attr_array

        #Connect the two nodes with a link
        link = self.create_link('link 1', node1['id'], node2['id'])

        #A network must contain an array of links. In this case, the array
        #contains a single link
        link_array = self.c.factory.create('ns1:LinkArray')
        link_array.Link.append(link)

        print "Making nodes & links took: %s"%(datetime.datetime.now()-start)
        start = datetime.datetime.now()

        #Create the scenario
        scenario = self.c.factory.create('ns1:Scenario')
        scenario.id = -1
        scenario.name = 'Scenario 1'
        scenario.description = 'Scenario Description'

        #Multiple data (Called ResourceScenario) means an array.
        scenario_data = self.c.factory.create('ns1:ResourceScenarioArray')

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
        scenario_array = self.c.factory.create('ns1:ScenarioArray')
        scenario_array.Scenario.append(scenario)

        print "Scenario definition took: %s"%(datetime.datetime.now()-start)
        start = datetime.datetime.now()

        network = self.create_network(p['id'], 'Network1', 'Test Network with 2 nodes and 1 link',nodes=node_array,links=link_array,scenarios=scenario_array)
        print self.c.last_sent()

        print "Network Creation took: %s"%(datetime.datetime.now()-start)
        #print "****************************"
        #print network
        #print c.last_received()

    def create_descriptor(self, ResourceAttr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        scenario_attr = self.c.factory.create('ns1:ResourceScenario')

        scenario_attr.attr_id = ResourceAttr.attr_id
        scenario_attr.resource_attr_id = ResourceAttr.id

        dataset = self.c.factory.create('ns1:Dataset')
        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit = 'metres / second'
        dataset.dimension = 'number of units per time unit'
        
        descriptor = self.c.factory.create('ns1:Descriptor')
        descriptor.desc_val = 'test'

        dataset.value = descriptor

        scenario_attr.value = dataset

        return scenario_attr


    def create_timeseries(self, ResourceAttr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        scenario_attr = self.c.factory.create('ns1:ResourceScenario')

        scenario_attr.attr_id = ResourceAttr.attr_id
        scenario_attr.resource_attr_id = ResourceAttr.id

        dataset = self.c.factory.create('ns1:Dataset')
        
        dataset.type = 'timeseries'
        dataset.name = 'my time series'
        dataset.unit = 'feet cubed'
        dataset.dimension = 'cubic capacity'

        ts1 = self.c.factory.create('ns1:TimeSeriesData')
        ts1.ts_time  = datetime.datetime.now()
        ts1.ts_value = str([1, 2, 3, 4, 5])

        ts2 = self.c.factory.create('ns1:TimeSeriesData')
        ts2.ts_time  = datetime.datetime.now() + datetime.timedelta(hours=1)
        ts2.ts_value = str([2, 3, 4, 5, 6])

        ts3 = self.c.factory.create('ns1:TimeSeries')
        ts3.ts_values.TimeSeriesData.append(ts1)
        ts3.ts_values.TimeSeriesData.append(ts2)

        dataset.value = ts3
        scenario_attr.value = dataset

        return scenario_attr

    def create_array(self, ResourceAttr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        scenario_attr = self.c.factory.create('ns1:ResourceScenario')

        scenario_attr.attr_id = ResourceAttr.attr_id
        scenario_attr.resource_attr_id = ResourceAttr.id
        
        dataset = self.c.factory.create('ns1:Dataset')
        dataset.type = 'array'
        dataset.name = 'my array'
        dataset.unit = 'joules'
        dataset.dimension = 'pressure'

        arr = self.c.factory.create('ns1:Array')
        arr.arr_data = str([[[1, 2, 3], [5, 4, 6]],[[10, 20, 30], [40, 50, 60]]])
        
        dataset.value = arr

        scenario_attr.value = dataset

        return scenario_attr

def run():

    #import profile
    #profile.run('unittest.main()')
    unittest.main()


if __name__ == "__main__":
    run() # run all tests
