#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest
import logging
import shutil
import os

from HydraLib import hydra_logging, config 

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
            element.prefix = 'ns0'

        for e in element.getChildren():
            self.fix_ns(e)

def connect(login=True):
    port = config.getint('hydra_server', 'port')
    url = 'http://localhost:%s?wsdl' % port
    client = Client(url, plugins=[FixNamespace()])
    
    client.add_prefix('hyd', 'soap_server.hydra_complexmodels')
    global CLIENT
    CLIENT = client
    return client

class SoapServerTest(unittest.TestCase):

    def setUp(self):
        hydra_logging.init(level='INFO')
        logging.getLogger('suds').setLevel(logging.ERROR)
        logging.getLogger('suds.client').setLevel(logging.CRITICAL)
        # Clear SUDS cache:
        shutil.rmtree(os.path.join(tmp(), 'suds'), True)
        global CLIENT
        if CLIENT is None:
            connect()
        
        self.client = CLIENT

        self.login('root', '')
        

    def tearDown(self):
        logging.debug("Tearing down")
        hydra_logging.shutdown()
        self.logout('root')

    def login(self, username, password):
        login_response = self.client.service.login(username, password)

        session_id = login_response.session_id
        user_id    = login_response.user_id

        token = self.client.factory.create('RequestHeader')
        token.session_id = session_id
        token.username = username
        token.user_id  = user_id
      
        self.client.set_options(cache=None, soapheaders=token)

    def logout(self, username):
        msg = self.client.service.logout(username)
        return msg

    def create_user(self, name):

        existing_user = self.client.service.get_user_by_name(name)
        if existing_user is not None:
            return existing_user 

        user = self.client.factory.create('hyd:User')
        user.username = name
        user.password = "password"

        new_user = self.client.service.add_user(user)
        return new_user

    def create_project(self, name):
        project = self.client.factory.create('hyd:Project')
        project.name = 'SOAP test %s'%(datetime.datetime.now())
        project = self.client.service.add_project(project)
        return project

    def create_network(self, project):
        network = self.client.factory.create('hyd:Network')
        network.name = 'Test network @ %s'% datetime.datetime.now()
        network.description = 'A test network.'
        network.project_id = project.id
        network.nodes = []
        network.links = []
        network.scenarios = []
        network.layout = ""
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

    def create_network_with_data(self, project_id=None):
        """
            Test adding data to a network through a scenario.
            This test adds attributes to one node and then assignes data to them.
            It assigns a descriptor, array and timeseries to the 
            attributes node. 
        """
        start = datetime.datetime.now()
        if project_id is None:
            (project) = {
                'name'        : 'New Project %s'%(datetime.datetime.now()),
                'description' : 'New Project Description',
            }
            p =  self.client.service.add_project(project)
            project_id = p.id

        print "Project creation took: %s"%(datetime.datetime.now()-start)
        start = datetime.datetime.now()

        #Create some attributes, which we can then use to put data on our nodes
        attr1 = self.create_attr("testattr_1")
        attr2 = self.create_attr("testattr_2")
        attr3 = self.create_attr("testattr_3")
        attr4 = self.create_attr("testattr_4")
        attr5 = self.create_attr("group_attr")

        print "Attribute creation took: %s"%(datetime.datetime.now()-start)
        start = datetime.datetime.now()

        #Create 2 nodes
        node1 = self.create_node(-1, node_name="Node 1")
        node2 = self.create_node(-2, node_name="Node 2")

        node_array = self.client.factory.create('hyd:NodeArray')
        node_array.Node.append(node1)
        node_array.Node.append(node2)

        #From our attributes, create a resource attr for our node
        #We don't assign data directly to these resource attributes. This
        #is done when creating the scenario -- a scenario is just a set of
        #data for a given list of resource attributes.
        attr_array         = self.client.factory.create('hyd:ResourceAttrArray')
        node_attr1         = self.client.factory.create('hyd:ResourceAttr')
        node_attr1.id      = -1
        node_attr1.attr_id = attr1.id
        node_attr2         = self.client.factory.create('hyd:ResourceAttr')
        node_attr2.attr_id = attr2.id
        node_attr2.id      = -2
        node_attr3         = self.client.factory.create('hyd:ResourceAttr')
        node_attr3.attr_id = attr3.id
        node_attr3.id      = -3
        node_attr4         = self.client.factory.create('hyd:ResourceAttr')
        node_attr4.attr_id = attr4.id
        node_attr4.id      = -4
        node_attr4.attr_is_var = 'Y'
        
        group_attr         = self.client.factory.create('hyd:ResourceAttr')
        group_attr.attr_id = attr5.id
        group_attr.id      = -5

        
        attr_array.ResourceAttr.append(node_attr1)
        attr_array.ResourceAttr.append(node_attr2)
        attr_array.ResourceAttr.append(node_attr3)
        attr_array.ResourceAttr.append(node_attr4)
        node1.attributes = attr_array

        group_attrs = self.client.factory.create('hyd:ResourceAttrArray')
        group_attrs.ResourceAttr.append(group_attr)

        #Connect the two nodes with a link
        link = self.create_link(node1['id'], node2['id'])

        #A network must contain an array of links. In this case, the array
        #contains a single link
        link_array = self.client.factory.create('hyd:LinkArray')
        link_array.Link.append(link)

        print "Making nodes & links took: %s"%(datetime.datetime.now()-start)
        start = datetime.datetime.now()

        #Create the scenario
        scenario = self.client.factory.create('hyd:Scenario')
        scenario.id = -1
        scenario.name = 'Scenario 1'
        scenario.description = 'Scenario Description'

        #Multiple data (Called ResourceScenario) means an array.
        scenario_data = self.client.factory.create('hyd:ResourceScenarioArray')

        #Our node has several dmin'resource attributes', created earlier.
        node_attrs = node1['attributes']


        group_array       = self.client.factory.create('hyd:ResourceGroupArray')
        group             = self.client.factory.create('hyd:ResourceGroup')
        group.id          = -1
        group.name        = "Test Group"
        group.description = "Test group description"

        group.attributes = group_attrs 

        group_array.ResourceGroup.append(group)

        group_item_array      = self.client.factory.create('hyd:ResourceGroupItemArray')
        group_item_1          = self.client.factory.create('hyd:ResourceGroupItem')
        group_item_1.ref_key  = 'NODE'
        group_item_1.ref_id   = node1.id
        group_item_1.group_id = group.id
        group_item_2          = self.client.factory.create('hyd:ResourceGroupItem')
        group_item_2.ref_key  = 'LINK'
        group_item_2.ref_id   = link.id
        group_item_2.group_id = group.id

        group_item_array.ResourceGroupItem.append(group_item_1)
        group_item_array.ResourceGroupItem.append(group_item_2)

        scenario.resourcegroupitems = group_item_array

        #This is an example of 3 diffent kinds of data
        #A simple string (Descriptor)
        #A time series, where the value may be a 1-D array
        #A multi-dimensional array.
        descriptor = self.create_descriptor(node_attrs.ResourceAttr[0])
        timeseries = self.create_timeseries(node_attrs.ResourceAttr[1])
        array      = self.create_array(node_attrs.ResourceAttr[2])
        
        grp_timeseries = self.create_timeseries(group_attrs.ResourceAttr[0])

        scenario_data.ResourceScenario.append(descriptor)
        scenario_data.ResourceScenario.append(timeseries)
        scenario_data.ResourceScenario.append(grp_timeseries)
        scenario_data.ResourceScenario.append(array)

        #Set the scenario's data to the array we have just populated
        scenario.resourcescenarios = scenario_data

        #A network can have multiple scenarios, so they are contained in
        #a scenario array
        scenario_array = self.client.factory.create('hyd:ScenarioArray')
        scenario_array.Scenario.append(scenario)

        print "Scenario definition took: %s"%(datetime.datetime.now()-start)
        start = datetime.datetime.now()

        layout = """
            <resource_layout>
                <layout>
                    <name>color</name>
                    <value>red</value>
                </layout>
                <layout>
                    <name>shapefile</name>
                    <value>blah.shp</value>
                </layout>
            </resource_layout>
        """

        (network) = {
            'name'        : 'Network @ %s'%datetime.datetime.now(),
            'description' : 'Test network with 2 nodes and 1 link',
            'project_id'  : project_id,
            'links'       : link_array,
            'nodes'       : node_array,
            'layout'      : layout,
            'scenarios'   : scenario_array,
            'resourcegroups' : group_array,
        }
        #print network
        network = self.client.service.add_network(network)

        print "Network Creation took: %s"%(datetime.datetime.now()-start)

        return network

    def create_descriptor(self, ResourceAttr, val="test"):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        scenario_attr = self.client.factory.create('hyd:ResourceScenario')

        scenario_attr.attr_id = ResourceAttr.attr_id
        scenario_attr.resource_attr_id = ResourceAttr.id

        dataset = self.client.factory.create('hyd:Dataset')
        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit = 'metres / second'
        dataset.dimension = 'number of units per time unit'
        
        descriptor = self.client.factory.create('hyd:Descriptor')
        descriptor.desc_val = val

        dataset.value = descriptor

        scenario_attr.value = dataset

        return scenario_attr


    def create_timeseries(self, ResourceAttr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        scenario_attr = self.client.factory.create('hyd:ResourceScenario')

        scenario_attr.attr_id = ResourceAttr.attr_id
        scenario_attr.resource_attr_id = ResourceAttr.id

        dataset = self.client.factory.create('hyd:Dataset')
        
        dataset.type = 'timeseries'
        dataset.name = 'my time series'
        dataset.unit = 'feet cubed'
        dataset.dimension = 'cubic capacity'

        ts1 = self.client.factory.create('hyd:TimeSeriesData')
        ts1.ts_time  = datetime.datetime.now()
        ts1.ts_value = str([1, 2, 3, 4, 5])

        ts2 = self.client.factory.create('hyd:TimeSeriesData')
        ts2.ts_time  = datetime.datetime.now() + datetime.timedelta(hours=1)
        ts2.ts_value = str([2, 3, 4, 5, 6])

        ts3 = self.client.factory.create('hyd:TimeSeries')
        ts3.ts_values.TimeSeriesData.append(ts1)
        ts3.ts_values.TimeSeriesData.append(ts2)

        dataset.value = ts3
        scenario_attr.value = dataset

        return scenario_attr

    def create_array(self, ResourceAttr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        scenario_attr = self.client.factory.create('hyd:ResourceScenario')

        scenario_attr.attr_id = ResourceAttr.attr_id
        scenario_attr.resource_attr_id = ResourceAttr.id
        
        dataset = self.client.factory.create('hyd:Dataset')
        dataset.type = 'array'
        dataset.name = 'my array'
        dataset.unit = 'joules'
        dataset.dimension = 'pressure'

        arr = self.client.factory.create('hyd:Array')
        arr.arr_data = str([[[1, 2, 3], [5, 4, 6]],[[10, 20, 30], [40, 50, 60]]])
        
        dataset.value = arr

        scenario_attr.value = dataset

        return scenario_attr


    def create_constraint(self, net, constant=5):
        #We are trying to achieve a structure that looks like:
        #(((A + 5) * B) - C) == 0
        #3 groups
        #4 items
        #group 1 contains group 2 & item C
        #group 2 contains group 3 & item B
        #group 3 contains item A and item 5
        #The best way to do this is in reverse order. Start with the inner
        #group and work your way out.

        #create all the groups & items
        #Innermost group first (A + 5)
        group_3 = self.client.factory.create('hyd:ConstraintGroup')
        group_3.op = '+'
        
        item_1 = self.client.factory.create('hyd:ConstraintItem')
        item_1.resource_attr_id = net.nodes.Node[0].attributes.ResourceAttr[0].id
        item_2 = self.client.factory.create('hyd:ConstraintItem')
        item_2.constant = constant

        #set the items in group 3 (aka 'A' and 5 from example above)
        group_3_items = self.client.factory.create('hyd:ConstraintItemArray')
        group_3_items.ConstraintItem.append(item_1)
        group_3_items.ConstraintItem.append(item_2)
        
        group_3.constraintitems = group_3_items

        #Then the next group out (group_1 * B)
        #Group 2 (which has both an item and a group)
        group_2 = self.client.factory.create('hyd:ConstraintGroup')
        group_2.op = '*' 

        item_3 = self.client.factory.create('hyd:ConstraintItem')
        item_3.resource_attr_id = net.nodes.Node[0].attributes.ResourceAttr[1].id
       
        group_2_items = self.client.factory.create('hyd:ConstraintItemArray')
        group_2_items.ConstraintItem.append(item_3)
        
        group_2_groups = self.client.factory.create('hyd:ConstraintGroupArray')
        group_2_groups.ConstraintGroup.append(group_3)
        
        group_2.constraintgroups = group_2_groups
        group_2.constraintitems  = group_2_items

        #Then the outermost group: (group_2 - C)
        #Group 1 has  also has an item and a group
        group_1 = self.client.factory.create('hyd:ConstraintGroup')
        group_1.op = '-'

        item_4 = self.client.factory.create('hyd:ConstraintItem')
        item_4.resource_attr_id = net.nodes.Node[0].attributes.ResourceAttr[2].id


        group_1_items = self.client.factory.create('hyd:ConstraintItemArray')
        group_1_items.ConstraintItem.append(item_4)

        group_1_groups = self.client.factory.create('hyd:ConstraintGroupArray')
        group_1_groups.ConstraintGroup.append(group_2)
        group_1.constraintgroups = group_1_groups
        group_1.constraintitems = group_1_items


        constraint = self.client.factory.create('hyd:Constraint')
        constraint.scenario_id = net.scenarios[0][0].id
        constraint.op = "=="
        constraint.constant = 0

        constraint.constraintgroup = group_1
       
        test_constraint = self.client.service.add_constraint(net.scenarios[0][0].id, constraint)
        return test_constraint

def run():
    unittest.main()

if __name__ == '__main__':
    run()  # all tests
