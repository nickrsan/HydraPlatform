import os
import datetime
import unittest
import shutil
from tempfile import gettempdir as tmp
shutil.rmtree(os.path.join(tmp(), 'suds'), True)
import test_SoapServer

class TestConstraint(test_SoapServer.SoapServerTest):

    def create_network(self, project_id, name, desc=None, nodes=None, links=None, scenarios=None):
        (network) = {
            'name'        : name,
            'description' : desc,
            'project_id'  : project_id,
            'links'       : links,
            'nodes'       : nodes,
            'scenarios'   : scenarios,
        }
        #print network
        network = self.client.service.add_network(network)
     
        return network
    
    def create_network_with_scenario(self):
        start = datetime.datetime.now()
        (project) = {
            'name'        : 'New Project',
            'description' : 'New Project Description',
        }
        p =  self.client.service.add_project(project)

      #  print "Project creation took: %s"%(datetime.datetime.now()-start)
        start = datetime.datetime.now()
        
        #Create some attributes, which we can then use to put data on our nodes
        attr1 = self.create_attr("attr1") 
        attr2 = self.create_attr("attr2") 
        attr3 = self.create_attr("attr3") 

       # print "Attribute creation took: %s"%(datetime.datetime.now()-start)
        start = datetime.datetime.now()

        #Create 2 nodes
        node1 = self.create_node(-1)
        node2 = self.create_node(-2)

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

       # print "Making nodes & links took: %s"%(datetime.datetime.now()-start)
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

        #Create 3 scalars which are to be used in my contraint.
        val_1 = self.create_scalar(node_attrs.ResourceAttr[0], 1)
        val_2 = self.create_scalar(node_attrs.ResourceAttr[1], 2)
        val_3 = self.create_scalar(node_attrs.ResourceAttr[2], 3)

        scenario_data.ResourceScenario.append(val_1)
        scenario_data.ResourceScenario.append(val_2)
        scenario_data.ResourceScenario.append(val_3)

        #Set the scenario's data to the array we have just populated
        scenario.resourcescenarios = scenario_data

        #A network can have multiple scenarios, so they are contained in
        #a scenario array
        scenario_array = self.client.factory.create('ns1:ScenarioArray')
        scenario_array.Scenario.append(scenario)

        #print "Scenario definition took: %s"%(datetime.datetime.now()-start)
        start = datetime.datetime.now()

        network = self.create_network(p['id'], 'Network1', 'Test Network with 2 nodes and 1 link',nodes=node_array,links=link_array,scenarios=scenario_array)

        #print "Network Creation took: %s"%(datetime.datetime.now()-start)

        all_net_scenarios = self.client.service.get_scenarios(network.id)

        assert len(all_net_scenarios.Scenario) == 1, "Scenarios were not retrieved correctly"
        return network
        
        #print network
        #print c.last_received()

    def create_scalar(self, ResourceAttr, value):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        scenario_attr = self.client.factory.create('ns1:ResourceScenario')
        scenario_attr.attr_id = ResourceAttr.attr_id
        scenario_attr.resource_attr_id = ResourceAttr.id
        
        dataset = self.client.factory.create('ns1:Dataset')
        dataset.value = {'param_value' : value}
        dataset.type = 'scalar'

        scenario_attr.value = dataset


        return scenario_attr

    def test_constraint(self):

        net = self.create_network_with_scenario()
        
        constraint = self.client.factory.create('ns1:Constraint')
        constraint.scenario_id = net.scenarios[0][0].id
        constraint.op = "=="
        constraint.constant = 0

        outer_group = self.client.factory.create('ns1:ConstraintGroup')
        outer_group.op = '+'


        outer_group_array = self.client.factory.create('ns1:ConstraintGroupArray')

        #this is empty, as there are no groups within the inner group.
        inner_group_array = self.client.factory.create('ns1:ConstraintGroupArray')

        inner_group = self.client.factory.create('ns1:ConstraintGroup')
        inner_group.op = '+'

        item_1 = self.client.factory.create('ns1:ConstraintItem')
        item_1.resource_attr_id = net.nodes.Node[0].attributes.ResourceAttr[0].id
        item_2 = self.client.factory.create('ns1:ConstraintItem')
        item_2.resource_attr_id = net.nodes.Node[0].attributes.ResourceAttr[1].id
        item_3 = self.client.factory.create('ns1:ConstraintItem')
        item_3.resource_attr_id = net.nodes.Node[0].attributes.ResourceAttr[2].id

        #set the items within the inner group
        inner_item_array = self.client.factory.create('ns1:ConstraintItemArray')
        inner_item_array.ConstraintItem.append(item_2)
        inner_item_array.ConstraintItem.append(item_3)
        inner_group.items = inner_item_array
        #set the empty inner group.
        inner_group.groups = inner_group_array

        outer_item_array = self.client.factory.create('ns1:ConstraintItemArray')
        outer_item_array.ConstraintItem.append(item_1)

        outer_group_array.ConstraintGroup.append(inner_group)

        outer_group.groups = outer_group_array
        outer_group.items = outer_item_array

        constraint.group = outer_group
       
        #print outer_group

        test_constraint = self.client.service.add_constraint(net.scenarios[0][0].id, constraint)

        #print test_constraint


if __name__ == "__main__":
    test_SoapServer.run()
