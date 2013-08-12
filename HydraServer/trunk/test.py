import os
import unittest
from decimal import Decimal
import shutil
from tempfile import gettempdir as tmp
shutil.rmtree(os.path.join(tmp(), 'suds'), True)
from suds.client import Client

from suds.plugin import MessagePlugin
from suds.sax.attribute import Attribute

class SoapFixer(MessagePlugin):
    def marshalled(self, context):
        # Alter the envelope so that the xsd namespace is allowed
        context.envelope.nsprefixes['xsd'] = 'http://www.w3.org/2001/XMLSchema'
        # Go through every node in the document and apply the fix function to patch up incompatible XML. 
        context.envelope.walk(self.fix_any_type_string)
    def fix_any_type_string(self, element):
        """Used as a filter function with walk in order to fix errors.
        If the element has a certain name, give it a xsi:type=xsd:int. Note that the nsprefix xsd must also
         be added in to make this work."""

        # Fix elements which have these names
        fix_names = ['value']
        if element.name in fix_names:
            element.attributes.append(Attribute('xsi:type', 'xsd:string'))


plugin=SoapFixer()

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
        c = Client('http://localhost:8000/?wsdl', plugins=[plugin])
        print c

        (project) = {
            'project_name' : 'New Project',
            'project_description' : 'New Project Description',
        }
        p =  c.service.add_project(project)

        attr = self.create_attr(c) 

        attr_array = c.factory.create('ns5:ResourceAttrArray')
        node_attr  = c.factory.create('ns5:ResourceAttr')
        node_attr.attr_id = attr.attr_id
        attr_array.ResourceAttr.append(node_attr)

        node1 = self.create_node(c, "Node 1", attributes=attr_array)
        node2 = self.create_node(c, "Node 2")

        link = self.create_link(c, 'link 1', node1['node_id'], node2['node_id'])

        link_array = c.factory.create('ns5:LinkArray')
        link_array.Link.append(link)

        scenario_array = c.factory.create('ns5:ScenarioArray')
        
        scenario = c.factory.create('ns5:Scenario')
        scenario.scenario_name = 'Scenario 1'
        scenario.scenario_description = 'Scenario Description'
        scenario_array.Scenario.append(scenario)

        scenario_data = c.factory.create('ns5:ScenarioAttrArray')  
        
        scenario_attr = c.factory.create('ns5:ScenarioAttr')
        node_attrs = node1.attributes

        scenario_attr.attr_id = node_attrs.ResourceAttr[0].attr_id
        scenario_attr.resource_attr_id = node_attrs.ResourceAttr[0].resource_attr_id

        descriptor  = c.factory.create('ns1:Descriptor')

        descriptor.desc_val = "I am a value"
        scenario_attr.value = "I am a value" 

        scenario_data.ScenarioAttr.append(scenario_attr)

        scenario.data = scenario_data

        network = self.create_network(c, p['project_id'], 'Network1', 'Test Network with 2 nodes and 1 link',links=link_array,scenarios=scenario_array)

        print c.last_sent()
        print network
        print c.service.testf()
        print c.last_received()

def run():
    unittest.main()

if __name__ == "__main__":
    run() # run all tests
