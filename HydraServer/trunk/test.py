import os
import unittest
from decimal import Decimal
import shutil
from tempfile import gettempdir as tmp
shutil.rmtree(os.path.join(tmp(), 'suds'), True)
from suds.client import Client

class TestSoap(unittest.TestCase):
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
        print Node1

        (Node2) = {
            'node_name' : 'Node 2',
            'node_description' : 'Node Two Description',
            'node_x' : Decimal('3.00'),
            'node_y' : Decimal('4.00'),
        }
        Node2 = c.service.add_node(Node2)
        print Node2

    def test_network(self):
        c = Client('http://localhost:8000/?wsdl')
        (project) = {
            'project_name' : 'New Project',
            'project_description' : 'New Project Description',
        }
        p =  c.service.add_project(project)

        (Node1) = {
            'node_name' : 'Node One',
            'node_description' : 'Node One Description',
            'node_x' : Decimal('1.50'),
            'node_y' : Decimal('2.00'),
        }

        Node1 = c.service.add_node(Node1)
        print Node1

        (Node2) = {
            'node_name' : 'Node 2',
            'node_description' : 'Node Two Description',
            'node_x' : Decimal('3.00'),
            'node_y' : Decimal('4.00'),
        }
        Node2 = c.service.add_node(Node2)
        print Node2

        c = Client('http://localhost:8000/?wsdl')
        Link = c.factory.create('ns5:Link')
        Link.link_name = 'Link 1'
        Link.link_description = 'Link One Description'
        Link.node_1_id = Node1['node_id']
        Link.node_2_id = Node2['node_id']
        print Link


        LinkArray = c.factory.create('ns5:LinkArray')
        LinkArray.Link.append(Link)
        print LinkArray
        (Network) = {
            'network_name'        : 'Network1',
            'network_description' : 'Test Network with 2 nodes and 1 link',
            'project_id'          : p['project_id'],
            'links'              : LinkArray,
        }
        Network = c.service.add_network(Network)
        print Network

def run():
   # hydra_logging.init(level='DEBUG')
   # HydraIface.init(hdb.connect())
    unittest.main()
   # hdb.disconnect()
   # hydra_logging.shutdown()

if __name__ == "__main__":
    run() # run all tests
