import os
import shutil
from tempfile import gettempdir as tmp
shutil.rmtree(os.path.join(tmp(), 'suds'), True)

from suds.client import Client
c = Client('http://localhost:8000/?wsdl')
print c
#print c.service.say_hello('punk',10)
#print c.service.add_project('New SOAP Project')
(project) = {
    'project_name' : 'New Project',
    'project_description' : 'New Project Description',
}
p =  c.service.add_project(project)
#print p
p1 =  c.service.get_project(p['project_id'])
#print p1
(project1) = {
    'project_id'   : p['project_id'],
    'project_name' : 'Updated Project',
    'project_description' : 'Updated Project Description',
}
p2 = c.service.update_project(project1)
print p2


(Node1) = {
    'node_name' : 'Node One',
    'node_description' : 'Node One Description',
    'node_x' : 1.00,
    'node_y' : 1.00,
}

Node1 = c.service.add_node(Node1)
print Node1

(Node2) = {
    'node_name' : 'Node 2',
    'node_description' : 'Node Two Description',
    'node_x' : 2.00,
    'node_y' : 2.00,
}
Node2 = c.service.add_node(Node2)
print Node2

(Link) = {
    'link_name'        : 'Link 1',
    'link_description' : 'Link One Description',
    'node_1_id'        : Node1['node_id'],
    'node_2_id'        : Node2['node_id'],
}
print Link
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

