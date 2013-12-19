#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
import copy


class NetworkTest(test_SoapServer.SoapServerTest):

    def test_update(self):
        project = self.create_project('test')
        network = self.client.factory.create('hyd:Network')
        nodes = self.client.factory.create('hyd:NodeArray')
        links = self.client.factory.create('hyd:LinkArray')

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = self.client.factory.create('hyd:Node')
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.Node.append(node)

        for i in range(nlinks):
            link = self.client.factory.create('hyd:Link')
            link.id = 1 * -1
            link.name = 'Link ' + str(i)
            link.description = 'Test link ' + str(i)
            link.node_1_id = nodes.Node[i].id
            link.node_2_id = nodes.Node[i + 1].id

            links.Link.append(link)

        network.project_id = project.id
        network.name = 'Test'
        network.description = 'A network for SOAP unit tests.'
        network.nodes = nodes
        network.links = links

        network = self.client.service.add_network(network)

        new_network = copy.deepcopy(network)

        new_network.links.Link[1].node_1_id = nodes.Node[2].id
        new_network.links.Link[1].node_2_id = nodes.Node[1].id

        new_network.description = \
            'A different network for SOAP unit tests.'

        new_network = self.client.service.update_network(new_network)

        assert network.id == new_network.id, \
            'network_id has changed on update.'
        assert network.name == new_network.name, \
            "network_name changed on update."
        assert network.description != new_network.description,\
            "project_description did not update"
        assert new_network.description == \
            'A different network for SOAP unit tests.', \
            "Update did not work correctly."


    def test_add_node(self):
        project = self.create_project('test')
        network = self.client.factory.create('hyd:Network')
        nodes = self.client.factory.create('hyd:NodeArray')
        links = self.client.factory.create('hyd:LinkArray')

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = self.client.factory.create('hyd:Node')
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.Node.append(node)

        for i in range(nlinks):
            link = self.client.factory.create('hyd:Link')
            link.id = i * -1
            link.name = 'Link ' + str(i)
            link.description = 'Test link ' + str(i)
            link.node_1_id = nodes.Node[i].id
            link.node_2_id = nodes.Node[i + 1].id

            links.Link.append(link)

        network.project_id = project.id
        network.name = 'Test'
        network.description = 'A network for SOAP unit tests.'
        network.nodes = nodes
        network.links = links

        network = self.client.service.add_network(network)

        old_network = copy.deepcopy(network)

        node = self.client.factory.create('hyd:Node')
        new_node_num = nnodes + 1
        node.id = new_node_num * -1
        node.name = 'Node ' + str(new_node_num)
        node.description = 'Test node ' + str(new_node_num)
        node.x = 100
        node.y = 101

        link = self.client.factory.create('hyd:Link')
        new_link_num = nlinks+1
        link.id = new_link_num * -1
        link.name = 'Link ' + str(new_link_num)
        link.description = 'Test link ' + str(new_link_num)
        link.node_1_id = node.id
        link.node_2_id = network.nodes.Node[0].id

        links.Link.append(link)

        network.nodes.Node.append(node)

        network.description = \
            'A different network for SOAP unit tests.'

        new_network = self.client.service.update_network(network)

        assert network.id == old_network.id, \
            'network_id has changed on update.'

        assert len(new_network.nodes.Node) == len(old_network.nodes.Node)+1; "New node was not added correctly"

    def test_load(self):
        project = self.create_project('test')
        network = self.client.factory.create('hyd:Network')
        nodes = self.client.factory.create('hyd:NodeArray')
        links = self.client.factory.create('hyd:LinkArray')

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = self.client.factory.create('hyd:Node')
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.Node.append(node)

        for i in range(nlinks):
            link = self.client.factory.create('hyd:Link')
            link.id = i * -1
            link.name = 'Link ' + str(i)
            link.description = 'Test link ' + str(i)
            link.node_1_id = nodes.Node[i].id
            link.node_2_id = nodes.Node[i + 1].id

            links.Link.append(link)

        network.project_id = project.id
        network.name = 'Test'
        network.description = 'A network for SOAP unit tests.'
        network.nodes = nodes
        network.links = links

        network = self.client.service.add_network(network)

        new_network = self.client.service.get_network(network.id)

        assert network.name == new_network.name, \
            "network_name has changed."
        assert network.description == new_network.description,\
            "project_description did not load correctly"

    def test_delete(self):
        project = self.create_project('test')
        network = self.client.factory.create('hyd:Network')
        nodes = self.client.factory.create('hyd:NodeArray')
        links = self.client.factory.create('hyd:LinkArray')

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = self.client.factory.create('hyd:Node')
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.Node.append(node)

        for i in range(nlinks):
            link = self.client.factory.create('hyd:Link')
            link.id = i * -1
            link.name = 'Link ' + str(i)
            link.description = 'Test link ' + str(i)
            link.node_1_id = nodes.Node[i].id
            link.node_2_id = nodes.Node[i + 1].id
            #link = self.client.service.add_link(link)

            links.Link.append(link)

        network.project_id = project.id
        network.name = 'Test'
        network.description = 'A network for SOAP unit tests.'
        network.nodes = nodes
        network.links = links

        network = self.client.service.add_network(network)

        self.client.service.delete_network(network.id)

        assert self.client.service.get_network(network.id).status == 'X', \
            'Deleting network did not work correctly.'


    def test_validate_topology(self):
        project = self.create_project('test')
        network = self.client.factory.create('hyd:Network')
        nodes = self.client.factory.create('hyd:NodeArray')
        links = self.client.factory.create('hyd:LinkArray')

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = self.client.factory.create('hyd:Node')
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.Node.append(node)

        #NOTE: NOT ADDING ENOUGH LINKS!!
        for i in range(nlinks-1):
            link = self.client.factory.create('hyd:Link')
            link.id = i * -1
            link.name = 'Link ' + str(i)
            link.description = 'Test link ' + str(i)
            link.node_1_id = nodes.Node[i].id
            link.node_2_id = nodes.Node[i + 1].id

            links.Link.append(link)

        network.project_id = project.id
        network.name = 'Test'
        network.description = 'A network for SOAP unit tests.'
        network.nodes = nodes
        network.links = links

        network = self.client.service.add_network(network)
        
        result = self.client.service.validate_network_topology(network.id)
        assert result == 'Orphan nodes are present.'

if __name__ == '__main__':
    test_SoapServer.run()
