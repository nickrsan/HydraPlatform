#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
import copy


class NetworkTest(test_SoapServer.SoapServerTest):

    def test_update(self):
        cli = test_SoapServer.SoapServerTest.connect(self)
        project = test_SoapServer.SoapServerTest.create_project(self, 'test')
        network = cli.factory.create('hyd:Network')
        nodes = cli.factory.create('hyd:NodeArray')
        links = cli.factory.create('hyd:LinkArray')

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = cli.factory.create('hyd:Node')
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.Node.append(node)

        for i in range(nlinks):
            link = cli.factory.create('hyd:Link')
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

        network = cli.service.add_network(network)

        new_network = copy.deepcopy(network)

        new_network.links.Link[1].node_1_id = nodes.Node[2].id
        new_network.links.Link[1].node_2_id = nodes.Node[1].id

        new_network.description = \
            'A different network for SOAP unit tests.'

        new_network = cli.service.update_network(new_network)

        assert network.id == new_network.id, \
            'network_id has changed on update.'
        assert network.name == new_network.name, \
            "network_name changed on update."
        assert network.description != new_network.description,\
            "project_description did not update"
        assert new_network.description == \
            'A different network for SOAP unit tests.', \
            "Update did not work correctly."

    def test_load(self):
        cli = test_SoapServer.SoapServerTest.connect(self)
        project = test_SoapServer.SoapServerTest.create_project(self, 'test')
        network = cli.factory.create('hyd:Network')
        nodes = cli.factory.create('hyd:NodeArray')
        links = cli.factory.create('hyd:LinkArray')

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = cli.factory.create('hyd:Node')
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.Node.append(node)

        for i in range(nlinks):
            link = cli.factory.create('hyd:Link')
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

        network = cli.service.add_network(network)

        new_network = cli.service.get_network(network.id)

        assert network.name == new_network.name, \
            "network_name has changed."
        assert network.description == new_network.description,\
            "project_description did not load correctly"

    def test_delete(self):
        cli = test_SoapServer.SoapServerTest.connect(self)
        project = test_SoapServer.SoapServerTest.create_project(self, 'test')
        network = cli.factory.create('hyd:Network')
        nodes = cli.factory.create('hyd:NodeArray')
        links = cli.factory.create('hyd:LinkArray')

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = cli.factory.create('hyd:Node')
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.Node.append(node)

        for i in range(nlinks):
            link = cli.factory.create('hyd:Link')
            link.id = i * -1
            link.name = 'Link ' + str(i)
            link.description = 'Test link ' + str(i)
            link.node_1_id = nodes.Node[i].id
            link.node_2_id = nodes.Node[i + 1].id
            #link = cli.service.add_link(link)

            links.Link.append(link)

        network.project_id = project.id
        network.name = 'Test'
        network.description = 'A network for SOAP unit tests.'
        network.nodes = nodes
        network.links = links

        network = cli.service.add_network(network)

        cli.service.delete_network(network.id)

        assert cli.service.get_network(network.id).status == 'X', \
            'Deleting network did not work correctly.'

if __name__ == '__main__':
    test_SoapServer.run()
