#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
import copy
import logging

class NetworkTest(test_SoapServer.SoapServerTest):

    def test_get_network(self):
        """
            Test for the potentially likely case of creating a network with two
            scenarios, then querying for the network without data to identify
            the scenarios, then querying for the network with data but in only
            a select few scenarios.
        """
        net = self.create_network_with_data()
        scenario_id = net.scenarios.Scenario[0].id       

        new_scenario = self.client.service.clone_scenario(scenario_id)

        full_network = self.client.service.get_network(new_scenario.network_id, 'N')

        for s in full_network.scenarios.Scenario:
            assert s.resourcescenarios is None
        
        partial_network = self.client.service.get_network(new_scenario.network_id, 'Y', [scenario_id])
        logging.debug(partial_network)

        assert len(partial_network.scenarios.Scenario) == 1
        assert len(full_network.scenarios.Scenario)    == 2
        for s in partial_network.scenarios.Scenario:
            assert len(s.resourcescenarios.ResourceScenario) > 0

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

    def test_consistency_of_update(self):
        """
            Test to ensure that updating a network which has not changed
            does not cause any changes to the network.
            Procedure:
                1: Create a network.
                2: Immediately update the network without changing it.
                3: Check that the original network and the updated network
                   are identical.
        """
        net = self.create_network_with_data()

        for node in net.nodes.Node:
            assert node.types is not None and  len(node.types) > 0

        updated_net = self.client.service.update_network(net)

        for node in updated_net.nodes.Node:
            assert node.types is not None and  len(node.types) > 0

        for attr in net.__keylist__:
            a = net.__getitem__(attr)
            b = updated_net.__getitem__(attr)
            #assert str(a) == str(b)
            if attr == 'scenarios':
                for s0 in net.scenarios.Scenario:
                    for s1 in updated_net.scenarios.Scenario:
                        if s0.id == s1.id:
                            for rs0 in s0.resourcescenarios.ResourceScenario:
                                for rs1 in s1.resourcescenarios.ResourceScenario:
                                    if rs0.resource_attr_id == rs1.resource_attr_id:
                                        assert str(rs0.value) == str(rs1.value)
            else:
                assert str(a) == str(b)

if __name__ == '__main__':
    test_SoapServer.run()
