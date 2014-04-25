# (c) Copyright 2013, 2014, University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
import copy
import logging
log = logging.getLogger(__name__)

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
        log.debug(partial_network)

        assert len(partial_network.scenarios.Scenario) == 1
        assert len(full_network.scenarios.Scenario)    == 2
        for s in partial_network.scenarios.Scenario:
            assert len(s.resourcescenarios.ResourceScenario) > 0

    def test_get_extents(self):
        """
        Extents test: Test that the min X, max X, min Y and max Y of a
        network are retrieved correctly.
        """
        net = self.create_network_with_data()

        extents = self.client.service.get_network_extents(net.id)

        assert extents.min_x == 10
        assert extents.max_x == 100
        assert extents.min_y == 9
        assert extents.max_y == 99

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


    def test_add_link(self):
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

        link = self.client.factory.create('hyd:Link')
        link.id = i * -1
        link.name = 'New Link'
        link.description = 'Test link ' + str(i)
        link.node_1_id = network.nodes.Node[0].id
        link.node_2_id = network.nodes.Node[2].id

        tmpl = self.create_template()

        type_summary_arr = self.client.factory.create('hyd:TypeSummaryArray')

        type_summary      = self.client.factory.create('hyd:TypeSummary')
        type_summary.id   = tmpl.id
        type_summary.name = tmpl.name
        type_summary.id   = tmpl.types.TemplateType[0].id
        type_summary.name = tmpl.types.TemplateType[0].name

        type_summary_arr.TypeSummary.append(type_summary)

        link.types = type_summary_arr

        new_link = self.client.service.add_link(network.id, link)

        link_attr_ids = []
        for resource_attr in new_link.attributes.ResourceAttr:
            link_attr_ids.append(resource_attr.attr_id)

        for typeattr in tmpl.types.TemplateType[0].typeattrs.TypeAttr:
            assert typeattr.attr_id in link_attr_ids

        new_network = self.client.service.get_network(network.id)

        assert len(new_network.links.Link) == len(network.links.Link)+1; "New node was not added correctly"
        return new_network

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
            node.name = 'node ' + str(i)
            node.description = 'test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            nodes.Node.append(node)

        for i in range(nlinks):
            link = self.client.factory.create('hyd:Link')
            link.id = i * -1
            link.name = 'link ' + str(i)
            link.description = 'test link ' + str(i)
            link.node_1_id = nodes.Node[i].id
            link.node_2_id = nodes.Node[i + 1].id

            links.Link.append(link)

        network.project_id = project.id
        network.name = 'test'
        network.description = 'a network for soap unit tests.'
        network.nodes = nodes
        network.links = links

        network = self.client.service.add_network(network)

        node = self.client.factory.create('hyd:Node')
        new_node_num = nnodes + 1
        node.id = new_node_num * -1
        node.name = 'node ' + str(new_node_num)
        node.description = 'test node ' + str(new_node_num)
        node.x = 100
        node.y = 101

        
        tmpl = self.create_template()

        type_summary_arr = self.client.factory.create('hyd:TypeSummaryArray')

        type_summary      = self.client.factory.create('hyd:TypeSummary')
        type_summary.id   = tmpl.id
        type_summary.name = tmpl.name
        type_summary.id   = tmpl.types.TemplateType[0].id
        type_summary.name = tmpl.types.TemplateType[0].name

        type_summary_arr.TypeSummary.append(type_summary)

        node.types = type_summary_arr

        new_node = self.client.service.add_node(network.id, node)

        node_attr_ids = []
        for resource_attr in new_node.attributes.ResourceAttr:
            node_attr_ids.append(resource_attr.attr_id)

        for typeattr in tmpl.types.TemplateType[0].typeattrs.TypeAttr:
            assert typeattr.attr_id in node_attr_ids

        new_network = self.client.service.get_network(network.id)

        assert len(new_network.nodes.Node) == len(network.nodes.Node)+1; "new node was not added correctly"
        
        return new_network

   
    def test_update_node(self):
        network = self.test_add_node()

        node_to_update = network.nodes.Node[0]
        node_to_update.name = "Updated Node Name"

        new_node = self.client.service.update_node(node_to_update)

        new_network = self.client.service.get_network(network.id)

        updated_node = None
        for n in new_network.nodes.Node:
            if n.id == node_to_update.id:
                updated_node = n
        assert updated_node.name == "Updated Node Name" 

    def test_update_link(self):
        network = self.test_add_link()

        link_to_update = network.links.Link[0]
        link_to_update.name = "Updated link Name"

        new_link = self.client.service.update_link(link_to_update)

        new_network = self.client.service.get_network(network.id)

        updated_link = None
        for l in new_network.links.Link:
            if l.id == link_to_update.id:
                updated_link = l
        assert updated_link.name == "Updated link Name" 

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
