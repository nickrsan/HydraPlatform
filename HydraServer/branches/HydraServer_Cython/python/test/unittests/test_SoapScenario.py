#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer


class ScenarioTest(test_SoapServer.SoapServerTest):

    def test_update(self):
        cli = test_SoapServer.SoapServerTest.connect(self)

        project = test_SoapServer.SoapServerTest.create_project(self, 'Test')

        # Create some attributes
        attr1 = test_SoapServer.SoapServerTest.create_attr(self)
        attr2 = test_SoapServer.SoapServerTest.create_attr(self)
        attr3 = test_SoapServer.SoapServerTest.create_attr(self)

        # Create node attributes
        attr_array = cli.factory.create('hyd:ResourceAttrArray')
        node_attr1 = cli.factory.create('hyd:ResourceAttr')
        node_attr1.attr_id = attr1.attr_id
        node_attr2 = cli.factory.create('hyd:ResourceAttr')
        node_attr2.attr_id = attr2.attr_id
        node_attr3 = cli.factory.create('hyd:ResourceAttr')
        node_attr3.attr_id = attr3.attr_id
        attr_array.ResourceAttr.append(node_attr1)
        attr_array.ResourceAttr.append(node_attr2)
        attr_array.ResourceAttr.append(node_attr3)

        # Create nodes and a link between.
        node1 = test_SoapServer.SoapServerTest.create_node(self,
                attributes=attr_array)
        node2 = test_SoapServer.SoapServerTest.create_node(self)

        link = test_SoapServer.SoapServerTest.create_link(self,
                network, node1, node2)

        # Links are stored in the network as a link array.
        link_array = cli.factory.create('hyd:LinkArray')
        link_array.Link.append(link)

        # Create a scenario
        scenario = cli.factory.create('hyd:Scenario')
        scenario.scenario_name = 'Scenario 1'
        scenario.scenario_description = 'A scenario.'

        # All the data sets that belong to a scenario are store in an array
        scenario_data = cli.factory.create('hyd:ResourceScenarioArray')

        node_attrs = node1.attributes

        # Create the data to be assigned to each attribute:

        # An array:
        array = cli.factory.create('hyd:Array')
        array.arr_data = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]


        # A descriptor

        # A time series
