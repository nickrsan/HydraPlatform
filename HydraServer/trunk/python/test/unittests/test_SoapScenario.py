#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
import datetime


class ScenarioTest(test_SoapServer.SoapServerTest):

    def test_update(self):

        network =  self.create_network_with_data()
        
        scenario = network.scenarios.Scenario[0]
        scenario_id = scenario.id

        resource_scenario = scenario.resourcescenarios.ResourceScenario[0]
        resource_attr_id = resource_scenario.resource_attr_id

        dataset = self.client.factory.create('ns1:Dataset')
       
        dataset = self.client.factory.create('ns1:Dataset')
        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit = 'metres / second'
        dataset.dimension = 'number of units per time unit'
        
        descriptor = self.client.factory.create('ns1:Descriptor')
        descriptor.desc_val = 'I am an updated test!'

        dataset.value = descriptor

        new_resource_scenario = self.client.service.add_data_to_attribute(scenario_id, resource_attr_id, dataset)


        assert new_resource_scenario.value.value.desc_val == 'I am an updated test!'; "Value was not updated correctly!!"

    def test_add_scenario(self):
        """
            Test adding a new scenario to a network.
        """
        network = self.create_network_with_data()

        #Create the new scenario
        scenario = self.client.factory.create('hyd:Scenario')
        scenario.id = -1
        scenario.name = 'Scenario 2'
        scenario.description = 'Scenario 2 Description'

        #Multiple data (Called ResourceScenario) means an array.
        scenario_data = self.client.factory.create('hyd:ResourceScenarioArray')
       
        for node in network.nodes.Node:
            if node.attributes is not None and len(node.attributes) > 0:
                node1 = node

        link = network.links.Link[0]

        #Our node has several dmin'resource attributes', created earlier.
        node_attrs = node1.attributes

        group_item_array      = self.client.factory.create('hyd:ResourceGroupItemArray')
        group_item_1          = self.client.factory.create('hyd:ResourceGroupItem')
        group_item_1.ref_key  = 'NODE'
        group_item_1.ref_id   = node1.id
        group_item_1.group_id = network.resourcegroups.ResourceGroup[0].id 
        group_item_2          = self.client.factory.create('hyd:ResourceGroupItem')
        group_item_2.ref_key  = 'LINK'
        group_item_2.ref_id   = link.id
        group_item_2.group_id = network.resourcegroups.ResourceGroup[0].id 

        group_item_array.ResourceGroupItem.append(group_item_1)
        group_item_array.ResourceGroupItem.append(group_item_2)

        scenario.resourcegroupitems = group_item_array

        #This is an example of 3 diffent kinds of data
        #A simple string (Descriptor)
        #A time series, where the value may be a 1-D array
        #A multi-dimensional array.
        descriptor = self.create_descriptor(node_attrs.ResourceAttr[0], "new_descriptor")
        timeseries = self.create_timeseries(node_attrs.ResourceAttr[1])
        array      = self.create_array(node_attrs.ResourceAttr[2])

        scenario_data.ResourceScenario.append(descriptor)
        scenario_data.ResourceScenario.append(timeseries)
        scenario_data.ResourceScenario.append(array)

        #Set the scenario's data to the array we have just populated
        scenario.resourcescenarios = scenario_data

        scenario = self.client.service.add_scenario(network.id, scenario)

        assert scenario is not None
        assert len(scenario.resourcegroupitems.ResourceGroupItem) > 0
        assert len(scenario.resourcescenarios) > 0


    def test_update_scenario(self):
        """
            Test updating an existing scenario.
        """
        network = self.create_network_with_data()

        #Create the new scenario
        scenario = network.scenarios.Scenario[0] 
        scenario.name = 'Updated Scenario'
        scenario.description = 'Updated Scenario Description'
       
        for node in network.nodes.Node:
            if node.attributes is not None and len(node.attributes) > 0:
                node1 = node
            else:
                node2 = node

        for item in scenario.resourcegroupitems.ResourceGroupItem:
            if item.ref_key == 'NODE':
                item.ref_id   = node2.id

        descriptor = self.create_descriptor(node1.attributes.ResourceAttr[0], 
                                                "updated_descriptor")

        #Set the scenario's data to the array we have just populated
        scenario.resourcescenarios.ResourceScenario.append(descriptor)
        
        for resourcescenario in scenario.resourcescenarios.ResourceScenario:
            if resourcescenario.attr_id == descriptor.attr_id:
                resourcescenario.value = descriptor.value

        updated_scenario = self.client.service.update_scenario(scenario)

        assert updated_scenario is not None
        assert updated_scenario.id == scenario.id
        assert updated_scenario.name == "Updated Scenario"
        assert updated_scenario.description == 'Updated Scenario Description'
        assert len(updated_scenario.resourcegroupitems.ResourceGroupItem) > 0
        for item in updated_scenario.resourcegroupitems.ResourceGroupItem:
            if item.ref_key == 'NODE':
                assert item.ref_id == node2.id
        assert len(updated_scenario.resourcescenarios) > 0

        for data in updated_scenario.resourcescenarios.ResourceScenario: 
            if data.value.type == 'descriptor':
                assert data.value.value.desc_val == "updated_descriptor"

    def test_bulk_add_data(self):

        data = self.client.factory.create('ns1:DatasetArray')

        dataset1 = self.client.factory.create('ns1:Dataset')
        
        dataset1.type = 'timeseries'
        dataset1.name = 'my time series'
        dataset1.unit = 'feet cubed'
        dataset1.dimension = 'cubic capacity'

        ts1 = self.client.factory.create('ns1:TimeSeriesData')
        ts1.ts_time  = datetime.datetime.now()
        ts1.ts_value = str([1, 2, 3, 4, 5])

        ts2 = self.client.factory.create('ns1:TimeSeriesData')
        ts2.ts_time  = datetime.datetime.now() + datetime.timedelta(hours=1)
        ts2.ts_value = str([2, 3, 4, 5, 6])

        ts3 = self.client.factory.create('ns1:TimeSeries')
        ts3.ts_values.TimeSeriesData.append(ts1)
        ts3.ts_values.TimeSeriesData.append(ts2)
       
        dataset1.value = ts3
        data.Dataset.append(dataset1)

        dataset2 = self.client.factory.create('ns1:Dataset')
        dataset2.type = 'descriptor'
        dataset2.name = 'Max Capacity'
        dataset2.unit = 'metres / second'
        dataset2.dimension = 'number of units per time unit'
        
        descriptor = self.client.factory.create('ns1:Descriptor')
        descriptor.desc_val = 'I am an updated test!'

        dataset2.value = descriptor

        data.Dataset.append(dataset2)

        new_datasets = self.client.service.bulk_insert_data(data)

        assert len(new_datasets.integer) == 2; "Data was not added correctly!"


    def test_clone(self):

        network =  self.create_network_with_data()
       

        assert len(network.scenarios.Scenario) == 1; "The network should have only one scenario!"

        self.create_constraint(network)
        
        network = self.client.service.get_network(network.id)

        scenario = network.scenarios.Scenario[0]
        scenario_id = scenario.id

        new_scenario = self.client.service.clone_scenario(scenario_id)

        updated_network = self.client.service.get_network(new_scenario.network_id)


        assert len(updated_network.scenarios.Scenario) == 2; "The network should have only one scenario!"

        assert updated_network.scenarios.Scenario[1].resourcescenarios is not None; "Data was not cloned!"

        scen_2_val = updated_network.scenarios.Scenario[1].resourcescenarios.ResourceScenario[0].value.id
        scen_1_val = network.scenarios.Scenario[0].resourcescenarios.ResourceScenario[0].value.id
        
        assert scen_2_val == scen_1_val; "Data was not cloned correctly"


        scen_1_constraint  = network.scenarios.Scenario[0].constraints.Constraint[0].value
        scen_2_constraint  = updated_network.scenarios.Scenario[1].constraints.Constraint[0].value

        assert scen_1_constraint == scen_2_constraint; "Constraints did not clone correctly!"
        
        scen_1_resourcegroupitems = network.scenarios.Scenario[0].resourcegroupitems.ResourceGroupItem
        scen_2_resourcegroupitems = updated_network.scenarios.Scenario[1].resourcegroupitems.ResourceGroupItem
        
        assert len(scen_1_resourcegroupitems) == len(scen_2_resourcegroupitems)

    def test_compare(self):

        network =  self.create_network_with_data()
       

        assert len(network.scenarios.Scenario) == 1; "The network should have only one scenario!"

        self.create_constraint(network)
        
        network = self.client.service.get_network(network.id)

        scenario = network.scenarios.Scenario[0]
        scenario_id = scenario.id

        new_scenario = self.client.service.clone_scenario(scenario_id)


        self.create_constraint(network, constant=4)

        resource_scenario = new_scenario.resourcescenarios.ResourceScenario[0]
        resource_attr_id = resource_scenario.resource_attr_id

        dataset = self.client.factory.create('ns1:Dataset')
       
        dataset = self.client.factory.create('ns1:Dataset')
        dataset.type = 'descriptor'
        dataset.name = 'Max Capacity'
        dataset.unit = 'metres / second'
        dataset.dimension = 'number of units per time unit'
 
        descriptor = self.client.factory.create('ns1:Descriptor')
        descriptor.desc_val = 'I am an updated test!'

        dataset.value = descriptor

        self.client.service.add_data_to_attribute(scenario_id, resource_attr_id, dataset)

        item_to_remove = new_scenario.resourcegroupitems.ResourceGroupItem[0].id
        self.client.service.delete_resourcegroupitem(item_to_remove)

        updated_network = self.client.service.get_network(new_scenario.network_id)

        scenarios = updated_network.scenarios.Scenario

        scenario_diff = self.client.service.compare_scenarios(scenarios[0].id, scenarios[1].id)
        
        print "Comparison result: %s"%(scenario_diff)

        assert len(scenario_diff.resourcescenarios.ResourceScenarioDiff) == 1; "Data comparison was not successful!"

        assert len(scenario_diff.constraints.common_constraints) == 1; "Constraint comparison was not successful!"
        
        assert len(scenario_diff.constraints.scenario_1_constraints) == 1; "Constraint comparison was not successful!"


        assert len(scenario_diff.groups.scenario_1_items) == 1; "Group comparison was not successful!"
        assert scenario_diff.groups.scenario_2_items is None; "Group comparison was not successful!"


if __name__ == '__main__':
    test_SoapServer.run()
