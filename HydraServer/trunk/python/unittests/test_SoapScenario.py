#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
import datetime
import logging
import copy

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


        assert new_resource_scenario.value.value.desc_val == 'I am an updated test!', "Value was not updated correctly!!"

    def test_add_scenario(self):
        """
            Test adding a new scenario to a network.
        """
        network = self.create_network_with_data()

        new_scenario = copy.deepcopy(network.scenarios.Scenario[0])
        new_scenario.id = -1
        new_scenario.name = 'Scenario 2'
        new_scenario.description = 'Scenario 2 Description'
        new_scenario.start_time = datetime.datetime.now()
        new_scenario.end_time = new_scenario.start_time + datetime.timedelta(hours=10)
        new_scenario.time_step = "1 day"

        node_attrs = network.nodes.Node[0].attributes

        #This is an example of 3 diffent kinds of data
        #A simple string (Descriptor)
        #A time series, where the value may be a 1-D array
        #A multi-dimensional array.
        descriptor = self.create_descriptor(node_attrs.ResourceAttr[0], "new_descriptor")
        timeseries = self.create_timeseries(node_attrs.ResourceAttr[1])

        for r in new_scenario.resourcescenarios.ResourceScenario:
            if r.resource_attr_id == node_attrs.ResourceAttr[0].id:
                r.value = descriptor['value']
            elif r.resource_attr_id == node_attrs.ResourceAttr[1].id:
                r.value = timeseries['value']

        scenario = self.client.service.add_scenario(network.id, new_scenario)

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
        scenario.start_time = datetime.datetime.now()
        scenario.end_time = scenario.start_time + datetime.timedelta(hours=10)
        scenario.time_step = "1 day" #measured in seconds
      
        #Identify 2 nodes to play around with -- the first and last in the list.
        node1 = network.nodes.Node[0]
        node2 = network.nodes.Node[-1]

        #Identify 1 resource group item to edit (the last one in the list).
        item_to_edit = scenario.resourcegroupitems.ResourceGroupItem[-1]
        #Just checking that we're not changing an item that is already
        #assigned to this node..
        assert item_to_edit.ref_id != node2.id
        item_to_edit.ref_id   = node2.id

        descriptor = self.create_descriptor(node1.attributes.ResourceAttr[0], 
                                                "updated_descriptor")

        for resourcescenario in scenario.resourcescenarios.ResourceScenario:
            if resourcescenario.attr_id == descriptor['attr_id']:
                resourcescenario.value = descriptor['value']
        
        updated_scenario = self.client.service.update_scenario(scenario)

        assert updated_scenario is not None
        assert updated_scenario.id == scenario.id
        assert updated_scenario.name == scenario.name 
        assert updated_scenario.description == scenario.description
        assert updated_scenario.start_time == str(scenario.start_time)
        assert updated_scenario.end_time   == str(scenario.end_time)
        assert updated_scenario.time_step  == scenario.time_step
        assert len(updated_scenario.resourcegroupitems.ResourceGroupItem) > 0
        for i in updated_scenario.resourcegroupitems.ResourceGroupItem:
            if i.id == item_to_edit.id:
                assert i.ref_id == node2.id
        assert len(updated_scenario.resourcescenarios) > 0

        for data in updated_scenario.resourcescenarios.ResourceScenario: 
            if data.attr_id == descriptor['attr_id']:
                assert data.value.value.desc_val == descriptor['value']['value']['desc_val'] 

    def test_bulk_add_data(self):

        data = self.client.factory.create('ns1:DatasetArray')

        dataset1 = self.client.factory.create('ns1:Dataset')
        
        dataset1.type = 'timeseries'
        dataset1.name = 'my time series'
        dataset1.unit = 'feet cubed'
        dataset1.dimension = 'cubic capacity'

        dataset1.value = {'ts_values':
            [
                {
                    'ts_time' : datetime.datetime.now(),
                    'ts_value' : str([1, 2, 3, 4, 5]),
                },
                {
                    'ts_time' : datetime.datetime.now() + datetime.timedelta(hours=1),
                    'ts_value' : str([2, 3, 4, 5, 6]),
                }
            ],
        }
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

        assert len(new_datasets.integer) == 2, "Data was not added correctly!"

    def test_get_data_between_times(self):
        net = self.create_network_with_data()
        scenario = net.scenarios.Scenario[0]
        val_to_query = None
        for d in scenario.resourcescenarios.ResourceScenario:
            if d.value.type == 'timeseries':
                val_to_query = d.value
                break

        val_a = val_to_query.value.ts_values[0].ts_value
        val_b = val_to_query.value.ts_values[1].ts_value

        now = datetime.datetime.now()

        vals = self.client.service.get_vals_between_times(
            val_to_query.id,
            now,
            now + datetime.timedelta(minutes=75),
            'minutes',
            )

        data = eval(vals.data)
        assert len(data) == 76
        for val in data[0:59]:
            assert val == eval(val_a)
        for val in data[60:75]:
            assert val == eval(val_b)

    def test_descriptor_get_data_between_times(self):
        net = self.create_network_with_data()
        scenario = net.scenarios.Scenario[0]
        val_to_query = None
        for d in scenario.resourcescenarios.ResourceScenario:
            if d.value.type == 'descriptor':
                val_to_query = d.value
                break

        now = datetime.datetime.now()

        value = self.client.service.get_vals_between_times(
            val_to_query.id,
            now,
            now + datetime.timedelta(minutes=75),
            'minutes',
            )
        logging.debug(value)
        assert value.data == 'test'


    def test_clone(self):

        network =  self.create_network_with_data()
       

        assert len(network.scenarios.Scenario) == 1, "The network should have only one scenario!"

        self.create_constraint(network)
        
        network = self.client.service.get_network(network.id)

        scenario = network.scenarios.Scenario[0]
        scenario_id = scenario.id

        new_scenario = self.client.service.clone_scenario(scenario_id)

        updated_network = self.client.service.get_network(new_scenario.network_id)


        assert len(updated_network.scenarios.Scenario) == 2, "The network should have two scenarios!"

        assert updated_network.scenarios.Scenario[1].resourcescenarios is not None, "Data was not cloned!"

        scen_2_val = updated_network.scenarios.Scenario[1].resourcescenarios.ResourceScenario[0].value.id
        scen_1_val = network.scenarios.Scenario[0].resourcescenarios.ResourceScenario[0].value.id
        
        assert scen_2_val == scen_1_val, "Data was not cloned correctly"


        scen_1_constraint  = network.scenarios.Scenario[0].constraints.Constraint[0].value
        scen_2_constraint  = updated_network.scenarios.Scenario[1].constraints.Constraint[0].value

        assert scen_1_constraint == scen_2_constraint, "Constraints did not clone correctly!"
        
        scen_1_resourcegroupitems = network.scenarios.Scenario[0].resourcegroupitems.ResourceGroupItem
        scen_2_resourcegroupitems = updated_network.scenarios.Scenario[1].resourcegroupitems.ResourceGroupItem
        
        assert len(scen_1_resourcegroupitems) == len(scen_2_resourcegroupitems)

    def test_compare(self):

        network =  self.create_network_with_data()
       

        assert len(network.scenarios.Scenario) == 1, "The network should have only one scenario!"

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
        
        scenario_1 = None
        scenario_2 = None
        for s in scenarios:
            if s.id == new_scenario.id:
                scenario_1 = s 
            else:
                scenario_2 = s

        scenario_diff = self.client.service.compare_scenarios(scenario_1.id, scenario_2.id)
        
        print "Comparison result: %s"%(scenario_diff)

        assert len(scenario_diff.resourcescenarios.ResourceScenarioDiff) == 1, "Data comparison was not successful!"

        assert len(scenario_diff.constraints.common_constraints) == 1, "Constraint comparison was not successful!"
        
        assert len(scenario_diff.constraints.scenario_2_constraints) == 1, "Constraint comparison was not successful!"

        assert len(scenario_diff.groups.scenario_2_items) == 1, "Group comparison was not successful!"
        assert scenario_diff.groups.scenario_1_items is None, "Group comparison was not successful!"


if __name__ == '__main__':
    test_SoapServer.run()
