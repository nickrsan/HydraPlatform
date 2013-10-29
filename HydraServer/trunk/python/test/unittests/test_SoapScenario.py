#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer


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

if __name__ == '__main__':
    test_SoapServer.run()
