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

    def test_bulk_add_data(self):

        network =  self.create_network_with_data()
        
        scenario = network.scenarios.Scenario[0]
        scenario_id = scenario.id

        resource_scenario = scenario.resourcescenarios.ResourceScenario[0]
        resource_attr_id = resource_scenario.resource_attr_id

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



if __name__ == '__main__':
    test_SoapServer.run()
