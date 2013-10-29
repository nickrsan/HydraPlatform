#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
import datetime

class DataGroupTest(test_SoapServer.SoapServerTest):

    def test_get_groups_like_name(self):
        groups = self.client.service.get_groups_like_name('test')
 
        assert len(groups) > 0; "groups were not retrieved correctly!"
   
    def test_get_group_datasets(self):
        groups = self.client.service.get_groups_like_name('test')
        
        datasets = self.client.service.get_group_datasets(groups.DatasetGroup[-1].group_id)
 
        assert len(datasets) > 0, "Datsets were not retrieved correctly!"

    def test_add_group(self):
        
        network = self.create_network_with_data()

        scenario_id = network.scenarios.Scenario[0].id
        scenario_data = self.client.service.get_scenario_data(scenario_id)

        group = self.client.factory.create('ns1:DatasetGroup')

        itemarray = self.client.factory.create('ns1:DatasetGroupItemArray')
        item1 = self.client.factory.create('ns1:DatasetGroupItem')
        item1.dataset_id = scenario_data.Dataset[0].id
        item2 = self.client.factory.create('ns1:DatasetGroupItem')
        item2.dataset_id = scenario_data.Dataset[1].id
        itemarray.DatasetGroupItem.append(item1)
        itemarray.DatasetGroupItem.append(item2)

        group.datasetgroupitems = itemarray
        group.group_name        = 'test soap group %s'%(datetime.datetime.now())

        newly_added_group = self.client.service.add_dataset_group(group)

        assert newly_added_group.group_id is not None, "Dataset group does not have an ID!"
        assert len(newly_added_group.datasetgroupitems.DatasetGroupItem) == 2, "Dataset group does not have any items!"
  

if __name__ == '__main__':
    test_SoapServer.run()
