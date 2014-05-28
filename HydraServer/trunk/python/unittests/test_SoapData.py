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
import datetime
import logging

class DataGroupTest(test_SoapServer.SoapServerTest):

    def test_get_groups_like_name(self):
        groups = self.client.service.get_groups_like_name('test')
 
        assert len(groups) > 0; "groups were not retrieved correctly!"
   
    def test_get_group_datasets(self):
        groups = self.client.service.get_groups_like_name('test')
        
        datasets = self.client.service.get_group_datasets(groups.DatasetGroup[-1].group_id)
 
        assert len(datasets) > 0, "Datasets were not retrieved correctly!"

    def test_add_group(self):
        
        network = self.create_network_with_data()

        scenario_id = network.scenarios.Scenario[0].id
        
        scenario_data = self.client.service.get_scenario_data(scenario_id)

        group = self.client.factory.create('ns1:DatasetGroup')

        itemarray = self.client.factory.create('ns1:DatasetGroupItemArray')
        item1 = self.client.factory.create('ns1:DatasetGroupItem')
        item1.dataset_id = scenario_data.Dataset[0].id
        for d in scenario_data.Dataset:
            if d.type == 'timeseries' and d.id != item1.dataset_id:

                item2 = self.client.factory.create('ns1:DatasetGroupItem')
                item2.dataset_id = d.id
                break
        itemarray.DatasetGroupItem.append(item1)
        itemarray.DatasetGroupItem.append(item2)

        group.datasetgroupitems = itemarray
        group.group_name        = 'test soap group %s'%(datetime.datetime.now())

        newly_added_group = self.client.service.add_dataset_group(group)

        assert newly_added_group.group_id is not None, "Dataset group does not have an ID!"
        assert len(newly_added_group.datasetgroupitems.DatasetGroupItem) == 2, "Dataset group does not have any items!"  

class SharingTest(test_SoapServer.SoapServerTest):

    def test_lock_data(self):
        """
            Test for the hiding of data.
            Create a network with some data.
            Hide the timeseries created, check if another user can see it.
            Share the time series with one users. Check if they can see it but a third user can't.
        """

        self.create_user("UserA")
        self.create_user("UserB")
        self.create_user("UserC")
        
        #One client is for the 'root' user and must remain open so it
        #can be closed correctly in the tear down. 
        old_client = self.client
        new_client = test_SoapServer.connect()
        self.client = new_client

        self.login("UserA", 'password')
        
        network_1 = self.create_network_with_data()

        #Let User B view network 1, but not edit it (read_only is 'Y')
        self.client.service.share_network(network_1.id, ["UserB", "UserC"], 'Y')
        
        scenario = network_1.scenarios.Scenario[0]
        
        data = [x.value for x in scenario.resourcescenarios.ResourceScenario]

        data_to_hide = data[-1].id

        self.client.service.lock_dataset(data_to_hide, ["UserB"], 'Y', 'Y', 'Y')

        self.client.service.logout("UserA")

        self.login("UserB", 'password')
       
        netA = self.client.service.get_network(network_1.id)
        scenario = netA.scenarios.Scenario[0]
        
        data = [x.value for x in scenario.resourcescenarios.ResourceScenario]

        for d in data:
            if d.id == data_to_hide:
                assert d.locked == 'Y'
                assert d.value is not None
            else:
                #The rest of the data is unlocked, so should be there.
                assert d.locked == 'N'
                assert d.value is not None

        #Check user B can see the dataset
        self.client.service.logout("UserB")

        self.login("UserC", 'password')
        #Check user C cannot see the dataset
        netB = self.client.service.get_network(network_1.id)
        
        scenario = netB.scenarios.Scenario[0]
        
        data = [x.value for x in scenario.resourcescenarios.ResourceScenario]
        
        for d in data:
            if d.id == data_to_hide:
                assert d.locked == 'Y'
                assert d.value is None
            else:
                #The rest of the data is unlocked, so should be there.
                assert d.locked == 'N'
                assert d.value is not None

        self.client.service.logout("UserC")

        self.client = old_client

    def test_replace_locked_data(self):
        """
            test_replace_locked_data
            Test for the case where one user locks data and another
            user sets the data to something else.
            
            User A Creates a network with some data
            User A Hides the timeseries created.
            User A shares network with User B
            
            Check user B cannot see timeseries value
            User B creates a new timeseries, and replaces the locked one.
            Save network.
            Attribute now should have a new, unlocked dataset assigned to that attribute.
        """

        self.create_user("UserA")
        self.create_user("UserB")
        
        #One client is for the 'root' user and must remain open so it
        #can be closed correctly in the tear down. 
        old_client = self.client
        new_client = test_SoapServer.connect()
        self.client = new_client

        self.login("UserA", 'password')
        
        network_1 = self.create_network_with_data()

        #Let User B view network 1, but not edit it (read_only is 'Y')
        self.client.service.share_network(network_1.id, ["UserB", "UserC"], 'N')

        scenario = network_1.scenarios.Scenario[0]

        data = [x for x in scenario.resourcescenarios.ResourceScenario]

        for d in data:
            if d.value.type == 'timeseries':
                attr_to_be_changed = d.resource_attr_id
                data_to_hide = d.value.id

        self.client.service.lock_dataset(data_to_hide, [], 'Y', 'Y', 'Y')

        self.client.service.logout("UserA")

        self.login("UserB", 'password')
       
        netA = self.client.service.get_network(network_1.id)
        scenario = netA.scenarios.Scenario[0]
        
        #Find the locked piece of data and replace it with another
        #to simulate a case of two people working on one attribute
        #where one cannot see the value of it.
        for d in scenario.resourcescenarios.ResourceScenario:
            if d.resource_attr_id == attr_to_be_changed:
                #THis piece of data is indeed the locked one.
                assert d.value.locked == 'Y'
                #set the value of the attribute to be a different
                #timeseries.
                dataset = self.client.factory.create('hyd:Dataset')

                dataset.type = 'timeseries'
                dataset.name = 'replacement time series'
                dataset.unit = 'feet cubed'
                dataset.dimension = 'cubic capacity'

                dataset.value = {'ts_values' : 
                    [
                        {'ts_time' : datetime.datetime.now(),
                        'ts_value' : str([11, 21, 31, 41, 51])},
                        {'ts_time' : datetime.datetime.now()+datetime.timedelta(hours=1),
                        'ts_value' : str([12, 22, 32, 42, 52])},
                    ]
                }
                d.value = dataset
            else:
                #The rest of the data is unlocked, so should be there.
                assert d.value.locked == 'N'
                assert d.value.value is not None

        updated_net = self.client.service.update_network(netA)
        scenario = updated_net.scenarios.Scenario[0]
        #After updating the network, check that the new dataset
        #has been applied
        for d in scenario.resourcescenarios.ResourceScenario:
            if d.resource_attr_id == attr_to_be_changed:
                assert d.value.locked == 'N'
                assert d.value.id     != data_to_hide
        #Now validate that the dataset was not overwritten, but replaced
        #by getting the old dataset and ensuring user B can still not see it.
        locked_dataset = self.client.service.get_dataset(data_to_hide)
        assert locked_dataset.locked == 'Y'
        assert locked_dataset.value  == None

        self.client.service.logout("UserB")

        self.client = old_client

    def test_edit_locked_data(self):
        """
            test_edit_locked_data
            Test for the case where one user locks data and another
            user sets the data to something else.
            
            User A Creates a network with some data
            User A Hides the timeseries created.
            User A shares network with User B
            
            Check user B cannot see timeseries value
            User B sets value of timeseries to something else.
            Save network.
            Attribute now should have a new, unlocked dataset assigned to that attribute.
        """

        self.create_user("UserA")
        self.create_user("UserB")
        
        #One client is for the 'root' user and must remain open so it
        #can be closed correctly in the tear down. 
        old_client = self.client
        new_client = test_SoapServer.connect()
        self.client = new_client

        self.login("UserA", 'password')
        
        network_1 = self.create_network_with_data()

        #Let User B view network 1, but not edit it (read_only is 'Y')
        self.client.service.share_network(network_1.id, ["UserB"], 'N')

        scenario = network_1.scenarios.Scenario[0]

        data = [x for x in scenario.resourcescenarios.ResourceScenario]

        for d in data:
            if d.value.type == 'timeseries':
                attr_to_be_changed = d.resource_attr_id
                data_to_hide = d.value.id
                break

        self.client.service.lock_dataset(data_to_hide, [], 'Y', 'Y', 'Y')
        self.client.service.logout("UserA")

        self.login("UserB", 'password')
       
        netA = self.client.service.get_network(network_1.id)
        scenario = netA.scenarios.Scenario[0]
        
        #Find the locked piece of data and replace it with another
        #to simulate a case of two people working on one attribute
        #where one cannot see the value of it.
        for d in scenario.resourcescenarios.ResourceScenario:
            if d.resource_attr_id == attr_to_be_changed:
                #THis piece of data is indeed the locked one.
                assert d.value.locked == 'Y'
                #Reassign the value of the dataset to something new.
                d.value.value = {'ts_values' : 
                    [
                        {'ts_time' : datetime.datetime.now(),
                        'ts_value' : str([11, 21, 31, 41, 51])},
                        {'ts_time' : datetime.datetime.now()+datetime.timedelta(hours=1),
                        'ts_value' : str([12, 22, 32, 42, 52])},
                    ]
                }
            else:
                #The rest of the data is unlocked, so should be there.
                assert d.value.locked == 'N'
                assert d.value.value is not None

        updated_net = self.client.service.update_network(netA)
        scenario = updated_net.scenarios.Scenario[0]
        #After updating the network, check that the new dataset
        #has been applied
        for d in scenario.resourcescenarios.ResourceScenario:
            if d.resource_attr_id == attr_to_be_changed:
                assert d.value.locked == 'N'
                assert d.value.id     != data_to_hide
        #Now validate that the dataset was not overwritten, but replaced
        #by getting the old dataset and ensuring user B can still not see it.
        locked_dataset = self.client.service.get_dataset(data_to_hide)
        assert locked_dataset.locked == 'Y'
        assert locked_dataset.value  == None

        self.client.service.logout("UserB")

        self.client = old_client

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

class RetrievalTest(test_SoapServer.SoapServerTest):
    def test_get_node_data(self):
        """
            Test for the potentially likely case of creating a network with two
            scenarios, then querying for the network without data to identify
            the scenarios, then querying for the network with data but in only
            a select few scenarios.
        """
        net = self.create_network_with_data()
        scenario_id = net.scenarios.Scenario[0].id       
        node_id     = net.nodes.Node[0].id
        
        node_data = self.client.service.get_node_data(node_id, scenario_id)

        assert len(node_data) > 0

if __name__ == '__main__':
    test_SoapServer.run()
