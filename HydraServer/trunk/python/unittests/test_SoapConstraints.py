import os
import shutil
from tempfile import gettempdir as tmp
shutil.rmtree(os.path.join(tmp(), 'suds'), True)
import test_SoapServer

class TestGroupsInNetwork(test_SoapServer.SoapServerTest):

    def test_network_update_group(self):
        net = self.create_network_with_data()
           
        resourcegroup = net.resourcegroups.ResourceGroup[0]
        assert resourcegroup is not None

        scenario = net.scenarios.Scenario[0]

        group_items = scenario.resourcegroupitems.ResourceGroupItem
        assert group_items is not None

        resourcegroup.name        = "Updated Name"
        resourcegroup.description = "Updated Description"
        resourcegroup.status      = "X"

        updated_net = self.client.service.update_network(net)

        new_resourcegroup = updated_net.resourcegroups.ResourceGroup[0]
        assert new_resourcegroup.id          == resourcegroup.id
        assert new_resourcegroup.name        == "Updated Name"
        assert new_resourcegroup.description == "Updated Description"
        assert new_resourcegroup.status      == "X"

    def test_network_add_valid_groupitem(self):
        net = self.create_network_with_data()
           
        resourcegroup = net.resourcegroups.ResourceGroup[0]
        assert resourcegroup is not None

        scenario = net.scenarios.Scenario[0]

        resourcegroupitems = scenario.resourcegroupitems.ResourceGroupItem

        assert len(resourcegroupitems) == 2

        new_item          = self.client.factory.create('hyd:ResourceGroupItem')
        new_item.ref_key  = 'NODE'
        new_item.ref_id   = net.nodes.Node[-1].id
        new_item.group_id = resourcegroup.id

        resourcegroupitems.append(new_item)

        updated_net = self.client.service.update_network(net)

        updated_scenario = updated_net.scenarios.Scenario[0]
        updated_items    = updated_scenario.resourcegroupitems.ResourceGroupItem

        assert len(updated_items) == 3

    def test_network_add_existing_groupitem(self):
        net = self.create_network_with_data()
           
        resourcegroup = net.resourcegroups.ResourceGroup[0]
        assert resourcegroup is not None

        scenario = net.scenarios.Scenario[0]

        resourcegroupitems = scenario.resourcegroupitems.ResourceGroupItem

        assert len(resourcegroupitems) == 2

        new_item          = self.client.factory.create('hyd:ResourceGroupItem')
        new_item.ref_key  = 'NODE'
        new_item.ref_id   = net.nodes.Node[0].id
        new_item.group_id = resourcegroup.id
        
        resourcegroupitems.append(new_item)
        
        error = None
        try:
            self.client.service.update_network(net)
        except Exception, e:
            error =  e.message

        assert error is not None
        assert error.find('Duplicate') > 0

    def test_network_add_invalid_groupitem(self):
        net = self.create_network_with_data()
           
        resourcegroup = net.resourcegroups.ResourceGroup[0]
        assert resourcegroup is not None

        scenario = net.scenarios.Scenario[0]

        resourcegroupitems = scenario.resourcegroupitems.ResourceGroupItem

        assert len(resourcegroupitems) == 2

        new_item          = self.client.factory.create('hyd:ResourceGroupItem')
        new_item.ref_key  = 'NODE'
        new_item.ref_id   = 99999
        new_item.group_id = resourcegroup.id
        
        resourcegroupitems.append(new_item)

        error = None 
        try:
            self.client.service.update_network(net)
        except Exception, e:
            error = e.message

        assert error is not None
        assert error.find("Invalid ref ID for group item!") > 0

    def test_network_remove_groupitem(self):
        net = self.create_network_with_data()
           
        resourcegroup = net.resourcegroups.ResourceGroup[0]
        assert resourcegroup is not None

        scenario = net.scenarios.Scenario[0]

        resourcegroupitems = scenario.resourcegroupitems.ResourceGroupItem

        assert len(resourcegroupitems) == 2

        result = self.client.service.delete_resourcegroupitem(resourcegroupitems[0].id)

        assert result == 'OK'

        updated_net = self.client.service.get_network(net.id)

        updated_scenario = updated_net.scenarios.Scenario[0]
        updated_items    = updated_scenario.resourcegroupitems.ResourceGroupItem

        assert len(updated_items) == 1

    #*************************#
    #NON NETWORK BASED UPDATES#
    #*************************#

class TestGroupsStandalone(test_SoapServer.SoapServerTest):
    def test_add_group(self):
        net = self.create_network_with_data()
           
        resourcegroup = self.client.factory.create('hyd:ResourceGroup') 
        resourcegroup.name        = "New Group Name"
        resourcegroup.description = "New Group Description"
        resourcegroup.status      = "X"

        new_resourcegroup = self.client.service.add_resourcegroup(resourcegroup, net.id)

        assert new_resourcegroup.name        == resourcegroup.name 
        assert new_resourcegroup.description == resourcegroup.description
        assert new_resourcegroup.status      == resourcegroup.status

    def test_update_group(self):
        net = self.create_network_with_data()
           
        resourcegroup = net.resourcegroups.ResourceGroup[0]
        assert resourcegroup is not None

        resourcegroup.name        = "Updated Name"
        resourcegroup.description = "Updated Description"
        resourcegroup.status      = "X"

        new_resourcegroup = self.client.service.update_resourcegroup(resourcegroup)

        assert new_resourcegroup.id          == resourcegroup.id
        assert new_resourcegroup.name        == "Updated Name"
        assert new_resourcegroup.description == "Updated Description"
        assert new_resourcegroup.status      == "X"

    def test_add_valid_groupitem(self):
        net = self.create_network_with_data()
           
        resourcegroup = net.resourcegroups.ResourceGroup[0]
        assert resourcegroup is not None

        scenario = net.scenarios.Scenario[0]

        resourcegroupitems = scenario.resourcegroupitems.ResourceGroupItem

        assert len(resourcegroupitems) == 2

        new_item          = self.client.factory.create('hyd:ResourceGroupItem')
        new_item.ref_key  = 'NODE'
        new_item.ref_id   = net.nodes.Node[-1].id
        new_item.group_id = resourcegroup.id

        resourcegroupitems.append(new_item)

        new_item = self.client.service.add_resourcegroupitem(new_item, scenario.id)
        
        updated_net      = self.client.service.get_network(net.id) 
        updated_scenario = updated_net.scenarios.Scenario[0]
        updated_items    = updated_scenario.resourcegroupitems.ResourceGroupItem

        assert len(updated_items) == 3

    def test_add_existing_groupitem(self):
        net = self.create_network_with_data()
           
        resourcegroup = net.resourcegroups.ResourceGroup[0]
        assert resourcegroup is not None

        scenario = net.scenarios.Scenario[0]

        new_item          = self.client.factory.create('hyd:ResourceGroupItem')
        new_item.ref_key  = 'NODE'
        new_item.ref_id   = net.nodes.Node[0].id
        new_item.group_id = resourcegroup.id
        
        error = None
        try:
            new_item = self.client.service.add_resourcegroupitem(new_item, scenario.id)
        except Exception, e:
            error =  e.message

        assert error is not None
        assert error.find('Duplicate') > 0

    def test_add_invalid_groupitem(self):
        net = self.create_network_with_data()
           
        resourcegroup = net.resourcegroups.ResourceGroup[0]
        assert resourcegroup is not None

        scenario = net.scenarios.Scenario[0]

        new_item          = self.client.factory.create('hyd:ResourceGroupItem')
        new_item.ref_key  = 'NODE'
        new_item.ref_id   = 99999
        new_item.group_id = resourcegroup.id
        
        error = None
        try:
            new_item = self.client.service.add_resourcegroupitem(new_item, scenario.id)
        except Exception, e:
            error =  e.message

        assert error is not None
        assert error.find("Invalid ref ID for group item!") > 0

    def test_remove_groupitem(self):
        net = self.create_network_with_data()
           
        scenario = net.scenarios.Scenario[0]

        resourcegroupitems = scenario.resourcegroupitems.ResourceGroupItem

        result = self.client.service.delete_resourcegroupitem(resourcegroupitems[0].id)

        assert result == 'OK'


class TestConstraints(test_SoapServer.SoapServerTest):

    def test_constraint(self):

        net = self.create_network_with_data()
        

        #We are trying to achieve a structure that looks like:
        #(((A + 5) * B) - C) == 0
        #3 groups
        #4 items
        #group 1 contains group 2 & item C
        #group 2 contains group 3 & item B
        #group 3 contains item A and item 5
        #The best way to do this is in reverse order. Start with the inner
        #group and work your way out.

        #create all the groups & items
        #Innermost group first (A + 5)
        group_3 = self.client.factory.create('ns1:ConstraintGroup')
        group_3.op = '+'
        
        item_1 = self.client.factory.create('ns1:ConstraintItem')
        item_1.resource_attr_id = net.nodes.Node[0].attributes.ResourceAttr[0].id
        item_2 = self.client.factory.create('ns1:ConstraintItem')
        item_2.constant = 5 

        #set the items in group 3 (aka 'A' and 5 from example above)
        group_3_items = self.client.factory.create('ns1:ConstraintItemArray')
        group_3_items.ConstraintItem.append(item_1)
        group_3_items.ConstraintItem.append(item_2)
        
        group_3.constraintitems = group_3_items

        #Then the next group out (group_1 * B)
        #Group 2 (which has both an item and a group)
        group_2 = self.client.factory.create('ns1:ConstraintGroup')
        group_2.op = '*' 

        item_3 = self.client.factory.create('ns1:ConstraintItem')
        item_3.resource_attr_id = net.nodes.Node[0].attributes.ResourceAttr[1].id
       
        group_2_items = self.client.factory.create('ns1:ConstraintItemArray')
        group_2_items.ConstraintItem.append(item_3)
        
        group_2_groups = self.client.factory.create('ns1:ConstraintGroupArray')
        group_2_groups.ConstraintGroup.append(group_3)
        
        group_2.constraintgroups = group_2_groups
        group_2.constraintitems  = group_2_items

        #Then the outermost group: (group_2 - C)
        #Group 1 has  also has an item and a group
        group_1 = self.client.factory.create('ns1:ConstraintGroup')
        group_1.op = '-'

        item_4 = self.client.factory.create('ns1:ConstraintItem')
        item_4.resource_attr_id = net.links.Link[0].attributes.ResourceAttr[0].id


        group_1_items = self.client.factory.create('ns1:ConstraintItemArray')
        group_1_items.ConstraintItem.append(item_4)

        group_1_groups = self.client.factory.create('ns1:ConstraintGroupArray')
        group_1_groups.ConstraintGroup.append(group_2)
        group_1.constraintgroups = group_1_groups
        group_1.constraintitems = group_1_items


        constraint = self.client.factory.create('ns1:Constraint')
        constraint.scenario_id = net.scenarios[0][0].id
        constraint.op = "=="
        constraint.constant = 0

        constraint.constraintgroup = group_1
       
        #print outer_group

        test_constraint = self.client.service.add_constraint(net.scenarios[0][0].id, constraint)

        print test_constraint


if __name__ == "__main__":
    test_SoapServer.run()
