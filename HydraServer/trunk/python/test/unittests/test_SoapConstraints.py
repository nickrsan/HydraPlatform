import os
import shutil
from tempfile import gettempdir as tmp
shutil.rmtree(os.path.join(tmp(), 'suds'), True)
import test_SoapServer

class TestConstraint(test_SoapServer.SoapServerTest):

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
        item_4.resource_attr_id = net.nodes.Node[0].attributes.ResourceAttr[2].id


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
