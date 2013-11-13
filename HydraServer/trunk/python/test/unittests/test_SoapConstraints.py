import os
import shutil
from tempfile import gettempdir as tmp
shutil.rmtree(os.path.join(tmp(), 'suds'), True)
import test_SoapServer

class TestConstraint(test_SoapServer.SoapServerTest):

    def test_constraint(self):

        net = self.create_network_with_data()
        
        constraint = self.client.factory.create('ns1:Constraint')
        constraint.scenario_id = net.scenarios[0][0].id
        constraint.op = "=="
        constraint.constant = 0

        outer_group = self.client.factory.create('ns1:ConstraintGroup')
        outer_group.op = '+'


        outer_group_array = self.client.factory.create('ns1:ConstraintGroupArray')

        #this is empty, as there are no groups within the inner group.
        inner_group_array = self.client.factory.create('ns1:ConstraintGroupArray')

        inner_group = self.client.factory.create('ns1:ConstraintGroup')
        inner_group.op = '+'

        item_1 = self.client.factory.create('ns1:ConstraintItem')
        item_1.resource_attr_id = net.nodes.Node[0].attributes.ResourceAttr[0].id
        item_2 = self.client.factory.create('ns1:ConstraintItem')
        item_2.resource_attr_id = net.nodes.Node[0].attributes.ResourceAttr[1].id
        item_3 = self.client.factory.create('ns1:ConstraintItem')
        item_3.resource_attr_id = net.nodes.Node[0].attributes.ResourceAttr[2].id

        #set the items within the inner group
        inner_item_array = self.client.factory.create('ns1:ConstraintItemArray')
        inner_item_array.ConstraintItem.append(item_2)
        inner_item_array.ConstraintItem.append(item_3)
        inner_group.constraintitems = inner_item_array
        #set the empty inner group.
        inner_group.constraintgroups = inner_group_array

        outer_item_array = self.client.factory.create('ns1:ConstraintItemArray')
        outer_item_array.ConstraintItem.append(item_1)

        outer_group_array.ConstraintGroup.append(inner_group)

        outer_group.constraintgroups = outer_group_array
        outer_group.constraintitems = outer_item_array

        constraint.constraintgroup = outer_group
       
        #print outer_group

        test_constraint = self.client.service.add_constraint(net.scenarios[0][0].id, constraint)

        #print test_constraint


if __name__ == "__main__":
    test_SoapServer.run()
