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
import test_HydraIface
from db import HydraIface

class ConstraintTest(test_HydraIface.HydraIfaceTest):

    def test_create(self):
        """
            Creating a condition on three nodes, whereby
            'flow' attribute of nodes a and b must be greater or equal
            to that of the 'flow' of node c.
        """
        project = self.create_project("Project A")
        network = self.create_network("Network A", project.db.project_id)
        scenario = self.create_scenario(network.db.network_id, "Scenario A")

        node_a = self.create_node("Node A", network.db.network_id)
        node_b = self.create_node("Node B", network.db.network_id)
        node_c = self.create_node("Node C", network.db.network_id)

        attr_a = self.create_attribute("Flow")

        ra = node_a.add_attribute(attr_a.db.attr_id)
        ra.save()
        rb = node_b.add_attribute(attr_a.db.attr_id)
        rb.save()
        rc = node_c.add_attribute(attr_a.db.attr_id)
        rc.save()

        rsa = HydraIface.ResourceScenario(scenario_id=scenario.db.scenario_id, resource_attr_id=ra.db.resource_attr_id)
        rsb = HydraIface.ResourceScenario(scenario_id=scenario.db.scenario_id, resource_attr_id=rb.db.resource_attr_id)
        rsc = HydraIface.ResourceScenario(scenario_id=scenario.db.scenario_id, resource_attr_id=rc.db.resource_attr_id)

        rsa.assign_value('scalar' ,1, 'm3', 'flow', 'int')
        rsb.assign_value('scalar' ,2, 'm3', 'flow', 'int')
        rsc.assign_value('scalar' ,3, 'm3', 'flow', 'int')

        con = HydraIface.Constraint()
        con.db.constraint_name = "Comparative resource allocation"
        con.db.constraint_description = "(A+5) * B must be >= C"
        con.db.scenario_id            = scenario.db.scenario_id
        con.db.constant               = 0
        con.db.op                     = "=="
        con.save()

        item_a = HydraIface.ConstraintItem(constraint=con)
        item_a.db.constraint_id = con.db.constraint_id
        item_a.db.resource_attr_id = ra.db.resource_attr_id
        item_a.save()

        item_constant = HydraIface.ConstraintItem(constraint=con)
        item_constant.db.constraint_id = con.db.constraint_id
        item_constant.db.constant = 5
        item_constant.save()

        item_b = HydraIface.ConstraintItem(constraint=con)
        item_b.db.constraint_id = con.db.constraint_id
        item_b.db.resource_attr_id = rb.db.resource_attr_id
        item_b.save()

        item_c = HydraIface.ConstraintItem(constraint=con)
        item_c.db.constraint_id = con.db.constraint_id
        item_c.db.resource_attr_id = rc.db.resource_attr_id
        item_c.save()

        grp_a = HydraIface.ConstraintGroup()
        grp_a.db.constraint_id = con.db.constraint_id
        grp_a.db.ref_key_1 = 'ITEM'
        grp_a.db.ref_id_1  = item_a.db.item_id
        grp_a.db.ref_key_2 = 'ITEM'
        grp_a.db.ref_id_2  = item_constant.db.item_id
        grp_a.db.op        = "*"
        grp_a.save()

        grp_b = HydraIface.ConstraintGroup()
        grp_b.db.constraint_id = con.db.constraint_id
        grp_b.db.ref_key_1 = 'GRP'
        grp_b.db.ref_id_1  = grp_a.db.group_id
        grp_b.db.ref_key_2 = 'ITEM'
        grp_b.db.ref_id_2  = item_b.db.item_id
        grp_b.db.op        = "-"
        grp_b.save()

        grp_c = HydraIface.ConstraintGroup()
        grp_c.db.constraint_id = con.db.constraint_id
        grp_c.db.ref_key_1 = 'GRP'
        grp_c.db.ref_id_1  = grp_b.db.group_id
        grp_c.db.ref_key_2 = 'ITEM'
        grp_c.db.ref_id_2  = item_c.db.item_id
        grp_c.db.op        = '-'
        grp_c.save()

        con.db.group_id = grp_c.db.group_id
        con.save()
        con.commit()

        condition_string = con.eval_condition()

        assert eval(condition_string) == True, \
                    "Condition %s did not evaluate"%condition_string

        return network

if __name__ == "__main__":
    test_HydraIface.run() # run all tests
