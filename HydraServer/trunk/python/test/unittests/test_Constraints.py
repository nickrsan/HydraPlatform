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

        node_a = self.create_node("Node A")
        node_b = self.create_node("Node B")
        node_c = self.create_node("Node C")

        attr_a = self.create_attribute("Flow")

        ra = node_a.add_attribute(attr_a.db.attr_id)
        rb = node_b.add_attribute(attr_a.db.attr_id)
        rc = node_c.add_attribute(attr_a.db.attr_id)

        rsa = node_a.assign_value(scenario.db.scenario_id, ra.db.resource_attr_id, 'scalar' ,1, 'm3', 'flow', 'int')
        rsb = node_a.assign_value(scenario.db.scenario_id, rb.db.resource_attr_id, 'scalar' ,2, 'm3', 'flow', 'int')
        rsc = node_a.assign_value(scenario.db.scenario_id, rc.db.resource_attr_id, 'scalar' ,3, 'm3', 'flow', 'int')


        con = HydraIface.Constraint()
        con.db.constraint_name = "Comparative resource allocation"
        con.db.constraint_description = "AB must be >= C"
        con.db.scenario_id            = scenario.db.scenario_id
        con.db.constant               = 0
        con.db.op                     = "=="
        con.save()
        con.commit()
        con.load()

        item_a = HydraIface.ConstraintItem()
        item_a.db.constraint_id = con.db.constraint_id
        item_a.db.resource_attr_id = ra.db.resource_attr_id
        item_a.save()
        item_a.commit()
        item_a.load()

        item_b = HydraIface.ConstraintItem()
        item_b.db.constraint_id = con.db.constraint_id
        item_b.db.resource_attr_id = rb.db.resource_attr_id
        item_b.save()
        item_b.commit()
        item_b.load()

        item_c = HydraIface.ConstraintItem()
        item_c.db.constraint_id = con.db.constraint_id
        item_c.db.resource_attr_id = rc.db.resource_attr_id
        item_c.save()
        item_c.commit()
        item_c.load()


        grp_a = HydraIface.ConstraintGroup()
        grp_a.db.constraint_id = con.db.constraint_id
        grp_a.db.ref_key_1 = 'ITEM'
        grp_a.db.ref_id_1  = item_a.db.item_id
        grp_a.db.ref_key_2 = 'ITEM'
        grp_a.db.ref_id_2  = item_b.db.item_id
        grp_a.db.op        = "+"
        grp_a.save()
        grp_a.commit()
        grp_a.load()

        grp_b = HydraIface.ConstraintGroup()
        grp_b.db.constraint_id = con.db.constraint_id
        grp_b.db.ref_key_1 = 'GRP'
        grp_b.db.ref_id_1  = grp_a.db.group_id
        grp_b.db.ref_key_2 = 'ITEM'
        grp_b.db.ref_id_2  = item_c.db.item_id
        grp_b.db.op        = '-'
        grp_b.save()
        grp_b.commit()
        grp_b.load()

        con.db.group_id = grp_b.db.group_id
        con.save()
        con.commit()

        condition_string = con.eval_condition()
        assert eval(condition_string) == True, \
                    "Condition %s did not evaluate"%condition_string

if __name__ == "__main__":
    test_HydraIface.run() # run all tests
