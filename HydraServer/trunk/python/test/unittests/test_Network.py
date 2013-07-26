import test_HydraIface
from db import HydraIface

class NetworkTest(test_HydraIface.HydraIfaceTest):

    def test_update(self):
        proj = self.create_project("test proj")
        x = HydraIface.Network()
        x.db.network_name = "test"
        x.db.network_description = "test description"
        x.db.project_id = proj.db.project_id
        x.save()
        x.commit()

        x.db.network_name = "test_new"
        x.save()
        x.commit()
        x.load()
        assert x.db.network_name == "test_new", "Network did not update correctly"

    def test_delete(self):
        proj = self.create_project("test proj")
        x = HydraIface.Network()
        x.db.network_name = "test"
        x.db.network_description = "test description"
        x.db.project_id = proj.db.project_id
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        proj = self.create_project("test proj")
        x = HydraIface.Network()
        x.db.network_name = "test"
        x.db.network_description = "test description"
        x.db.project_id = proj.db.project_id
        x.save()
        x.commit()
        x.load()
        y = HydraIface.Network(network_id=x.db.network_id)
        assert y.load() == True, "Load did not work correctly"

class LinkTest(test_HydraIface.HydraIfaceTest):

    def test_update(self):
        proj = self.create_project("Test Proj)")
        net = self.create_network("Test Net", proj.db.project_id)
        node_a = self.create_node("Node A")
        node_b = self.create_node("Node B")
        x = HydraIface.Link()
        x.db.link_name = "test"
        x.db.link_description = "test description"
        x.db.network_id = net.db.network_id
        x.db.node_1_id = node_a.db.node_id
        x.db.node_2_id = node_b.db.node_id
        x.save()
        x.commit()

        x.db.link_name = "test_new"
        x.save()
        x.commit()
        x.load()
        assert x.db.link_name == "test_new", "Link did not update correctly"

    def test_delete(self):
        proj = self.create_project("Test Proj)")
        net = self.create_network("Test Net", proj.db.project_id)
        node_a = self.create_node("Node A")
        node_b = self.create_node("Node B")
        x = HydraIface.Link()
        x.db.link_name = "test"
        x.db.link_description = "test description"
        x.db.network_id = net.db.network_id
        x.db.node_1_id = node_a.db.node_id
        x.db.node_2_id = node_b.db.node_id
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        proj = self.create_project("Test Proj)")
        net = self.create_network("Test Net", proj.db.project_id)
        node_a = self.create_node("Node A")
        node_b = self.create_node("Node B")
        x = HydraIface.Link()
        x.db.link_name = "test"
        x.db.link_description = "test description"
        x.db.network_id = net.db.network_id
        x.db.node_1_id = node_a.db.node_id
        x.db.node_2_id = node_b.db.node_id
        x.save()
        x.commit()
        x.load()

        y = HydraIface.Link(link_id=x.db.link_id)
        assert y.load() == True, "Load did not work correctly"

class NodeTest(test_HydraIface.HydraIfaceTest):

    def test_update(self):
        x = HydraIface.Node()
        x.db.node_name = "test"
        x.db.node_description = "test description"
        x.save()
        x.commit()

        x.db.node_name = "test_new"
        x.save()
        x.commit()
        x.load()
        assert x.db.node_name == "test_new", "Node did not update correctly"


    def test_delete(self):
        x = HydraIface.Node()
        x.db.node_name = "test"
        x.db.node_description = "test description"
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.Node()
        x.db.node_name = "test"
        x.db.node_description = "test description"
        x.save()
        x.commit()
        x.load()
        y = HydraIface.Node(node_id=x.db.node_id)
        assert y.load() == True, "Load did not work correctly"

if __name__ == "__main__":
    test_HydraIface.run() # run all tests
