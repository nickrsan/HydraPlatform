import test_HydraIface
from db import HydraIface

class NetworkTest(test_HydraIface.HydraIfaceTest):

    def test_update(self):
        x = HydraIface.Network()
        x.db.network_name = "test"
        x.db.network_description = "test description"
        x.save()
        x.commit()

        x.db.network_name = "test_new"
        x.save()
        x.commit()
        x.load()
        assert x.db.network_name == "test_new", "Network did not update correctly"

    def test_delete(self):
        x = HydraIface.Network()
        x.db.network_name = "test"
        x.db.network_description = "test description"
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.Network()
        x.db.network_name = "test"
        x.db.network_description = "test description"
        x.save()
        x.commit()
        x.load()
        y = HydraIface.Network(network_id=x.db.network_id)
        assert y.load() == True, "Load did not work correctly"

class LinkTest(test_HydraIface.HydraIfaceTest):

    def test_update(self):
        x = HydraIface.Link()
        x.db.link_name = "test"
        x.db.link_description = "test description"
        x.save()
        x.commit()

        x.db.link_name = "test_new"
        x.save()
        x.commit()
        x.load()
        assert x.db.link_name == "test_new", "Link did not update correctly"

    def test_delete(self):
        x = HydraIface.Link()
        x.db.link_name = "test"
        x.db.link_description = "test description"
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.Link()
        x.db.link_name = "test"
        x.db.link_description = "test description"
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
