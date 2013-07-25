import unittest
from db import HydraIface
from HydraLib import util, hydra_logging

class IfaceTest(unittest.TestCase):
    def setUp(self):
        self.connection = util.connect()
        hydra_logging.init(level='DEBUG')
 
    def tearDown(self):
        hydra_logging.shutdown()
        util.disconnect(self.connection)

    def test_loadProject(self):
        cursor = self.connection.cursor()
        cursor.execute("select min(project_id) as project_id from tProject")
        project_id = cursor.fetchall()[0][0]
        x = HydraIface.Project(project_id=project_id)
        assert x.load() == True, "Load did not work correctly"

    def test_updateProject(self):
        x = HydraIface.Project()
        x.db.project_name = "test"
        x.db.project_description = "test description"
        x.save()
        x.commit()

        x.db.project_name = "test_new"
        x.save()
        x.commit()
        x.load()
        assert x.db.project_name == "test_new", "Project did not update correctly"


    def test_deleteProject(self):
        x = HydraIface.Project()
        x.db.project_name = "test"
        x.db.project_description = "test description"
        x.save()
        x.commit()
        assert x.load() == True, "Pre-delete save did not work correctly."

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_loadNetwork(self):
        cursor = self.connection.cursor()
        cursor.execute("select min(network_id) as network_id from tNetwork")
        network_id = cursor.fetchall()[0][0]
        x = HydraIface.Network(network_id=network_id)
        assert x.load() == True, "Load did not work correctly"

    def test_updateNetwork(self):
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


    def test_deleteNetwork(self):
        x = HydraIface.Network()
        x.db.network_name = "test"
        x.db.network_description = "test description"
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_loadNode(self):
        cursor = self.connection.cursor()
        cursor.execute("select min(node_id) as node_id from tNode")
        node_id = cursor.fetchall()[0][0]
        x = HydraIface.Node(node_id=node_id)
        assert x.load() == True, "Load did not work correctly"

    def test_updateNode(self):
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


    def test_deleteNode(self):
        x = HydraIface.Node()
        x.db.node_name = "test"
        x.db.node_description = "test description"
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."



if __name__ == "__main__":
    unittest.main() # run all tests