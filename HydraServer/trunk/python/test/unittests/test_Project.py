import unittest
from db import HydraIface
from HydraLib import util, hydra_logging

class HydraIfaceTest(unittest.TestCase):
    def setUp(self):
        self.connection = util.connect()
        hydra_logging.init(level='DEBUG')
 
    def tearDown(self):
        hydra_logging.shutdown()
        util.disconnect(self.connection)

    def test_load(self):
        cursor = self.connection.cursor()
        cursor.execute("select min(project_id) as project_id from tProject")
        project_id = cursor.fetchall()[0][0]
        x = HydraIface.Project(project_id=project_id)
        assert x.load() == True, "Load did not work correctly"

    def test_update(self):
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

    def test_delete(self):
        x = HydraIface.Project()
        x.db.project_name = "test"
        x.db.project_description = "test description"
        x.save()
        x.commit()
        assert x.load() == True, "Pre-delete save did not work correctly."

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

if __name__ == "__main__":
    unittest.main() # run all tests
