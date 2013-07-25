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
        cursor.execute("select min(user_id) as user_id from tUser")
        user_id = cursor.fetchall()[0][0]
        x = HydraIface.User(user_id=user_id)
        assert x.load() == True, "Load did not work correctly"

    def test_update(self):
        x = HydraIface.User()
        x.db.user_name = "test"
        x.db.user_description = "test description"
        x.save()
        x.commit()

        x.db.user_name = "test_new"
        x.save()
        x.commit()
        x.load()
        assert x.db.user_name == "test_new", "User did not update correctly"

    def test_delete(self):
        x = HydraIface.User()
        x.db.user_name = "test"
        x.db.user_description = "test description"
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

if __name__ == "__main__":
    unittest.main() # run all tests
