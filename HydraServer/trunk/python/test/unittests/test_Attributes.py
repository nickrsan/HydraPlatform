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
        cursor.execute("select min(attr_id) as attr_id from tAttr")
        attr_id = cursor.fetchall()[0][0]
        x = HydraIface.Attr(attr_id=attr_id)
        assert x.load() == True, "Load did not work correctly"

    def test_update(self):
        x = HydraIface.Attr()
        x.db.attr_name = "test"
        x.db.attr_description = "test description"
        x.save()
        x.commit()

        x.db.attr_name = "test_new"
        x.save()
        x.commit()
        x.load()
        assert x.db.attr_name == "test_new", "Attr did not update correctly"

    def test_delete(self):
        x = HydraIface.Attr()
        x.db.attr_name = "test"
        x.db.attr_description = "test description"
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

if __name__ == "__main__":
    unittest.main() # run all tests
