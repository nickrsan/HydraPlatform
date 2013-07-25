import unittest
from db import HydraIface
from HydraLib import util, hydra_logging

class ResourceTemplateGroupIfaceTest(unittest.TestCase):
    def setUp(self):
        self.connection = util.connect()
        hydra_logging.init(level='DEBUG')
 
    def tearDown(self):
        hydra_logging.shutdown()
        util.disconnect(self.connection)

    def test_load(self):
        cursor = self.connection.cursor()
        cursor.execute("select min(group_id) as group_id from tResourceTemplateGroup")
        group_id = cursor.fetchall()[0][0]
        x = HydraIface.ResourceTemplateGroup(group_id=group_id)
        assert x.load() == True, "Load did not work correctly"

    def test_update(self):
        x = HydraIface.ResourceTemplateGroup()
        x.db.group_name = "test"
        x.save()
        x.commit()

        x.db.group_name = "test_new"
        x.save()
        x.commit()
        x.load()
        assert x.db.group_name == "test_new", "ResourceTemplate did not update correctly"

    def test_delete(self):
        x = HydraIface.ResourceTemplateGroup()
        x.db.group_name = "test"
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

class ResourceTemplateIfaceTest(unittest.TestCase):
    def setUp(self):
        self.connection = util.connect()
        hydra_logging.init(level='DEBUG')
 
    def tearDown(self):
        hydra_logging.shutdown()
        util.disconnect(self.connection)

    def test_load(self):
        cursor = self.connection.cursor()
        cursor.execute("select min(template_id) as template_id from tResourceTemplate")
        template_id = cursor.fetchall()[0][0]
        x = HydraIface.ResourceTemplate(template_id=template_id)
        assert x.load() == True, "Load did not work correctly"

    def test_update(self):
        x = HydraIface.ResourceTemplate()
        x.db.template_name = "test"
        x.save()
        x.commit()

        x.db.template_name = "test_new"
        x.save()
        x.commit()
        x.load()
        assert x.db.template_name == "test_new", "ResourceTemplate did not update correctly"

    def test_delete(self):
        x = HydraIface.ResourceTemplate()
        x.db.template_name = "test"
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."
	
    def test_correct_fk(self):
        x = HydraIface.ResourceTemplateGroup()
        x.db.group_name = "test"
        x.save()
        x.commit()
        x.load()

        y = HydraIface.ResourceTemplate()
        y.db.template_name = 'test_fk'
        y.db.group_id      = x.db.group_id

        y.save()
        y.commit()
        y.load()

    def test_incorrect_fk(self):
        x = HydraIface.ResourceTemplateGroup()
        x.db.group_name = "test"
        x.save()
        x.commit()
        x.load()

        y = HydraIface.ResourceTemplate()
        y.db.template_name = 'test_fk'
        y.db.group_id      = x.db.group_id + 1

        self.assertRaises(Exception, y.save)

if __name__ == "__main__":
    unittest.main() # run all tests
