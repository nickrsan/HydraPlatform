import test_HydraIface
from db import HydraIface

class ResourceTemplateGroupTest(test_HydraIface.HydraIfaceTest):
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

    def test_load(self):
        x = HydraIface.ResourceTemplateGroup()
        x.db.group_name = "test"
        x.save()
        x.commit()
        x.load()
        y = HydraIface.ResourceTemplateGroup(group_id=x.db.group_id)
        assert y.load() == True, "Load did not work correctly"


class ResourceTemplateTest(test_HydraIface.HydraIfaceTest):
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

    def test_load(self):
        x = HydraIface.ResourceTemplate()
        x.db.template_name = "test"
        x.save()
        x.commit()
        x.load()
        y = HydraIface.ResourceTemplate(template_id=x.db.template_id)
        assert y.load() == True, "Load did not work correctly"


if __name__ == "__main__":
    test_HydraIface.run() # run all tests
