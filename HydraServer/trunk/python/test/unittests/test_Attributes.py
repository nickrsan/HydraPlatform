import test_HydraIface
from db import HydraIface

class AttributeTest(test_HydraIface.HydraIfaceTest):

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

    def test_load(self):
        x = HydraIface.Attr()
        x.db.attr_name = "test"
        x.db.attr_description = "test description"
        x.save()
        x.commit()
        x.load()

        y = HydraIface.Attr(attr_id=x.db.attr_id)
        assert y.load() == True, "Load did not work correctly"

if __name__ == "__main__":
    test_HydraIface.run() # run all tests
