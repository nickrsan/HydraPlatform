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

class AttrMapTest(test_HydraIface.HydraIfaceTest):

    def create_attribute(self, name):
        x = HydraIface.Attr()
        x.db.attr_name = name
        x.db.attr_description = "test description"
        x.save()
        x.commit()
        x.load()
        return x

    def test_update(self):
        a1 = self.create_attribute("Attr1")
        a2 = self.create_attribute("Attr2")
        a3 = self.create_attribute("Attr3")

        am = HydraIface.AttrMap()
        am.db.attr_id_a = a1.db.attr_id
        am.db.attr_id_b = a2.db.attr_id

        am.save()
        am.commit()
        am.load()

        am.db.attr_id_b = a3.db.attr_id
        am.save()
        am.commit()
        am.load()
        assert am.db.attr_id_b == a3.db.attr_id, "AttrMap did not update correctly"

    def test_delete(self):
        a1 = self.create_attribute("Attr1")
        a2 = self.create_attribute("Attr2")

        am = HydraIface.AttrMap()
        am.db.attr_id_a = a1.db.attr_id
        am.db.attr_id_b = a2.db.attr_id

        am.save()
        am.commit()
        am.load()

        am.delete()
        assert am.load() == False, "AttrMap did not delete correctly"

    def test_load(self):
        a1 = self.create_attribute("Attr1")
        a2 = self.create_attribute("Attr2")

        am = HydraIface.AttrMap()
        am.db.attr_id_a = a1.db.attr_id
        am.db.attr_id_b = a2.db.attr_id

        am.save()
        am.commit()
        am.load()

        am = HydraIface.AttrMap(attr_id_a=a1.db.attr_id, attr_id_b=a2.db.attr_id)
        assert am.load() == True, "AttrMap did not load correctly"
    
    def test_fk(self):
        a1 = self.create_attribute("Attr1")
        a2 = self.create_attribute("Attr2")

        am1 = HydraIface.AttrMap(attr_id_a = a1.db.attr_id, attr_id_b=0)
        self.assertRaises(Exception, am1.save)
        am2 = HydraIface.AttrMap(attr_id_a = 0, attr_id_b=a2.db.attr_id)
        self.assertRaises(Exception, am2.save)

if __name__ == "__main__":
    test_HydraIface.run() # run all tests
