import test_HydraIface
from db import HydraIface
import mysql.connector

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

        y = HydraIface.Attr(attr_id=x.db.attr_id)
        assert y.load() == True, "Load did not work correctly"

class AttrMapTest(test_HydraIface.HydraIfaceTest):

    def create_attribute(self, name):
        x = HydraIface.Attr()
        x.db.attr_name = name
        x.db.attr_description = "test description"
        x.save()
        x.commit()
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

        am.db.attr_id_b = a3.db.attr_id
        am.save()
        am.commit()
        assert am.db.attr_id_b == a3.db.attr_id, "AttrMap did not update correctly"

    def test_delete(self):
        a1 = self.create_attribute("Attr1")
        a2 = self.create_attribute("Attr2")

        am = HydraIface.AttrMap()
        am.db.attr_id_a = a1.db.attr_id
        am.db.attr_id_b = a2.db.attr_id

        am.save()
        am.commit()

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

        am = HydraIface.AttrMap(attr_id_a=a1.db.attr_id, attr_id_b=a2.db.attr_id)
        assert am.load() == True, "AttrMap did not load correctly"
    
    def test_fk(self):
        a1 = self.create_attribute("Attr1")
        a2 = self.create_attribute("Attr2")

        am1 = HydraIface.AttrMap(attr_id_a = a1.db.attr_id, attr_id_b=0)
        self.assertRaises(mysql.connector.DatabaseError, am1.save)
        am2 = HydraIface.AttrMap(attr_id_a = 0, attr_id_b=a2.db.attr_id)
        self.assertRaises(mysql.connector.DatabaseError, am2.save)

class ResourceAttrTest(test_HydraIface.HydraIfaceTest):

    def create_attribute(self, name):
        x = HydraIface.Attr()
        x.db.attr_name = name
        x.db.attr_description = "test description"
        x.save()
        x.commit()
        return x

    def test_create(self):
        n1 = self.create_node("Node1")
        a1 = self.create_attribute("Attr1")

        ra = HydraIface.ResourceAttr()
        ra.db.attr_id = a1.db.attr_id
        ra.db.ref_key = 'NODE'
        ra.db.ref_id  = n1.db.node_id

        ra.save()
        ra.commit()

        assert ra.load() == True, "AttrMap did not update correctly"

    def test_delete(self):
        n1 = self.create_node("Node1")
        a1 = self.create_attribute("Attr1")

        ra = HydraIface.ResourceAttr()
        ra.db.attr_id = a1.db.attr_id
        ra.db.ref_key = 'NODE'
        ra.db.ref_id  = n1.db.node_id

        ra.save()
        ra.commit()

        ra.delete()
        assert ra.load() == False, "AttrMap did not delete correctly"

    def test_load(self):
        n1 = self.create_node("Node1")
        a1 = self.create_attribute("Attr1")

        ra1 = HydraIface.ResourceAttr()
        ra1.db.attr_id = a1.db.attr_id
        ra1.db.ref_key = 'NODE'
        ra1.db.ref_id  = n1.db.node_id

        ra1.save()
        ra1.commit()
        ra2 = HydraIface.ResourceAttr(resource_attr_id=ra1.db.resource_attr_id)
  
        n1.load()
        n1.get_attributes()

        assert ra2.load() == True, "ResourceAttr did not load correctly"

    def test_fk(self):
        n1 = self.create_node("Node1")

        ra = HydraIface.ResourceAttr()
        ra.db.attr_id = 0
        ra.db.ref_key = 'NODE'
        ra.db.ref_id  = n1.db.node_id
        self.assertRaises(mysql.connector.DatabaseError, ra.save)


if __name__ == "__main__":
    test_HydraIface.run() # run all tests
