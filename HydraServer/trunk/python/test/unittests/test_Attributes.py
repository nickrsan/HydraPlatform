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
        self.x = x

    def test_delete(self):
        x = HydraIface.Attr()
        x.db.attr_name = "test"
        x.db.attr_description = "test description"
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."
        self.x = x

    def test_load(self):
        x = HydraIface.Attr()
        x.db.attr_name = "test"
        x.db.attr_description = "test description"
        x.save()
        x.commit()

        y = HydraIface.Attr(attr_id=x.db.attr_id)
        assert y.load() == True, "Load did not work correctly"
        self.x = x

class AttrMapTest(test_HydraIface.HydraIfaceTest):

    def test_update(self):
        a1 = self.create_attribute("Attr1")
        a2 = self.create_attribute("Attr2")
        a3 = self.create_attribute("Attr3")

        am = HydraIface.AttrMap()
        am.db.attr_id_a = a1.db.attr_id
        am.db.attr_id_b = a2.db.attr_id

        am.save()
        am.commit()

        assert am.load(), "Attr map was not created"
        
        am.delete()
        
        am1 = HydraIface.AttrMap()
        am1.db.attr_id_a = a2.db.attr_id
        am1.db.attr_id_b = a3.db.attr_id
        am1.save()
        am1.commit()

        assert am1.load(), "AttrMap did not update correctly"
        self.am = am1

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
        self.am = am

    def test_fk(self):
        a1 = self.create_attribute("Attr1")
        a2 = self.create_attribute("Attr2")

        am1 = HydraIface.AttrMap(attr_id_a = a1.db.attr_id, attr_id_b=0)
        self.assertRaises(mysql.connector.DatabaseError, am1.save)
        am2 = HydraIface.AttrMap(attr_id_a = 0, attr_id_b=a2.db.attr_id)
        self.assertRaises(mysql.connector.DatabaseError, am2.save)

class ResourceAttrTest(test_HydraIface.HydraIfaceTest):

    def create_attribute(self, name):
        sql = """
            select
                attr_id
            from
                tAttr
            where
                attr_name = '%s'
        """ % name
        
        rs = HydraIface.execute(sql)

        if len(rs) == 0:
            x = HydraIface.Attr()
            x.db.attr_name = name
            x.db.attr_description = "test description"
            x.save()
            x.commit()
        else:
            x = HydraIface.Attr(attr_id=rs[0].attr_id)
            x.load()

        return x

    def test_create(self):
        proj = self.create_project("Test Proj)")
        net = self.create_network("Test Net", proj.db.project_id)
        n1 = self.create_node("Node1", net.db.network_id)
        a1 = self.create_attribute("Attr1")

        ra = HydraIface.ResourceAttr()
        ra.db.attr_id = a1.db.attr_id
        ra.db.ref_key = 'NODE'
        ra.db.ref_id  = n1.db.node_id

        ra.save()
        ra.commit()

        assert ra.load() == True, "AttrMap did not update correctly"

    def test_delete(self):
        proj = self.create_project("Test Proj)")
        net = self.create_network("Test Net", proj.db.project_id)
        n1 = self.create_node("Node1", net.db.network_id)
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

        proj = self.create_project("Test Proj)")
        net = self.create_network("Test Net", proj.db.project_id)
        n1 = self.create_node("Node1", net.db.network_id)
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

        proj = self.create_project("Test Proj)")
        net = self.create_network("Test Net", proj.db.project_id)
        n1 = self.create_node("Node1", net.db.network_id)

        ra = HydraIface.ResourceAttr()
        ra.db.attr_id = 0
        ra.db.ref_key = 'NODE'
        ra.db.ref_id  = n1.db.node_id
        self.assertRaises(mysql.connector.DatabaseError, ra.save)


if __name__ == "__main__":
    test_HydraIface.run() # run all tests
