import test_HydraIface
import mysql.connector
from db import HydraIface
import datetime
from HydraLib.HydraException import HydraError

class TemplateGroupTest(test_HydraIface.HydraIfaceTest):
    def test_update(self):
        x = HydraIface.TemplateGroup()
        x.db.group_name = "test @ %s"%datetime.datetime.now()
        x.save()
        x.commit()

        new_name = "new test @ %s"%datetime.datetime.now()
        x.db.group_name = new_name
        x.save()
        x.commit()
        x.load()
        assert x.db.group_name == new_name, "Template did not update correctly"

    def test_delete(self):
        x = HydraIface.TemplateGroup()
        x.db.group_name = "test @ %s"%datetime.datetime.now()
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.TemplateGroup()
        x.db.group_name = "test @ %s"%datetime.datetime.now()
        x.save()
        x.commit()
        x.load()
        y = HydraIface.TemplateGroup(group_id=x.db.group_id)
        assert y.load() == True, "Load did not work correctly"


class TemplateTest(test_HydraIface.HydraIfaceTest):
    def test_update(self):
        x = HydraIface.Template()

        x.db.template_name = "test"
        x.save()
        x.commit()

        x.db.template_name = "test_new"
        x.save()
        x.commit()
        x.load()
        assert x.db.template_name == "test_new", "Template did not update correctly"

    def test_delete(self):
        x = HydraIface.Template()
        x.db.template_name = "test @ %s" % datetime.datetime.now()
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."
	
    def test_correct_fk(self):
        x = HydraIface.TemplateGroup()
        x.db.group_name = "test @ %s"%datetime.datetime.now()
        x.save()
        x.commit()
        x.load()

        y = HydraIface.Template()
        y.db.template_name = 'test_fk'
        y.db.group_id      = x.db.group_id

        y.save()
        y.commit()
        y.load()

    def test_incorrect_fk(self):
        x = HydraIface.TemplateGroup()
        x.db.group_name = "test @ %s"%datetime.datetime.now()
        x.save()
        x.commit()
        x.load()

        y = HydraIface.Template()
        y.db.template_name = 'test_fk'
        y.db.group_id      = x.db.group_id + 1
        self.assertRaises(HydraError, y.save)

    def test_load(self):
        x = HydraIface.Template()
        x.db.template_name = "test @ %s" % datetime.datetime.now()
        x.save()
        x.commit()
        x.load()
        y = HydraIface.Template(template_id=x.db.template_id)
        assert y.load() == True, "Load did not work correctly"

class TemplateItemTest(test_HydraIface.HydraIfaceTest):


    def create_template(self, name):
        x = HydraIface.Template()
        x.db.template_name = "test @ %s"%(datetime.datetime.now())
        x.save()
        x.commit()
        x.load()
        return x

    def test_update(self):

        a = self.create_attribute("attr1")
        t = self.create_template("template1")
        t1 = self.create_template("template2")

        x = HydraIface.TemplateItem(attr_id = a.db.attr_id, template_id=t.db.template_id)
        x.save()
        x.commit()

        x.db.template_id = t1.db.template_id
        x.save()
        x.commit()
        x.load()
        assert x.db.template_id == t1.db.template_id, "TemplateItem did not update correctly"

    def test_delete(self):
        a = self.create_attribute("attr1")
        t = self.create_template("template1")

        x = HydraIface.TemplateItem(attr_id = a.db.attr_id, template_id=t.db.template_id)
        x.save()
        x.commit()
        x.delete()
        assert x.load() == False, "Delete did not work correctly."
	
    def test_fk(self):
        a = self.create_attribute("attr1")
        t = self.create_template("template1")

        x = HydraIface.TemplateItem(attr_id=a.db.attr_id, template_id=0)
        self.assertRaises(HydraError, x.save)

        y = HydraIface.TemplateItem(attr_id=0, template_id=t.db.template_id)
        self.assertRaises(HydraError, y.save)

    def test_load(self):
        a = self.create_attribute("attr1")
        t = self.create_template("template1")

        x = HydraIface.TemplateItem(attr_id = a.db.attr_id, template_id=t.db.template_id)
        x.save()
        x.commit()
        x.load()
       
        y = HydraIface.TemplateItem(attr_id = x.db.attr_id, template_id=x.db.template_id)

        assert y.load() == True, "Load did not work correctly"



if __name__ == "__main__":
    test_HydraIface.run() # run all tests
