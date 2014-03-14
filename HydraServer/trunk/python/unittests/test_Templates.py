# (c) Copyright 2013, 2014, University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
import test_HydraIface
from db import HydraIface
import datetime
from HydraLib.HydraException import HydraError

class TemplateTest(test_HydraIface.HydraIfaceTest):
    def test_update(self):
        x = HydraIface.Template()
        x.db.template_name = "test @ %s"%datetime.datetime.now()
        x.save()
        x.commit()

        new_name = "new test @ %s"%datetime.datetime.now()
        x.db.template_name = new_name
        x.save()
        x.commit()
        x.load()
        assert x.db.template_name == new_name, "Template did not update correctly"

    def test_delete(self):
        x = HydraIface.Template()
        x.db.template_name = "test @ %s"%datetime.datetime.now()
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.Template()
        x.db.template_name = "test @ %s"%datetime.datetime.now()
        x.save()
        x.commit()
        x.load()
        y = HydraIface.Template(template_id=x.db.template_id)
        assert y.load() == True, "Load did not work correctly"


class TemplateTypeTest(test_HydraIface.HydraIfaceTest):
    def test_update(self):
        x = HydraIface.TemplateType()

        x.db.type_name = "test"
        x.save()
        x.commit()

        x.db.type_name = "test_new"
        x.save()
        x.commit()
        x.load()
        assert x.db.type_name == "test_new", "TemplateType did not update correctly"

    def test_delete(self):
        x = HydraIface.TemplateType()
        x.db.type_name = "test @ %s" % datetime.datetime.now()
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_correct_fk(self):
        x = HydraIface.Template()
        x.db.template_name = "test @ %s"%datetime.datetime.now()
        x.save()
        x.commit()
        x.load()

        y = HydraIface.TemplateType()
        y.db.type_name = 'test_fk'
        y.db.template_id      = x.db.template_id

        y.save()
        y.commit()
        y.load()

    def test_incorrect_fk(self):
        x = HydraIface.Template()
        x.db.template_name = "test @ %s"%datetime.datetime.now()
        x.save()
        x.commit()
        x.load()

        y = HydraIface.TemplateType()
        y.db.type_name = 'test_fk'
        y.db.template_id      = x.db.template_id + 1
        self.assertRaises(HydraError, y.save)

    def test_load(self):
        x = HydraIface.TemplateType()
        x.db.type_name = "test @ %s" % datetime.datetime.now()
        x.save()
        x.commit()
        x.load()
        y = HydraIface.TemplateType(type_id=x.db.type_id)
        assert y.load() == True, "Load did not work correctly"

class TypeAttrTest(test_HydraIface.HydraIfaceTest):


    def create_type(self, name):
        x = HydraIface.TemplateType()
        x.db.type_name = "test @ %s"%(datetime.datetime.now())
        x.save()
        x.commit()
        x.load()
        return x

    def test_update(self):

        a = self.create_attribute("attr1")
        t = self.create_type("type1")
        t1 = self.create_type("type2")

        x = HydraIface.TypeAttr(attr_id = a.db.attr_id, type_id=t.db.type_id)
        x.save()
        x.commit()

        x.db.type_id = t1.db.type_id
        x.save()
        x.commit()
        x.load()
        assert x.db.type_id == t1.db.type_id, "TypeAttr did not update correctly"

    def test_delete(self):
        a = self.create_attribute("attr1")
        t = self.create_type("type1")

        x = HydraIface.TypeAttr(attr_id = a.db.attr_id, type_id=t.db.type_id)
        x.save()
        x.commit()
        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_fk(self):
        a = self.create_attribute("attr1")
        t = self.create_type("type1")

        x = HydraIface.TypeAttr(attr_id=a.db.attr_id, type_id=0)
        self.assertRaises(HydraError, x.save)

        y = HydraIface.TypeAttr(attr_id=0, type_id=t.db.type_id)
        self.assertRaises(HydraError, y.save)

    def test_load(self):
        a = self.create_attribute("attr1")
        t = self.create_type("type1")

        x = HydraIface.TypeAttr(attr_id = a.db.attr_id, type_id=t.db.type_id)
        x.save()
        x.commit()
        x.load()

        y = HydraIface.TypeAttr(attr_id = x.db.attr_id, type_id=x.db.type_id)

        assert y.load() is True, "Load did not work correctly"


class TestResourceType(test_HydraIface.HydraIfaceTest):

    def create_type(self, attribute):
        x = HydraIface.TemplateType()
        x.db.type_name = "test @ %s"%(datetime.datetime.now())
        x.save()
        x.commit()
        it = HydraIface.TypeAttr(attr_id=attribute.db.attr_id,
                                     type_id=x.db.type_id)
        it.save()
        it.commit()
        x.load()
        return x

    def create_resource(self):
        proj = self.create_project('Test proj @ %s' % datetime.datetime.now())
        netw = self.create_network('test @ %s' % datetime.datetime.now(),
                                   proj.db.project_id)
        node = self.create_node('test @ %s' % datetime.datetime.now(),
                                netw.db.network_id)
        return node

    def test_load(self):
        attr = self.create_attribute('attr2')
        tmpl = self.create_type(attr)
        node = self.create_resource()

        x = HydraIface.ResourceType('NODE', node.db.node_id, tmpl.db.type_id)
        x.save()
        x.commit()
        x.load()

        y = HydraIface.ResourceType('NODE', node.db.node_id, tmpl.db.type_id)
        assert y.load() is True, 'Loading resource type did not work properly.'

    def test_get_type(self):
        attr = self.create_attribute('attr2')
        tmpl = self.create_type(attr)
        node = self.create_resource()

        x = HydraIface.ResourceType('NODE', node.db.node_id, tmpl.db.type_id)
        x.save()
        x.commit()
        x.load()

        new_tmpl = x.get_type()

        assert tmpl.db.type_name == new_tmpl.db.type_name and \
                tmpl.db.type_id == new_tmpl.db.type_id, \
                'get_type did not work correctly.'

if __name__ == "__main__":
    test_HydraIface.run() # run all tests
