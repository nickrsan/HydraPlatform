
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
import bcrypt
import datetime

class TemplatesTest(test_SoapServer.SoapServerTest):

    def test_add_group(self):

        attr_1 = self.create_attr() 
        attr_2 = self.create_attr() 
        attr_3 = self.create_attr() 
        
        group = self.client.factory.create('hyd:ResourceTemplateGroup')
        group.name = 'Test Group'

        
        templates = self.client.factory.create('hyd:ResourceTemplateArray')

        template = self.client.factory.create('hyd:ResourceTemplate')
        template.name = "Test template name"

        items = self.client.factory.create('hyd:ResourceTemplateItemArray')
        
        item_1 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_1.attr_id = attr_1.id
        items.ResourceTemplateItem.append(item_1)

        item_2 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_2.attr_id = attr_2.id
        items.ResourceTemplateItem.append(item_2)
        
        item_3 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_3.attr_id = attr_3.id
        items.ResourceTemplateItem.append(item_3)

        templates.ResourceTemplate.append(template)

        template.resourcetemplateitems = items
        
        group.resourcetemplates = templates

        new_group = self.client.service.add_resourcetemplategroup(group)
        
        assert new_group.name == group.name, "Names are not the same!"
        assert new_group.id is not None, "New Group has no ID!"
        assert new_group.id > 0, "New Group has incorrect ID!"

        assert len(new_group.resourcetemplates) == 1, "Resource templates did not add correctly"
        assert len(new_group.resourcetemplates[0][0].resourcetemplateitems[0]) == 3, "Resource template items did not add correctly"


    def test_update_group(self):

        attr_1 = self.create_attr() 
        attr_2 = self.create_attr() 
        attr_3 = self.create_attr() 
        
        group = self.client.factory.create('hyd:ResourceTemplateGroup')
        group.name = 'Test Group'

        
        templates = self.client.factory.create('hyd:ResourceTemplateArray')

        template_1 = self.client.factory.create('hyd:ResourceTemplate')
        template_1.name = "Test template 1"

        template_2 = self.client.factory.create('hyd:ResourceTemplate')
        template_2.name = "Test template 2"

        items_1 = self.client.factory.create('hyd:ResourceTemplateItemArray')
        items_2 = self.client.factory.create('hyd:ResourceTemplateItemArray')
        
        item_1 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_1.attr_id = attr_1.id
        items_1.ResourceTemplateItem.append(item_1)

        item_2 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_2.attr_id = attr_2.id
        items_2.ResourceTemplateItem.append(item_2)
        
        templates.ResourceTemplate.append(template_1)
        templates.ResourceTemplate.append(template_2)

        template_1.resourcetemplateitems = items_1
        template_2.resourcetemplateitems = items_2
        
        group.resourcetemplates = templates

        new_group = self.client.service.add_resourcetemplategroup(group)
        
        assert new_group.name == group.name, "Names are not the same!"
        assert new_group.id is not None, "New Group has no ID!"
        assert new_group.id > 0, "New Group has incorrect ID!"

        assert len(new_group.resourcetemplates[0]) == 2, "Resource templates did not add correctly"
        assert len(new_group.resourcetemplates[0][0].resourcetemplateitems[0]) == 1, "Resource template items did not add correctly"
        
        #update the name of one of the templates
        new_group.resourcetemplates[0][0].name = "Test template 3"

        #add an item to one of the templates
        item_3 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_3.attr_id = attr_3.id
        new_group.resourcetemplates[0][0].resourcetemplateitems.ResourceTemplateItem.append(item_3)

        updated_group = self.client.service.add_resourcetemplategroup(new_group)

        assert updated_group.name == group.name, "Names are not the same!"

        assert updated_group.resourcetemplates[0][0].name == "Test template 3", "Resource templates did not add correctly"

        assert len(updated_group.resourcetemplates[0][0].resourcetemplateitems[0]) == 2, "Resource template items did not update correctly"

    def test_add_template(self):

        attr_1 = self.create_attr() 
        attr_2 = self.create_attr() 
        attr_3 = self.create_attr() 
        
        template = self.client.factory.create('hyd:ResourceTemplate')
        template.name = "Test template name"

        items = self.client.factory.create('hyd:ResourceTemplateItemArray')
        
        item_1 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_1.attr_id = attr_1.id
        items.ResourceTemplateItem.append(item_1)

        item_2 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_2.attr_id = attr_2.id
        items.ResourceTemplateItem.append(item_2)
        
        item_3 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_3.attr_id = attr_3.id
        items.ResourceTemplateItem.append(item_3)

        template.resourcetemplateitems = items
        
        new_template = self.client.service.add_resourcetemplate(template)
        
        assert new_template.name == template.name, "Names are not the same!"
        assert new_template.id is not None, "New template has no ID!"
        assert new_template.id > 0, "New template has incorrect ID!"

        assert len(new_template.resourcetemplateitems[0]) == 3, "Resource template items did not add correctly"

    def test_update_template(self):

        attr_1 = self.create_attr() 
        attr_2 = self.create_attr() 
        attr_3 = self.create_attr() 
        
        template = self.client.factory.create('hyd:ResourceTemplate')
        template.name = "Test template name"

        items = self.client.factory.create('hyd:ResourceTemplateItemArray')
        
        item_1 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_1.attr_id = attr_1.id
        items.ResourceTemplateItem.append(item_1)

        item_2 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_2.attr_id = attr_2.id
        items.ResourceTemplateItem.append(item_2)

        template.resourcetemplateitems = items
        
        new_template = self.client.service.add_resourcetemplate(template)
        
        assert new_template.name == template.name, "Names are not the same!"
        assert new_template.id is not None, "New template has no ID!"
        assert new_template.id > 0, "New template has incorrect ID!"

        assert len(new_template.resourcetemplateitems[0]) == 2, "Resource template items did not add correctly"
        
        new_template.name = "Updated template name"
        
        items = self.client.factory.create('hyd:ResourceTemplateItemArray')

        item_3 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_3.attr_id = attr_3.id
        items.ResourceTemplateItem.append(item_3)
        
        new_template.resourcetemplateitems = items

        updated_template = self.client.service.update_resourcetemplate(new_template)

        assert new_template.name == updated_template.name, "Names are not the same!"
        assert new_template.id == updated_template.id, "template ids to not match!"
        assert new_template.id > 0, "New template has incorrect ID!"

        assert len(updated_template.resourcetemplateitems[0]) == 3, "Resource template items did not update correctly"

    def test_add_item(self):

        attr_1 = self.create_attr() 
        attr_2 = self.create_attr() 
        attr_3 = self.create_attr() 
        
        template = self.client.factory.create('hyd:ResourceTemplate')
        template.name = "Test template name"

        items = self.client.factory.create('hyd:ResourceTemplateItemArray')
        
        item_1 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_1.attr_id = attr_1.id
        items.ResourceTemplateItem.append(item_1)

        item_2 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_2.attr_id = attr_2.id
        items.ResourceTemplateItem.append(item_2)

        template.resourcetemplateitems = items
        
        new_template = self.client.service.add_resourcetemplate(template)
        
        item_3 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_3.attr_id = attr_3.id
        item_3.template_id = new_template.id
        

        updated_template = self.client.service.add_resourcetemplateitem(item_3)

        assert len(updated_template.resourcetemplateitems[0]) == 3, "Resource template item did not add correctly"


    def test_delete_item(self):

        attr_1 = self.create_attr() 
        attr_2 = self.create_attr() 
        
        template = self.client.factory.create('hyd:ResourceTemplate')
        template.name = "Test template name"

        items = self.client.factory.create('hyd:ResourceTemplateItemArray')
        
        item_1 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_1.attr_id = attr_1.id
        items.ResourceTemplateItem.append(item_1)

        item_2 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_2.attr_id = attr_2.id
        items.ResourceTemplateItem.append(item_2)

        template.resourcetemplateitems = items
        
        new_template = self.client.service.add_resourcetemplate(template)
        
        item_2.template_id = new_template.id
        
        updated_template = self.client.service.delete_resourcetemplateitem(item_2)

        assert len(updated_template.resourcetemplateitems[0]) == 1, "Resource template item did not add correctly"



    def test_get_groups(self):
        groups = self.client.service.get_groups()
        assert len(groups) > 0, "Groups were not retrieved!"

def setup():
    test_SoapServer.connect()

if __name__ == '__main__':
    test_SoapServer.run()
