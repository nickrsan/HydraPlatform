
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
import datetime
import copy

class TemplatesTest(test_SoapServer.SoapServerTest):

    def set_group(self, group):
        self.group = group

    def get_group(self):
        if hasattr(self, 'group'):
            return self.group
        else:
            self.test_add_group()
        return self.group

    def test_add_xml(self):
        template_file = open('template.xml', 'r')

        file_contents = template_file.read()

        new_grp = self.client.service.upload_template_xml(file_contents)

        assert new_grp is not None, "Adding template from XML was not successful!"

    def test_add_group(self):

        attr_1 = self.create_attr("testattr_1") 
        attr_2 = self.create_attr("testattr_2") 
        attr_3 = self.create_attr("testattr_3") 
        attr_4 = self.create_attr("testattr_3") 
        
        group = self.client.factory.create('hyd:ResourceTemplateGroup')
        group.name = 'Test Group @ %s'%datetime.datetime.now()

        
        templates = self.client.factory.create('hyd:ResourceTemplateArray')
        #**********************
        #TEMPLATE 1           #
        #**********************
        template1 = self.client.factory.create('hyd:ResourceTemplate')
        template1.name = "Test template 1"

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

        template1.resourcetemplateitems = items
        
        templates.ResourceTemplate.append(template1)
        #**********************
        #TEMPLATE 2           #
        #**********************
        template2 = self.client.factory.create('hyd:ResourceTemplate')
        template2.name = "Test template 2"

        items = self.client.factory.create('hyd:ResourceTemplateItemArray')
        
        item_1 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_1.attr_id = attr_1.id
        items.ResourceTemplateItem.append(item_1)

        item_2 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_2.attr_id = attr_2.id
        items.ResourceTemplateItem.append(item_2)
        
        item_4 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_4.attr_id = attr_4.id
        items.ResourceTemplateItem.append(item_4)
        
        template2.resourcetemplateitems = items

        templates.ResourceTemplate.append(template2)

        
        group.resourcetemplates = templates

        new_group = self.client.service.add_resourcetemplategroup(group)
        
        assert new_group.name == group.name, "Names are not the same!"
        assert new_group.id is not None, "New Group has no ID!"
        assert new_group.id > 0, "New Group has incorrect ID!"

        assert len(new_group.resourcetemplates) == 1, "Resource templates did not add correctly"
        assert len(new_group.resourcetemplates[0][0].resourcetemplateitems[0]) == 3, "Resource template items did not add correctly"

        self.set_group(new_group)

    def test_update_group(self):


        attr_1 = self.create_attr("testattr_1") 
        attr_2 = self.create_attr("testattr_2") 
        attr_3 = self.create_attr("testattr_3") 
        
        group = self.client.factory.create('hyd:ResourceTemplateGroup')
        
        group.name = 'Test Group @ %s'%datetime.datetime.now()
        
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
        updated_template_id = new_group.resourcetemplates[0][0].id

        #add an item to one of the templates
        item_3 = self.client.factory.create('hyd:ResourceTemplateItem')
        item_3.attr_id = attr_3.id
        new_group.resourcetemplates[0][0].resourcetemplateitems.ResourceTemplateItem.append(item_3)

        updated_group = self.client.service.update_resourcetemplategroup(new_group)

        assert updated_group.name == group.name, "Names are not the same!"
        
        updated_template = None
        for tmpl in new_group.resourcetemplates.ResourceTemplate:
            if tmpl.id == updated_template_id:
                updated_template = tmpl
                break

        assert updated_template.name == "Test template 3", "Resource templates did not update correctly"

        assert len(updated_template.resourcetemplateitems[0]) == 2, "Resource template items did not update correctly"

    def test_add_template(self):

        attr_1 = self.create_attr("testattr_1") 
        attr_2 = self.create_attr("testattr_2") 
        attr_3 = self.create_attr("testattr_3") 
        
        template = self.client.factory.create('hyd:ResourceTemplate')
        template.name = "Test template name @ %s"%(datetime.datetime.now())

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
        
        return new_template

    def test_update_template(self):

        attr_1 = self.create_attr("testattr_1") 
        attr_2 = self.create_attr("testattr_2") 
        attr_3 = self.create_attr("testattr_3") 
        
        template = self.client.factory.create('hyd:ResourceTemplate')
        template.name = "Test template name @ %s"%(datetime.datetime.now())
        template.group_id = self.get_group().id

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
        
        new_template.name = "Updated template name @ %s"%(datetime.datetime.now())
        
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

    def test_get_template(self):
        new_template = self.get_group().resourcetemplates.ResourceTemplate[0]
        new_template = self.client.service.get_resourcetemplate(new_template.id)
        assert new_template is not None, "Resource template items not retrived by ID!"

    def test_get_template_by_name(self):
        new_template = self.get_group().resourcetemplates.ResourceTemplate[0]
        new_template = self.client.service.get_resourcetemplate(new_template.group_id, new_template.name)
        assert new_template is not None, "Resource template items not retrived by name!"


    def test_add_item(self):


        attr_1 = self.create_attr("testattr_1") 
        attr_2 = self.create_attr("testattr_2") 
        attr_3 = self.create_attr("testattr_3") 
        
        template = self.client.factory.create('hyd:ResourceTemplate')
        template.name = "Test template name @ %s"%(datetime.datetime.now())
        template.group_id = self.get_group().id

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


        attr_1 = self.create_attr("testattr_1") 
        attr_2 = self.create_attr("testattr_2") 
        
        template = self.client.factory.create('hyd:ResourceTemplate')
        template.name = "Test template name @ %s"%(datetime.datetime.now())

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
        groups = self.client.service.get_resourcetemplategroups()
        assert len(groups) > 0, "Groups were not retrieved!"

    
    def test_get_group(self):
        group = self.get_group()
        new_group = self.client.service.get_resourcetemplategroup(group.id)

        assert new_group.name == group.name, "Names are not the same! Retrieval by ID did not work!"



    def test_get_group_by_name(self):
        group = self.get_group()
        new_group = self.client.service.get_resourcetemplategroup_by_name(group.name)

        assert new_group.name == group.name, "Names are not the same! Retrieval by name did not work!"

    def test_add_resource_template(self):

        group = self.get_group()
        templates = group.resourcetemplates.ResourceTemplate
        template_name = templates[0].name
        template_id   = templates[0].id

        project = self.create_project('test')
        network = self.client.factory.create('hyd:Network')
        nodes = self.client.factory.create('hyd:NodeArray')
        links = self.client.factory.create('hyd:LinkArray')

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = self.client.factory.create('hyd:Node')
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            grp_summary = self.client.factory.create('hyd:ResourceGroupSummary')
            grp_summary.id = group.id
            grp_summary.name = group.name
            
            tmpl_summary = self.client.factory.create('hyd:ResourceTemplateSummary')
            tmpl_summary.id = template_id
            tmpl_summary.name = template_name
            
            grp_summary.templates.ResourceTemplateSummary.append(tmpl_summary)
            
            node.templates.ResourceGroupSummary.append(grp_summary)

            nodes.Node.append(node)

        for i in range(nlinks):
            link = self.client.factory.create('hyd:Link')
            link.id = i * -1
            link.name = 'Link ' + str(i)
            link.description = 'Test link ' + str(i)
            link.node_1_id = nodes.Node[i].id
            link.node_2_id = nodes.Node[i + 1].id

            links.Link.append(link)

        network.project_id = project.id
        network.name = 'Test'
        network.description = 'A network for SOAP unit tests.'
        network.nodes = nodes
        network.links = links

        network = self.client.service.add_network(network)

        for node in network.nodes.Node:
            assert node.templates is not None and node.templates.ResourceGroupSummary[0].templates.ResourceTemplateSummary[0].name == "Test template 1"; "Template was not added correctly!"

    def test_update_resource_template(self):

        group = self.get_group()
        templates = group.resourcetemplates.ResourceTemplate
        template_name = templates[0].name
        template_id   = templates[0].id

        project = self.create_project('test')
        network = self.client.factory.create('hyd:Network')
        nodes = self.client.factory.create('hyd:NodeArray')
        links = self.client.factory.create('hyd:LinkArray')

        nnodes = 3
        nlinks = 2
        x = [0, 0, 1]
        y = [0, 1, 0]

        for i in range(nnodes):
            node = self.client.factory.create('hyd:Node')
            node.id = i * -1
            node.name = 'Node ' + str(i)
            node.description = 'Test node ' + str(i)
            node.x = x[i]
            node.y = y[i]

            grp_summary = self.client.factory.create('hyd:ResourceGroupSummary')
            grp_summary.id = group.id
            grp_summary.name = group.name
            
            tmpl_summary = self.client.factory.create('hyd:ResourceTemplateSummary')
            tmpl_summary.id = template_id
            tmpl_summary.name = template_name
            
            grp_summary.templates.ResourceTemplateSummary.append(tmpl_summary)
            
            node.templates.ResourceGroupSummary.append(grp_summary)

            nodes.Node.append(node)

        for i in range(nlinks):
            link = self.client.factory.create('hyd:Link')
            link.id = i * -1
            link.name = 'Link ' + str(i)
            link.description = 'Test link ' + str(i)
            link.node_1_id = nodes.Node[i].id
            link.node_2_id = nodes.Node[i + 1].id

            links.Link.append(link)

        network.project_id = project.id
        network.name = 'Test'
        network.description = 'A network for SOAP unit tests.'
        network.nodes = nodes
        network.links = links

        network = self.client.service.add_network(network)

        new_template_id   = templates[1].id

        updated_node = network.nodes.Node[0]

        grp_summary = self.client.factory.create('hyd:ResourceGroupSummary')
        grp_summary.id = group.id
        grp_summary.name = group.name
        
        tmpl_summary = self.client.factory.create('hyd:ResourceTemplateSummary')
        tmpl_summary.id   = new_template_id
        tmpl_summary.name = template_name
        
        grp_summary.templates.ResourceTemplateSummary.append(tmpl_summary)
        
        node.templates.ResourceGroupSummary.append(grp_summary)
        
        del updated_node.templates.ResourceGroupSummary[0]
        updated_node.templates.ResourceGroupSummary.append(grp_summary)

        new_network = self.client.service.update_network(network)

        for node in new_network.nodes.Node:
            if node.id == updated_node.id:
                assert node.templates is not None and node.templates.ResourceGroupSummary[0].templates.ResourceTemplateSummary[0].name == "Test template 2"; "Template was not added correctly!"

    def test_find_matching_resource_templates(self):

        group       = self.get_group()
        templates   = group.resourcetemplates.ResourceTemplate
        template_id = templates[0].id

        network = self.create_network_with_data()

        node_to_check = network.nodes.Node[0]
        matching_templates = self.client.service.get_matching_resource_templates('NODE', node_to_check.id)

        assert len(matching_templates) > 0; "No templates returned!"
        
        matching_template_ids = []
        for grp in matching_templates.ResourceGroupSummary:
            for tmpl in grp.templates.ResourceTemplateSummary:
                matching_template_ids.append(tmpl.id)

        assert template_id in matching_template_ids; "Template ID not found in matching templates!"

def setup():
    test_SoapServer.connect()

if __name__ == '__main__':
    test_SoapServer.run()
