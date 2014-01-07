#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
import datetime

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
        
        group = self.client.factory.create('hyd:TemplateGroup')
        group.name = 'Test Group @ %s'%datetime.datetime.now()

        
        templates = self.client.factory.create('hyd:TemplateArray')
        #**********************
        #TEMPLATE 1           #
        #**********************
        template1 = self.client.factory.create('hyd:Template')
        template1.name = "Test template 1"
        template1.alias = "Test template alias"

        items = self.client.factory.create('hyd:TemplateItemArray')
        
        item_1 = self.client.factory.create('hyd:TemplateItem')
        item_1.attr_id = attr_1.id
        items.TemplateItem.append(item_1)

        item_2 = self.client.factory.create('hyd:TemplateItem')
        item_2.attr_id = attr_2.id
        items.TemplateItem.append(item_2)
        
        item_3 = self.client.factory.create('hyd:TemplateItem')
        item_3.attr_id = attr_3.id
        items.TemplateItem.append(item_3)

        template1.templateitems = items
        
        templates.Template.append(template1)
        #**********************
        #TEMPLATE 2           #
        #**********************
        template2 = self.client.factory.create('hyd:Template')
        template2.name = "Test template 2"

        items = self.client.factory.create('hyd:TemplateItemArray')
        
        item_1 = self.client.factory.create('hyd:TemplateItem')
        item_1.attr_id = attr_1.id
        items.TemplateItem.append(item_1)

        item_2 = self.client.factory.create('hyd:TemplateItem')
        item_2.attr_id = attr_2.id
        items.TemplateItem.append(item_2)
        
        item_4 = self.client.factory.create('hyd:TemplateItem')
        item_4.attr_id = attr_4.id
        items.TemplateItem.append(item_4)
        
        template2.templateitems = items

        templates.Template.append(template2)

        
        group.templates = templates

        new_group = self.client.service.add_templategroup(group)
        
        assert new_group.name == group.name, "Names are not the same!"
        assert new_group.id is not None, "New Group has no ID!"
        assert new_group.id > 0, "New Group has incorrect ID!"

        assert len(new_group.templates) == 1, "Resource templates did not add correctly"
        assert len(new_group.templates[0][0].templateitems[0]) == 3, "Resource template items did not add correctly"

        self.set_group(new_group)

    def test_update_group(self):


        attr_1 = self.create_attr("testattr_1") 
        attr_2 = self.create_attr("testattr_2") 
        attr_3 = self.create_attr("testattr_3") 
        
        group = self.client.factory.create('hyd:TemplateGroup')
        
        group.name = 'Test Group @ %s'%datetime.datetime.now()
        
        templates = self.client.factory.create('hyd:TemplateArray')

        template_1 = self.client.factory.create('hyd:Template')
        template_1.name = "Test template 1"
        template_1.name = "Test template 1 alias"

        template_2 = self.client.factory.create('hyd:Template')
        template_2.name = "Test template 2"
        template_2.name = "Test template 2 alias"

        items_1 = self.client.factory.create('hyd:TemplateItemArray')
        items_2 = self.client.factory.create('hyd:TemplateItemArray')
        
        item_1 = self.client.factory.create('hyd:TemplateItem')
        item_1.attr_id = attr_1.id
        items_1.TemplateItem.append(item_1)

        item_2 = self.client.factory.create('hyd:TemplateItem')
        item_2.attr_id = attr_2.id
        items_2.TemplateItem.append(item_2)
        
        templates.Template.append(template_1)
        templates.Template.append(template_2)

        template_1.templateitems = items_1
        template_2.templateitems = items_2
        
        group.templates = templates

        new_group = self.client.service.add_templategroup(group)
        
        assert new_group.name == group.name, "Names are not the same!"
        assert new_group.id is not None, "New Group has no ID!"
        assert new_group.id > 0, "New Group has incorrect ID!"

        assert len(new_group.templates[0]) == 2, "Resource templates did not add correctly"
        assert len(new_group.templates[0][0].templateitems[0]) == 1, "Resource template items did not add correctly"
        
        #update the name of one of the templates
        new_group.templates[0][0].name = "Test template 3"
        updated_template_id = new_group.templates[0][0].id

        #add an item to one of the templates
        item_3 = self.client.factory.create('hyd:TemplateItem')
        item_3.attr_id = attr_3.id
        new_group.templates[0][0].templateitems.TemplateItem.append(item_3)

        updated_group = self.client.service.update_templategroup(new_group)

        assert updated_group.name == group.name, "Names are not the same!"
        
        updated_template = None
        for tmpl in new_group.templates.Template:
            if tmpl.id == updated_template_id:
                updated_template = tmpl
                break

        assert updated_template.name == "Test template 3", "Resource templates did not update correctly"

        assert len(updated_template.templateitems[0]) == 2, "Resource template items did not update correctly"

    def test_add_template(self):

        attr_1 = self.create_attr("testattr_1") 
        attr_2 = self.create_attr("testattr_2") 
        attr_3 = self.create_attr("testattr_3") 
        
        template = self.client.factory.create('hyd:Template')
        template.name = "Test template name @ %s"%(datetime.datetime.now())
        template.alias = "%s alias" % template.name

        items = self.client.factory.create('hyd:TemplateItemArray')
        
        item_1 = self.client.factory.create('hyd:TemplateItem')
        item_1.attr_id = attr_1.id
        items.TemplateItem.append(item_1)

        item_2 = self.client.factory.create('hyd:TemplateItem')
        item_2.attr_id = attr_2.id
        items.TemplateItem.append(item_2)
        
        item_3 = self.client.factory.create('hyd:TemplateItem')
        item_3.attr_id = attr_3.id
        items.TemplateItem.append(item_3)

        template.templateitems = items

        new_template = self.client.service.add_template(template)
        
        assert new_template.name == template.name, "Names are not the same!"
        assert new_template.alias == template.alias, "Aliases are not the same!"
        assert new_template.id is not None, "New template has no ID!"
        assert new_template.id > 0, "New template has incorrect ID!"

        assert len(new_template.templateitems[0]) == 3, "Resource template items did not add correctly"
        
        return new_template

    def test_update_template(self):

        attr_1 = self.create_attr("testattr_1") 
        attr_2 = self.create_attr("testattr_2") 
        attr_3 = self.create_attr("testattr_3") 
        
        template = self.client.factory.create('hyd:Template')
        template.name = "Test template name @ %s" % (datetime.datetime.now())
        template.alias = template.name + " alias"
        template.group_id = self.get_group().id

        items = self.client.factory.create('hyd:TemplateItemArray')
        
        item_1 = self.client.factory.create('hyd:TemplateItem')
        item_1.attr_id = attr_1.id
        items.TemplateItem.append(item_1)

        item_2 = self.client.factory.create('hyd:TemplateItem')
        item_2.attr_id = attr_2.id
        items.TemplateItem.append(item_2)

        template.templateitems = items
        
        new_template = self.client.service.add_template(template)
        
        assert new_template.name == template.name, "Names are not the same!"
        assert new_template.alias == template.alias, "Aliases are not the same!"
        assert new_template.id is not None, "New template has no ID!"
        assert new_template.id > 0, "New template has incorrect ID!"

        assert len(new_template.templateitems[0]) == 2, "Resource template items did not add correctly"
        
        new_template.name = "Updated template name @ %s"%(datetime.datetime.now())
        new_template.alias = template.name + " alias"
        
        items = self.client.factory.create('hyd:TemplateItemArray')

        item_3 = self.client.factory.create('hyd:TemplateItem')
        item_3.attr_id = attr_3.id
        items.TemplateItem.append(item_3)
        
        new_template.templateitems = items

        updated_template = self.client.service.update_template(new_template)

        assert new_template.name == updated_template.name, "Names are not the same!"
        assert new_template.alias == updated_template.alias, "Aliases are not the same!"
        assert new_template.id == updated_template.id, "template ids to not match!"
        assert new_template.id > 0, "New template has incorrect ID!"

        assert len(updated_template.templateitems[0]) == 3, "Resource template items did not update correctly"

    def test_get_template(self):
        new_template = self.get_group().templates.Template[0]
        new_template = self.client.service.get_template(new_template.id)
        assert new_template is not None, "Resource template items not retrived by ID!"

    def test_get_template_by_name(self):
        new_template = self.get_group().templates.Template[0]
        new_template = self.client.service.get_template(new_template.group_id, new_template.name)
        assert new_template is not None, "Resource template items not retrived by name!"


    def test_add_item(self):


        attr_1 = self.create_attr("testattr_1") 
        attr_2 = self.create_attr("testattr_2") 
        attr_3 = self.create_attr("testattr_3") 
        
        template = self.client.factory.create('hyd:Template')
        template.name = "Test template name @ %s"%(datetime.datetime.now())
        template.alias = template.name + " alias"
        template.group_id = self.get_group().id

        items = self.client.factory.create('hyd:TemplateItemArray')
        
        item_1 = self.client.factory.create('hyd:TemplateItem')
        item_1.attr_id = attr_1.id
        items.TemplateItem.append(item_1)

        item_2 = self.client.factory.create('hyd:TemplateItem')
        item_2.attr_id = attr_2.id
        items.TemplateItem.append(item_2)

        template.templateitems = items
        
        new_template = self.client.service.add_template(template)
        
        item_3 = self.client.factory.create('hyd:TemplateItem')
        item_3.attr_id = attr_3.id
        item_3.template_id = new_template.id
        

        updated_template = self.client.service.add_templateitem(item_3)

        assert len(updated_template.templateitems[0]) == 3, "Resource template item did not add correctly"


    def test_delete_item(self):


        attr_1 = self.create_attr("testattr_1") 
        attr_2 = self.create_attr("testattr_2") 
        
        template = self.client.factory.create('hyd:Template')
        template.name = "Test template name @ %s"%(datetime.datetime.now())
        template.alias = template.name + " alias"

        items = self.client.factory.create('hyd:TemplateItemArray')
        
        item_1 = self.client.factory.create('hyd:TemplateItem')
        item_1.attr_id = attr_1.id
        items.TemplateItem.append(item_1)

        item_2 = self.client.factory.create('hyd:TemplateItem')
        item_2.attr_id = attr_2.id
        items.TemplateItem.append(item_2)

        template.templateitems = items
        
        new_template = self.client.service.add_template(template)
        
        item_2.template_id = new_template.id
        
        updated_template = self.client.service.delete_templateitem(item_2)

        assert len(updated_template.templateitems[0]) == 1, "Resource template item did not add correctly"



    def test_get_groups(self):
        groups = self.client.service.get_templategroups()
        assert len(groups) > 0, "Groups were not retrieved!"

    
    def test_get_group(self):
        group = self.get_group()
        new_group = self.client.service.get_templategroup(group.id)

        assert new_group.name == group.name, "Names are not the same! Retrieval by ID did not work!"



    def test_get_group_by_name(self):
        group = self.get_group()
        new_group = self.client.service.get_templategroup_by_name(group.name)

        assert new_group.name == group.name, "Names are not the same! Retrieval by name did not work!"

    def test_add_resource_template(self):

        group = self.get_group()
        templates = group.templates.Template
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

            grp_summary = self.client.factory.create('hyd:GroupSummary')
            grp_summary.id = group.id
            grp_summary.name = group.name
            
            tmpl_summary = self.client.factory.create('hyd:TemplateSummary')
            tmpl_summary.id = template_id
            tmpl_summary.name = template_name
            
            grp_summary.templates.TemplateSummary.append(tmpl_summary)
            
            node.templates.GroupSummary.append(grp_summary)

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
            assert node.templates is not None and node.templates.GroupSummary[0].templates.TemplateSummary[0].name == "Test template 1"; "Template was not added correctly!"

    def test_update_resource_template(self):

        group = self.get_group()
        templates = group.templates.Template
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

            grp_summary = self.client.factory.create('hyd:GroupSummary')
            grp_summary.id = group.id
            grp_summary.name = group.name
            
            tmpl_summary = self.client.factory.create('hyd:TemplateSummary')
            tmpl_summary.id = template_id
            tmpl_summary.name = template_name
            
            grp_summary.templates.TemplateSummary.append(tmpl_summary)
            
            node.templates.GroupSummary.append(grp_summary)

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

        grp_summary = self.client.factory.create('hyd:GroupSummary')
        grp_summary.id = group.id
        grp_summary.name = group.name
        
        tmpl_summary = self.client.factory.create('hyd:TemplateSummary')
        tmpl_summary.id   = new_template_id
        tmpl_summary.name = template_name
        
        grp_summary.templates.TemplateSummary.append(tmpl_summary)
        
        node.templates.GroupSummary.append(grp_summary)
        
        del updated_node.templates.GroupSummary[0]
        updated_node.templates.GroupSummary.append(grp_summary)

        new_network = self.client.service.update_network(network)

        for node in new_network.nodes.Node:
            if node.id == updated_node.id:
                assert node.templates is not None and node.templates.GroupSummary[0].templates.TemplateSummary[0].name == "Test template 2"; "Template was not added correctly!"

    def test_find_matching_resource_templates(self):

        group       = self.get_group()
        templates   = group.templates.Template
        template_id = templates[0].id

        network = self.create_network_with_data()

        node_to_check = network.nodes.Node[0]
        matching_templates = self.client.service.get_matching_resource_templates('NODE', node_to_check.id)

        assert len(matching_templates) > 0; "No templates returned!"
        
        matching_template_ids = []
        for grp in matching_templates.GroupSummary:
            for tmpl in grp.templates.TemplateSummary:
                matching_template_ids.append(tmpl.id)

        assert template_id in matching_template_ids; "Template ID not found in matching templates!"

def setup():
    test_SoapServer.connect()

if __name__ == '__main__':
    test_SoapServer.run()
