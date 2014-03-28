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
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
import datetime
from lxml import etree
from HydraLib import config

class TemplatesTest(test_SoapServer.SoapServerTest):

    def set_template(self, template):
        self.template = template

    def get_template(self):
        if hasattr(self, 'template'):
            return self.template
        else:
            self.test_add_template()
        return self.template

    def test_add_xml(self):
        template_file = open('template.xml', 'r')

        file_contents = template_file.read()

        new_tmpl = self.client.service.upload_template_xml(file_contents)

        assert new_tmpl is not None, "Adding template from XML was not successful!"

    def test_add_template(self):

        link_attr_1 = self.create_attr("link_attr_1", dimension='Pressure')
        link_attr_2 = self.create_attr("link_attr_2", dimension='Speed')
        node_attr_1 = self.create_attr("node_attr_1", dimension='Volume')
        node_attr_2 = self.create_attr("node_attr_2", dimension='Speed')

        template = self.client.factory.create('hyd:Template')
        template.name = 'Test template @ %s'%datetime.datetime.now()


        types = self.client.factory.create('hyd:TemplateTypeArray')
        #**********************
        #type 1           #
        #**********************
        type1 = self.client.factory.create('hyd:TemplateType')
        type1.name = "Test type 1"
        type1.alias = "Test type alias"

        tattrs = self.client.factory.create('hyd:TypeAttrArray')

        tattr_1 = self.client.factory.create('hyd:TypeAttr')
        tattr_1.attr_id = node_attr_1.id
        tattrs.TypeAttr.append(tattr_1)

        tattr_2 = self.client.factory.create('hyd:TypeAttr')
        tattr_2.attr_id = node_attr_2.id
        tattrs.TypeAttr.append(tattr_2)

        type1.typeattrs = tattrs

        types.TemplateType.append(type1)
        #**********************
        #type 2           #
        #**********************
        type2 = self.client.factory.create('hyd:TemplateType')
        type2.name = "Test type 2"

        tattrs = self.client.factory.create('hyd:TypeAttrArray')

        tattr_1 = self.client.factory.create('hyd:TypeAttr')
        tattr_1.attr_id = link_attr_1.id
        tattrs.TypeAttr.append(tattr_1)

        tattr_2 = self.client.factory.create('hyd:TypeAttr')
        tattr_2.attr_id = link_attr_2.id
        tattrs.TypeAttr.append(tattr_2)

        type2.typeattrs = tattrs

        types.TemplateType.append(type2)


        template.types = types

        new_template = self.client.service.add_template(template)

        assert new_template.name == template.name, "Names are not the same!"
        assert new_template.id is not None, "New Template has no ID!"
        assert new_template.id > 0, "New Template has incorrect ID!"

        assert len(new_template.types) == 1, "Resource types did not add correctly"
        for t in new_template.types.TemplateType[0].typeattrs.TypeAttr:
            assert t.attr_id in (node_attr_1.id, node_attr_2.id);
            "Node types were not added correctly!"

        for t in new_template.types.TemplateType[1].typeattrs.TypeAttr:
            assert t.attr_id in (link_attr_1.id, link_attr_2.id);
            "Node types were not added correctly!"

        self.set_template(new_template)

    def test_update_template(self):


        attr_1 = self.create_attr("link_attr_1", dimension='Pressure')
        attr_2 = self.create_attr("link_attr_2", dimension='Speed')
        attr_3 = self.create_attr("node_attr_1", dimension='Volume')

        template = self.client.factory.create('hyd:Template')

        template.name = 'Test Template @ %s'%datetime.datetime.now()

        types = self.client.factory.create('hyd:TemplateTypeArray')

        type_1 = self.client.factory.create('hyd:TemplateType')
        type_1.name = "Test type 1"
        type_1.name = "Test type 1 alias"

        type_2 = self.client.factory.create('hyd:TemplateType')
        type_2.name = "Test type 2"
        type_2.name = "Test type 2 alias"

        tattrs_1 = self.client.factory.create('hyd:TypeAttrArray')
        tattrs_2 = self.client.factory.create('hyd:TypeAttrArray')

        tattr_1 = self.client.factory.create('hyd:TypeAttr')
        tattr_1.attr_id = attr_1.id
        tattrs_1.TypeAttr.append(tattr_1)

        tattr_2 = self.client.factory.create('hyd:TypeAttr')
        tattr_2.attr_id = attr_2.id
        tattrs_2.TypeAttr.append(tattr_2)

        types.TemplateType.append(type_1)
        types.TemplateType.append(type_2)

        type_1.typeattrs = tattrs_1
        type_2.typeattrs = tattrs_2

        template.types = types

        new_template = self.client.service.add_template(template)

        assert new_template.name == template.name, "Names are not the same!"
        assert new_template.id is not None, "New Template has no ID!"
        assert new_template.id > 0, "New Template has incorrect ID!"

        assert len(new_template.types[0]) == 2, "Resource types did not add correctly"
        assert len(new_template.types[0][0].typeattrs[0]) == 1, "Resource type attrs did not add correctly"

        #update the name of one of the types
        new_template.types[0][0].name = "Test type 3"
        updated_type_id = new_template.types[0][0].id

        #add an template attr to one of the types
        tattr_3 = self.client.factory.create('hyd:TypeAttr')
        tattr_3.attr_id = attr_3.id
        new_template.types[0][0].typeattrs.TypeAttr.append(tattr_3)

        updated_template = self.client.service.update_template(new_template)

        assert updated_template.name == template.name, "Names are not the same!"

        updated_type = None
        for tmpltype in new_template.types.TemplateType:
            if tmpltype.id == updated_type_id:
                updated_type = tmpltype
                break

        assert updated_type.name == "Test type 3", "Resource types did not update correctly"

        assert len(updated_type.typeattrs[0]) == 2, "Resource type template attr did not update correctly"

    def test_add_type(self):

        attr_1 = self.create_attr("link_attr_1", dimension='Pressure')
        attr_2 = self.create_attr("link_attr_2", dimension='Speed')
        attr_3 = self.create_attr("node_attr_1", dimension='Volume')

        templatetype = self.client.factory.create('hyd:TemplateType')
        templatetype.name = "Test type name @ %s"%(datetime.datetime.now())
        templatetype.alias = "%s alias" % templatetype.name
        layout = self.client.factory.create("xs:anyType")
        layout.color = 'red'
        layout.shapefile = 'blah.shp'

        tattrs = self.client.factory.create('hyd:TypeAttrArray')

        tattr_1 = self.client.factory.create('hyd:TypeAttr')
        tattr_1.attr_id = attr_1.id
        tattrs.TypeAttr.append(tattr_1)

        tattr_2 = self.client.factory.create('hyd:TypeAttr')
        tattr_2.attr_id = attr_2.id
        tattrs.TypeAttr.append(tattr_2)

        tattr_3 = self.client.factory.create('hyd:TypeAttr')
        tattr_3.attr_id = attr_3.id
        tattrs.TypeAttr.append(tattr_3)

        templatetype.typeattrs = tattrs

        new_type = self.client.service.add_templatetype(templatetype)

        assert new_type.name == templatetype.name, "Names are not the same!"
        assert new_type.alias == templatetype.alias, "Aliases are not the same!"
        assert repr(new_type.layout) == repr(templatetype.layout), "Layouts are not the same!"
        assert new_type.id is not None, "New type has no ID!"
        assert new_type.id > 0, "New type has incorrect ID!"

        assert len(new_type.typeattrs[0]) == 3, "Resource type attrs did not add correctly"

        return new_type 

    def test_update_type(self):

        attr_1 = self.create_attr("link_attr_1", dimension='Pressure')
        attr_2 = self.create_attr("link_attr_2", dimension='Speed')
        attr_3 = self.create_attr("node_attr_1", dimension='Volume')

        templatetype = self.client.factory.create('hyd:TemplateType')
        templatetype.name = "Test type name @ %s" % (datetime.datetime.now())
        templatetype.alias = templatetype.name + " alias"
        templatetype.template_id = self.get_template().id

        tattrs = self.client.factory.create('hyd:TypeAttrArray')

        tattr_1 = self.client.factory.create('hyd:TypeAttr')
        tattr_1.attr_id = attr_1.id
        tattrs.TypeAttr.append(tattr_1)

        tattr_2 = self.client.factory.create('hyd:TypeAttr')
        tattr_2.attr_id = attr_2.id
        tattrs.TypeAttr.append(tattr_2)

        templatetype.typeattrs = tattrs

        new_type = self.client.service.add_templatetype(templatetype)

        assert new_type.name == templatetype.name, "Names are not the same!"
        assert new_type.alias == templatetype.alias, "Aliases are not the same!"
        assert new_type.id is not templatetype, "New type has no ID!"
        assert new_type.id > 0, "New type has incorrect ID!"

        assert len(new_type.typeattrs[0]) == 2, "Resource type attrs did not add correctly"

        new_type.name = "Updated type name @ %s"%(datetime.datetime.now())
        new_type.alias = templatetype.name + " alias"

        tattrs = self.client.factory.create('hyd:TypeAttrArray')

        tattr_3 = self.client.factory.create('hyd:TypeAttr')
        tattr_3.attr_id = attr_3.id
        tattrs.TypeAttr.append(tattr_3)

        new_type.typeattrs = tattrs

        updated_type = self.client.service.update_templatetype(new_type)

        assert new_type.name == updated_type.name, "Names are not the same!"
        assert new_type.alias == updated_type.alias, "Aliases are not the same!"
        assert new_type.id == updated_type.id, "type ids to not match!"
        assert new_type.id > 0, "New type has incorrect ID!"

        assert len(updated_type.typeattrs[0]) == 3, "Resource type attrs did not update correctly"

    def test_get_type(self):
        new_type = self.get_template().types.TemplateType[0]
        new_type = self.client.service.get_templatetype(new_type.id)
        assert new_type is not None, "Resource type attrs not retrived by ID!"

    def test_get_type_by_name(self):
        new_type = self.get_template().types.TemplateType[0]
        new_type = self.client.service.get_templatetype_by_name(new_type.template_id, new_type.name)
        assert new_type is not None, "Resource type attrs not retrived by name!"


    def test_add_typeattr(self):


        attr_1 = self.create_attr("link_attr_1", dimension='Pressure')
        attr_2 = self.create_attr("link_attr_2", dimension='Speed')
        attr_3 = self.create_attr("node_attr_1", dimension='Volume')

        templatetype = self.client.factory.create('hyd:TemplateType')
        templatetype.name = "Test type name @ %s"%(datetime.datetime.now())
        templatetype.alias = templatetype.name + " alias"
        templatetype.template_id = self.get_template().id

        tattrs = self.client.factory.create('hyd:TypeAttrArray')

        tattr_1 = self.client.factory.create('hyd:TypeAttr')
        tattr_1.attr_id = attr_1.id
        tattrs.TypeAttr.append(tattr_1)

        tattr_2 = self.client.factory.create('hyd:TypeAttr')
        tattr_2.attr_id = attr_2.id
        tattrs.TypeAttr.append(tattr_2)

        templatetype.typeattrs = tattrs

        new_type = self.client.service.add_templatetype(templatetype)

        tattr_3 = self.client.factory.create('hyd:TypeAttr')
        tattr_3.attr_id = attr_3.id
        tattr_3.type_id = new_type.id


        updated_type = self.client.service.add_typeattr(tattr_3)

        assert len(updated_type.typeattrs[0]) == 3, "Resource type attr did not add correctly"


    def test_delete_typeattr(self):


        attr_1 = self.create_attr("link_attr_1", dimension='Pressure')
        attr_2 = self.create_attr("link_attr_2", dimension='Speed')

        templatetype = self.client.factory.create('hyd:TemplateType')
        templatetype.name = "Test type name @ %s"%(datetime.datetime.now())
        templatetype.alias = templatetype.name + " alias"

        tattrs = self.client.factory.create('hyd:TypeAttrArray')

        tattr_1 = self.client.factory.create('hyd:TypeAttr')
        tattr_1.attr_id = attr_1.id
        tattrs.TypeAttr.append(tattr_1)

        tattr_2 = self.client.factory.create('hyd:TypeAttr')
        tattr_2.attr_id = attr_2.id
        tattrs.TypeAttr.append(tattr_2)

        templatetype.typeattrs = tattrs

        new_type = self.client.service.add_templatetype(templatetype)

        tattr_2.type_id = new_type.id

        updated_type = self.client.service.delete_typeattr(tattr_2)

        assert len(updated_type.typeattrs[0]) == 1, "Resource type attr did not add correctly"



    def test_get_templates(self):
        templates = self.client.service.get_templates()
        assert len(templates) > 0, "Templates were not retrieved!"


    def test_get_template(self):
        template = self.get_template()
        new_template = self.client.service.get_template(template.id)

        assert new_template.name == template.name, "Names are not the same! Retrieval by ID did not work!"



    def test_get_template_by_name_good(self):
        template = self.get_template()
        new_template = self.client.service.get_template_by_name(template.name)

        assert new_template.name == template.name, "Names are not the same! Retrieval by name did not work!"

    def test_get_template_by_name_bad(self):
        new_template = self.client.service.get_template_by_name("Not a template!")

        assert new_template is None

    def test_add_resource_type(self):

        template = self.get_template()
        types = template.types.TemplateType
        type_name = types[0].name
        type_id   = types[0].id

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

            type_summary = self.client.factory.create('hyd:TypeSummary')
            type_summary.template_id = template.id
            type_summary.template_name = template.name
            type_summary.id = type_id
            type_summary.name = type_name

            node.types.TypeSummary.append(type_summary)

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
            assert node.types is not None and node.types.TypeSummary[0].name == "Test type 1"; "type was not added correctly!"


    def test_find_matching_resource_types(self):

        template = self.get_template()
        types    = template.types.TemplateType
        type_id  = types[0].id

        network = self.create_network_with_data()

        node_to_check = network.nodes.Node[0]
        matching_types = self.client.service.get_matching_resource_types('NODE', node_to_check.id)

        assert len(matching_types) > 0, "No types returned!"

        matching_type_ids = []
        for tmpltype in matching_types.TypeSummary:
            matching_type_ids.append(tmpltype.id)

        assert type_id in matching_type_ids, "TemplateType ID not found in matching types!"

    def test_assign_type_to_resource(self):
        network = self.create_network_with_data()
        template = self.get_template()
        templatetype = template.types.TemplateType[0]

        node_to_assign = network.nodes.Node[0]

        result = self.client.service.assign_type_to_resource(templatetype.id,
                                                             'NODE',
                                                             node_to_assign.id)

        node = self.client.service.get_node(node_to_assign.id)


        assert node.types is not None, \
            'Assigning type did not work properly.'

        assert str(result) in [str(x) for x in node.types.TypeSummary]

    def test_remove_type_from_resource(self):
        network = self.create_network_with_data()
        template = self.get_template()
        templatetype = template.types.TemplateType[0]

        node_to_assign = network.nodes.Node[0]

        result = self.client.service.assign_type_to_resource(templatetype.id,
                                                             'NODE',
                                                             node_to_assign.id)

        
        node = self.client.service.get_node(node_to_assign.id)


        assert node.types is not None, \
            'Assigning type did not work properly.'

        assert str(result) in [str(x) for x in node.types.TypeSummary]

       
        result = self.client.service.remove_type_from_resource(templatetype.id,
                                                            'NODE',
                                                            node_to_assign.id) 
        assert result == 'OK'
        
        node = self.client.service.get_node(node_to_assign.id)
 
        assert node.types.TypeSummary is None or str(result) not in [str(x) for x in node.types.TypeSummary] 

    def test_create_template_from_network(self):
        network = self.create_network_with_data()

        net_template = self.client.service.get_network_as_xml_template(network.id)

        assert net_template is not None

        template_xsd_path = config.get('templates', 'template_xsd_path')
        xmlschema_doc = etree.parse(template_xsd_path)

        xmlschema = etree.XMLSchema(xmlschema_doc)

        xml_tree = etree.fromstring(net_template)

        xmlschema.assertValid(xml_tree)


if __name__ == '__main__':
    test_SoapServer.run()
