
from spyne.model.complex import Array as SpyneArray
from spyne.model.primitive import String, Integer, Unicode
from spyne.decorator import rpc
from hydra_complexmodels import Template,\
TemplateGroup,\
TemplateItem,\
GroupSummary,\
TemplateSummary,\
get_as_complexmodel

from db import HydraIface
from hydra_base import HydraService
from HydraLib.HydraException import HydraError
import logging
from HydraLib import config
from lxml import etree
import attributes
from decimal import Decimal
from xml.sax.saxutils import unescape

def parse_attribute(attribute):

    dimension = attribute.find('dimension').text
    name      = attribute.find('name').text

    attr_cm = attributes.get_attribute_by_name(name)

    if attr_cm is None:
        attr_i         = HydraIface.Attr()
        attr_i.db.attr_dimen = attribute.find('dimension').text
        attr_i.db.attr_name  = attribute.find('name').text
        attr_i.save()
    else:
        attr_i = HydraIface.Attr(attr_id = attr_cm.id)
        if attr_i.db.attr_dimen != dimension:
            raise HydraError(
                "An attribute with name "
                "%s already exists but with a different"
                " dimension (%s). Please rename your attribute." %
                (name, attr_i.db.attr_dimen))

    return attr_i

def parse_templateitem(template_id, attribute):

    attr_i = parse_attribute(attribute)

    dimension = attribute.find('dimension').text

    templateitem_i = HydraIface.TemplateItem()
    templateitem_i.db.attr_id = attr_i.db.attr_id
    templateitem_i.db.template_id = template_id

    templateitem_i.load()

    if attribute.find('is_var') is not None:
        templateitem_i.db.attr_is_var = attribute.find('is_var').text

    if attribute.find('default') is not None:
        default = attribute.find('default')
        unit = default.find('unit').text
        val  = default.find('value').text
        try:
            val = Decimal(val)
            data_type = 'scalar'
        except:
            data_type = 'descriptor'

        dataset_id = HydraIface.add_dataset(data_type,
                               val,
                               unit,
                               dimension,
                               name="%s Default"%attr_i.db.attr_name,)
        templateitem_i.db.default_dataset_id = dataset_id

    templateitem_i.save()

    return templateitem_i

class TemplateService(HydraService):
    """
        The template SOAP service
    """

    @rpc(Unicode, _returns=TemplateGroup)
    def upload_template_xml(ctx, template_xml):
        """
            Add the group, template and items described
            in an XML file.

            Delete template, templateitem entries in the DB that are not in the XML file
            The assumption is that they have been deleted and are no longer required.
        """

        template_xsd_path = config.get('templates', 'template_xsd_path')
        xmlschema_doc = etree.parse(template_xsd_path)

        xmlschema = etree.XMLSchema(xmlschema_doc)

        xml_tree = etree.fromstring(template_xml)

        xmlschema.assertValid(xml_tree)

        group_name = xml_tree.find('template_name').text

        sql = """
            select
                grp.group_id,
                grp.group_name,
                tmpl.template_name,
                tmpl.template_id,
                tmpl.alias,
                tmpl.layout,
                attr.attr_name,
                attr.attr_id
            from
                tTemplateGroup grp,
                tTemplate tmpl,
                tTemplateItem item,
                tAttr attr
            where
                grp.group_name = '%s'
            and tmpl.group_id  = grp.group_id
            and item.template_id = tmpl.template_id
            and attr.attr_id     = item.attr_id
        """ % group_name

        rs = HydraIface.execute(sql)

        if len(rs) > 0:
            grp_i = HydraIface.TemplateGroup(group_id = rs[0].group_id)
            grp_i.load_all()
        else:
            grp_i = HydraIface.TemplateGroup()
            grp_i.db.group_name = group_name
            grp_i.save()

        templates = xml_tree.find('resources')

        #Delete any templates which are in the DB but no longer in the XML file
        template_name_map = {r.template_name:r.template_id for r in rs}

        existing_templates = set([r.template_name for r in rs])

        new_templates = set([r.find('name').text for r in templates.findall('resource')])

        templates_to_delete = existing_templates - new_templates

        for template_to_delete in templates_to_delete:
            template_id = template_name_map[template_to_delete]
            template_i = HydraIface.Template(template_id=template_id)
            template_i.load_all()
            template_i.delete()
            template_i.save()

        #Add or update templates.
        for resource in templates.findall('resource'):
            template_name = resource.find('name').text
            #check if the template is already in the DB. If not, create a new one.
            for r in rs:
                if r.template_name == template_name:
                    template_i = HydraIface.Template(template_id=r.template_id)
                    break
            else:
                template_i = HydraIface.Template()
                template_i.db.group_id = grp_i.db.group_id
                template_i.db.template_name = resource.find('name').text

                if resource.find('alias') is not None:
                    template_i.db.alias = resource.find('alias').text

                if resource.find('layout') is not None and \
                   resource.find('layout').text is not None:
                    layout = unescape(resource.find('layout').text)
                    layout_tree = HydraIface.validate_layout(layout)
                    layout_string = convert_layout_xml_to_dict(layout_tree)
                    template_i.db.layout = str(layout_string)

                template_i.save()

            #delete any TemplateItems which are in the DB but not in the XML file
            existing_attrs = []
            for r in rs:
                if r.template_name == template_name:
                    existing_attrs.append(r.attr_name)

            existing_attrs = set(existing_attrs)

            attr_name_map = {r.attr_name:(r.attr_id,r.template_id) for r in rs}


            new_attrs = set([r.find('name').text for r in resource.findall('attribute')])

            attrs_to_delete = existing_attrs - new_attrs

            for attr_to_delete in attrs_to_delete:
                attr_id, template_id = attr_name_map[attr_to_delete]
                attr_i = HydraIface.TemplateItem(attr_id=attr_id, template_id=template_id)
                attr_i.delete()
                attr_i.save()

            #Add or update template items
            for attribute in resource.findall('attribute'):
                parse_templateitem(template_i.db.template_id, attribute)

        grp_i.load_all()

        return get_as_complexmodel(ctx, grp_i)

    @rpc(String, Integer, _returns=SpyneArray(GroupSummary))
    def get_matching_resource_templates(ctx, resource_type, resource_id):
        """
            Get the possible types (templates) of a resource by checking its attributes
            against all available templates.

            @returns A list of GroupSummary objects.
        """
        resource_i = None
        if resource_type == 'NETWORK':
            resource_i = HydraIface.Network(network_id=resource_id)
        elif resource_type == 'NODE':
            resource_i = HydraIface.Node(node_id=resource_id)
        elif resource_type == 'LINK':
            resource_i = HydraIface.Link(link_id=resource_id)
        elif resource_type == 'GROUP':
            resource_i = HydraIface.ResourceGroup(link_id=resource_id)

        template_groups = resource_i.get_templates_by_attr()
        template_list = []
        for group_id, group in template_groups.items():
            group_name = group['group_name']
            templates  = group['templates']

            group_summary           = GroupSummary()
            group_summary.id        = group_id
            group_summary.name      = group_name
            group_summary.templates = []

            for template_id, template_name in templates:
                template_summary      = TemplateSummary()
                template_summary.id   = template_id
                template_summary.name = template_name
                group_summary.templates.append(template_summary)

            template_list.append(group_summary)

        return template_list

    @rpc(Integer, String, Integer, _returns=Template)
    def assign_type_to_resource(ctx, type_id, resource_type, resource_id):
        """Assign new type to a resource. This function checks if the necessary
        attributes are present and adds them if needed. Non existing attributes
        are also added when the type is already assigned. This means that this
        function can also be used to update resources, when a resource type has
        changed.
        """
        # Get necessary information
        resource_i = None
        if resource_type == 'NETWORK':
            resource_i = HydraIface.Network(network_id=resource_id)
            res_id = resource_i.db.network_id
        elif resource_type == 'NODE':
            resource_i = HydraIface.Node(node_id=resource_id)
            res_id = resource_i.db.node_id
        elif resource_type == 'LINK':
            resource_i = HydraIface.Link(link_id=resource_id)
            res_id = resource_i.db.link_id
        elif resource_type == 'GROUP':
            resource_i = HydraIface.ResourceGroup(link_id=resource_id)
            res_id = resource_i.db.group_id

        resource_i.load_children()
        existing_attr_ids = []
        for attr in resource_i.attributes:
            existing_attr_ids.append(attr.db.attr_id)

        template_i = HydraIface.Template(template_id=type_id)
        template_i.load_children()
        tmpl_attrs = dict()
        for tmplattr in template_i.templateitems:
            tmpl_attrs.update({tmplattr.db.attr_id:
                               tmplattr.db.attr_is_var})

        # check if attributes exist
        missing_attr_ids = set(tmpl_attrs.keys()) - set(existing_attr_ids)

        # add attributes if necessary
        for attr_id in missing_attr_ids:
            resource_i.add_attribute(attr_id, attr_is_var=tmpl_attrs[attr_id])

        resource_i.save()

        # add type to tResourceType if it doesn't exist already
        resource_type = HydraIface.ResourceType(ref_key=resource_type,
                                                ref_id=res_id,
                                                template_id=type_id)
        if not resource_type.load():
            resource_type.save()

        template = resource_type.get_template()
        return get_as_complexmodel(ctx, template)

    @rpc(TemplateGroup, _returns=TemplateGroup)
    def add_templategroup(ctx, group):
        """
            Add group and a template and items.
        """
        grp_i = HydraIface.TemplateGroup()
        grp_i.db.group_name = group.name

        grp_i.save()

        for template in group.templates:
            rt_i = grp_i.add_template(template.name)
            for item in template.templateitems:
                rt_i.add_item(item.attr_id)

        return get_as_complexmodel(ctx, grp_i)

    @rpc(TemplateGroup, _returns=TemplateGroup)
    def update_templategroup(ctx, group):
        """
            Update group and a template and items.
        """
        grp_i = HydraIface.TemplateGroup(group_id=group.id)
        grp_i.db.group_name = group.name
        grp_i.load_all()

        for template in group.templates:
            if template.id is not None:
                for tmpl_i in grp_i.templates:
                    if tmpl_i.db.template_id == template.id:
                        tmpl_i.db.template_name = template.name
                        tmpl_i.db.alias = template.alias
                        tmpl_i.db.layout = template.layout
                        HydraIface.validate_layout(tmpl_i.db.layout)
                        tmpl_i.save()
                        tmpl_i.load_all()
                        break
            else:
                tmpl_i = grp_i.add_template(template.name)

            for item in template.templateitems:
                for item_i in tmpl_i.templateitems:
                    if item_i.db.attr_id == item.attr_id:
                        break
                else:
                    tmpl_i.add_item(item.attr_id)
                    tmpl_i.save()

        grp_i.commit()
        return get_as_complexmodel(ctx, grp_i)

    @rpc(_returns=SpyneArray(TemplateGroup))
    def get_templategroups(ctx):
        """
            Get all resource template groups.
        """

        groups = []

        sql = """
            select
                group_id
            from
                tTemplateGroup
        """

        rs = HydraIface.execute(sql)

        for r in rs:
            grp_i = HydraIface.TemplateGroup(group_id=r.group_id)
            grp_i.load_all()
            grp = get_as_complexmodel(ctx, grp_i)
            groups.append(grp)

        return groups

    @rpc(Integer, _returns=TemplateGroup)
    def get_templategroup(ctx, group_id):
        """
            Get a specific resource template group, either by ID or name.
        """

        grp_i = HydraIface.TemplateGroup(group_id=group_id)
        grp_i.load_all()
        grp = get_as_complexmodel(ctx, grp_i)

        return grp

    @rpc(String, _returns=TemplateGroup)
    def get_templategroup_by_name(ctx, name):
        """
            Get a specific resource template group, either by ID or name.
        """
        sql = """
            select
                group_id
            from
                tTemplateGroup
            where
                group_name = '%s'
        """ % name

        rs = HydraIface.execute(sql)

        if len(rs) != 1:
            logging.info("%s is not a valid identifier for a group",name)
            return None

        group_id = rs[0].group_id

        grp_i = HydraIface.TemplateGroup(group_id=group_id)
        grp_i.load_all()
        grp = get_as_complexmodel(ctx, grp_i)

        return grp

    @rpc(Template, _returns=Template)
    def add_template(ctx, template):
        """
            Add a resource template with items.
        """
        rt_i = HydraIface.Template()
        rt_i.db.template_name  = template.name
        rt_i.db.alias  = template.alias
        rt_i.db.layout = template.layout
        HydraIface.validate_layout(rt_i.db.layout)

        if template.group_id is not None:
            rt_i.db.group_id = template.group_id
        else:
            rt_i.db.group_id = 1 # 1 is always the default group

        rt_i.save()

        for item in template.templateitems:
            rt_i.add_item(item.attr_id)

        return get_as_complexmodel(ctx, rt_i)

    @rpc(Template, _returns=Template)
    def update_template(ctx, template):
        """
            Update a resource template and its items.
            New items will be added. Items not sent will be ignored.
            To delete items, call delete_templateitem
        """
        rt_i = HydraIface.Template(template_id = template.id)
        rt_i.db.template_name  = template.name
        rt_i.db.alias  = template.alias
        rt_i.db.layout = template.layout
        HydraIface.validate_layout(rt_i.db.layout)
        rt_i.load_all()

        if template.group_id is not None:
            rt_i.db.group_id = template.group_id
        else:
            rt_i.db.group_id = 1 # 1 is always the default group

        for item in template.templateitems:
            for item_i in rt_i.templateitems:
                if item_i.db.attr_id == item.attr_id:
                    break
            else:
                rt_i.add_item(item.attr_id)

        rt_i.save()

        return get_as_complexmodel(ctx, rt_i)

    @rpc(Integer, _returns=Template)
    def get_template(ctx, template_id):
        """
            Get a specific resource template by ID.
        """
        tmpl_i = HydraIface.Template(template_id=template_id)
        tmpl_i.load_all()
        tmpl = get_as_complexmodel(ctx, tmpl_i)

        return tmpl

    @rpc(Integer, String, _returns=Template)
    def get_template_by_name(ctx, group_id, template_name):
        """
            Get a specific resource template by name.
        """

        if group_id is None:
            logging.info("Group is empty, setting group to default group (group_id=1)")
            group_id = 1 # group 1 is the default

        sql = """
            select
                tmpl.template_id
            from
                tTemplate tmpl,
                tTemplateGroup grp
            where
                grp.group_id = %s
            and tmpl.group_id = grp.group_id
            and tmpl.template_name = '%s'
        """ % (group_id, template_name)

        rs = HydraIface.execute(sql)

        if len(rs) != 1:
            raise HydraError("%s is not a valid identifier for a template"%(template_name))

        template_id = rs[0].template_id

        tmpl_i = HydraIface.Template(template_id=template_id)
        tmpl_i.load_all()
        tmpl = get_as_complexmodel(ctx, tmpl_i)

        return tmpl

    @rpc(TemplateItem, _returns=Template)
    def add_templateitem(ctx, item):
        """
            Add an item to an existing template.
        """
        rt_i = HydraIface.Template(template_id = item.template_id)
        rt_i.load_all()
        rt_i.add_item(item.attr_id)
        rt_i.save()
        return get_as_complexmodel(ctx, rt_i)


    @rpc(TemplateItem, _returns=Template)
    def delete_templateitem(ctx, item):
        """
            Remove an item from an existing template
        """
        rt_i = HydraIface.Template(template_id = item.template_id)

        rt_i.load_all()
        rt_i.remove_item(item.attr_id)
        rt_i.save()

        return get_as_complexmodel(ctx, rt_i)

    @rpc(Integer, _returns=Unicode)
    def get_network_as_xml_template(ctx, network_id):
        """
            Turn an existing network into an xml template
            using its attributes.
            If an optional scenario ID is passed in, default
            values will be populated from that scenario.
        """
        template_xml = etree.Element("template_definition")

        net_i = HydraIface.Network(network_id=network_id)
        net_i.load_all()

        template_name = etree.SubElement(template_xml, "template_name")
        template_name.text = "Templated from Network %s"%(net_i.db.network_name)

        resources = etree.SubElement(template_xml, "resources")
        if net_i.get_attributes():
            net_resource    = etree.SubElement(resources, "resource")

            resource_type   = etree.SubElement(net_resource, "type")
            resource_type.text   = "NETWORK"

            resource_name   = etree.SubElement(net_resource, "name")
            resource_name.text   = net_i.db.network_name

            if net_i.db.network_layout is not None:
                resource_layout = etree.SubElement(net_resource, "layout")
                resource_layout.text = net_i.db.network_layout

            for net_attr in net_i.get_attributes():
                _make_attr_element(net_resource, net_attr)

        existing_tmpls = {'NODE': [], 'LINK': []}
        for node_i in net_i.nodes:
            node_attributes = node_i.get_attributes()
            attr_ids = [attr.db.attr_id for attr in node_attributes]
            if attr_ids>0 and attr_ids not in existing_tmpls['NODE']:

                node_resource    = etree.Element("resource")

                resource_type   = etree.SubElement(node_resource, "type")
                resource_type.text   = "NODE"

                resource_name   = etree.SubElement(node_resource, "name")
                resource_name.text   = node_i.db.node_name

                if node_i.db.node_layout not in ('', None):
                    resource_layout = etree.SubElement(node_resource, "layout")
                    resource_layout.text = node_i.db.node_layout

                for node_attr in node_attributes:
                    _make_attr_element(node_resource, node_attr)

                existing_tmpls['NODE'].append(attr_ids)
                resources.append(node_resource)

        for link_i in net_i.links:
            link_attributes = link_i.get_attributes()
            attr_ids = [attr.db.attr_id for attr in link_attributes]
            if attr_ids>0 and attr_ids not in existing_tmpls['LINK']:
                link_resource    = etree.Element("resource")

                resource_type   = etree.SubElement(link_resource, "type")
                resource_type.text   = "LINK"

                resource_name   = etree.SubElement(link_resource, "name")
                resource_name.text   = link_i.db.link_name

                if link_i.db.link_layout not in ('', None):
                    resource_layout = etree.SubElement(link_resource, "layout")
                    resource_layout.text = link_i.db.link_layout

                for link_attr in link_attributes:
                    _make_attr_element(link_resource, link_attr)

                existing_tmpls['LINK'].append(attr_ids)
                resources.append(link_resource)

        xml_string = etree.tostring(template_xml)

        return xml_string

def _make_attr_element(parent, resource_attr_i):
    """
        General function to add an attribute element to a resource element.
    """
    attr = etree.SubElement(parent, "attribute")
    attr_i = resource_attr_i.get_attr()

    attr_name      = etree.SubElement(attr, 'name')
    attr_name.text = attr_i.db.attr_name

    attr_dimension = etree.SubElement(attr, 'dimension')
    attr_dimension.text = attr_i.db.attr_dimen

    attr_is_var    = etree.SubElement(attr, 'is_var')
    attr_is_var.text = resource_attr_i.db.attr_is_var

    # if scenario_id is not None:
    #     for rs in resource_attr_i.get_resource_scenarios():
    #         if rs.db.scenario_id == scenario_id
    #             attr_default   = etree.SubElement(attr, 'default')
    #             default_val = etree.SubElement(attr_default, 'value')
    #             default_val.text = rs.get_dataset().get_val()
    #             default_unit = etree.SubElement(attr_default, 'unit')
    #             default_unit.text = rs.get_dataset().db.unit

    return attr

def convert_layout_xml_to_dict(layout_tree):
    """
    Convert something that looks like this:
    <resource_layout>
        <layout>
            <name>color</name>
            <value>red</value>
        </layout>
        <layout>
            <name>shapefile</name>
            <value>blah.shp</value>
        </layout>
    </resource_layout>
    Into something that looks like this:
    {
        'color' : ['red'],
        'shapefile' : ['blah.shp']
    }
    """
    layout_dict = dict()

    for layout in layout_tree.findall('layout'):
        name = layout.find('name').text
        value = layout.find('value').text
        layout_dict[name] = [value]
    return layout_dict
