from spyne.model.complex import Array as SpyneArray
from spyne.model.primitive import String, Integer, Unicode
from spyne.decorator import rpc
from hydra_complexmodels import Template,\
TemplateType,\
TypeAttr,\
TypeSummary,\
ResourceTypeDef,\
get_as_complexmodel

from db import HydraIface
from db import IfaceLib
from hydra_base import HydraService
from HydraLib.HydraException import HydraError
import logging
from HydraLib import config
from lxml import etree
import attributes
from decimal import Decimal
from db.hdb import make_param
import datetime

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

def parse_typeattr(type_id, attribute):

    attr_i = parse_attribute(attribute)

    dimension = attribute.find('dimension').text

    typeattr_i = HydraIface.TypeAttr(type_id=type_id, attr_id=attr_i.db.attr_id)

    if attribute.find('is_var') is not None:
        typeattr_i.db.attr_is_var = attribute.find('is_var').text

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
        typeattr_i.db.default_dataset_id = dataset_id

    return typeattr_i

class TemplateService(HydraService):
    """
        The template SOAP service
    """

    @rpc(Unicode, _returns=Template)
    def upload_template_xml(ctx, template_xml):
        """
            Add the template, type and typeattrs described
            in an XML file.

            Delete type, typeattr entries in the DB that are not in the XML file
            The assumption is that they have been deleted and are no longer required.
        """

        template_xsd_path = config.get('templates', 'template_xsd_path')
        xmlschema_doc = etree.parse(template_xsd_path)

        xmlschema = etree.XMLSchema(xmlschema_doc)

        xml_tree = etree.fromstring(template_xml)

        xmlschema.assertValid(xml_tree)

        template_name = xml_tree.find('template_name').text

        sql = """
            select
                tmpl.template_id,
                tmpl.template_name,
                type.type_name,
                type.type_id,
                type.alias,
                type.layout,
                attr.attr_name,
                attr.attr_id
            from
                tTemplate tmpl,
                tTemplateType type,
                tTypeAttr typeattr,
                tAttr attr
            where
                tmpl.template_name = '%s'
            and type.template_id   = tmpl.template_id
            and typeattr.type_id   = type.type_id
            and attr.attr_id       = typeattr.attr_id
        """ % template_name

        rs = HydraIface.execute(sql)

        if len(rs) > 0:
            tmpl_i = HydraIface.Template(template_id = rs[0].template_id)
            tmpl_i.load_all()
        else:
            tmpl_i = HydraIface.Template()
            tmpl_i.db.template_name = template_name
            tmpl_i.save()

        types = xml_tree.find('resources')

        #Delete any types which are in the DB but no longer in the XML file
        type_name_map = {r.type_name:r.type_id for r in rs}
        attr_name_map = {r.attr_name:(r.attr_id,r.type_id) for r in rs}

        existing_types = set([r.type_name for r in rs])

        new_types = set([r.find('name').text for r in types.findall('resource')])

        types_to_delete = existing_types - new_types

        for type_to_delete in types_to_delete:
            type_id = type_name_map[type_to_delete]
            type_i = HydraIface.TemplateType(type_id=type_id)
            type_i.load_all()
            type_i.delete()
            type_i.save()

        #Add or update types.
        for resource in types.findall('resource'):
            type_name = resource.find('name').text
            #check if the type is already in the DB. If not, create a new one.
            if type_name in existing_types:
                type_id = type_name_map[type_name]
                type_i = HydraIface.TemplateType(type_id=type_id)
            else:
                type_i = HydraIface.TemplateType()
                type_i.db.template_id = tmpl_i.db.template_id
                type_i.db.type_name = resource.find('name').text

                if resource.find('alias') is not None:
                    type_i.db.alias = resource.find('alias').text

                if resource.find('layout') is not None and \
                   resource.find('layout').text is not None:
                    layout = resource.find('layout')
                    layout_string = get_layout_as_dict(layout)
                    type_i.db.layout = str(layout_string)

                type_i.save()

            #delete any TypeAttrs which are in the DB but not in the XML file
            existing_attrs = []
            for r in rs:
                if r.type_name == type_name:
                    existing_attrs.append(r.attr_name)

            existing_attrs = set(existing_attrs)

            template_attrs = set([r.find('name').text for r in resource.findall('attribute')])

            attrs_to_delete = existing_attrs - template_attrs

            for attr_to_delete in attrs_to_delete:
                attr_id, type_id = attr_name_map[attr_to_delete]
                attr_i = HydraIface.TypeAttr(attr_id=attr_id, type_id=type_id)
                attr_i.delete()
                attr_i.save()

            #Add or update type typeattrs
            typeattrs = []
            for attribute in resource.findall('attribute'):
                typeattr = parse_typeattr(type_i.db.type_id, attribute)
                if typeattr.in_db is False:
                    typeattrs.append(typeattr)
                else:
                    typeattr.save()
            IfaceLib.bulk_insert(typeattrs, 'tTypeAttr')

        tmpl_i.load_all()

        return get_as_complexmodel(ctx, tmpl_i)

    @rpc(String, Integer, _returns=SpyneArray(TypeSummary))
    def get_matching_resource_types(ctx, resource_type, resource_id):
        """
            Get the possible types of a resource by checking its attributes
            against all available types.

            @returns A list of TypeSummary objects.
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

        templates = resource_i.get_types_by_attr()
        type_list = []
        for template_id, template in templates.items():
            template_name = template['template_name']
            types  = template['types']

            for type_id, type_name in types:
                type_summary      = TypeSummary()
                type_summary.id   = template_id
                type_summary.name = template_name
                type_summary.id   = type_id
                type_summary.name = type_name

                type_list.append(type_summary)

        return type_list

    @rpc(SpyneArray(ResourceTypeDef),
         _returns=SpyneArray(TemplateType))
    def assign_types_to_resources(ctx, resource_types):
        """Assign new types to list of resources.
        This function checks if the necessary
        attributes are present and adds them if needed. Non existing attributes
        are also added when the type is already assigned. This means that this
        function can also be used to update resources, when a resource type has
        changed.
        """
        types = {}
        types_resources = {}
        all_new_attrs = []
        all_new_resource_types = []
        resources = []
        x = datetime.datetime.now()
        for resource_type in resource_types:
            ref_id = resource_type.ref_id
            type_id = resource_type.type_id
            if types.get(resource_type.type_id) is None:
                t = HydraIface.TemplateType(type_id=type_id)
                t.load_all()
                types[resource_type.type_id] = t
        logging.info("Types loaded in %s",(datetime.datetime.now()-x))
        node_ids = []
        link_ids = []
        network_id = None
        for resource_type in resource_types:
            ref_id  = resource_type.ref_id
            ref_key = resource_type.ref_key
            type_id = resource_type.type_id
            if resource_type.ref_key == 'NETWORK':
                resource = HydraIface.Network()
                resource.db.network_id=ref_id
                network_id=ref_id
            elif resource_type.ref_key == 'NODE':
                resource = HydraIface.Node()
                resource.db.node_id=ref_id
                node_ids.append(ref_id)
            elif resource_type.ref_key == 'LINK':
                resource = HydraIface.Link()
                resource.db.link_id=ref_id
                link_ids.append(ref_id)
            elif resource_type.ref_key == 'GROUP':
                resource = HydraIface.Group()
                resouce.db.resourcegroup_id=ref_id
            resource.ref_key = ref_key
            resource.ref_id  = ref_id
            resources.append(resource)
            types_resources.update({(ref_key, ref_id): type_id})

        logging.info("Types loaded in %s",(datetime.datetime.now()-x))
        sql = """
            select
                rt.type_id,
                rt.ref_id,
                rt.ref_key
            from
                tResourceType rt
            where
                (rt.ref_key = 'NETWORK' and rt.ref_id = %(network_id)s
            or  rt.ref_key = 'NODE'    and rt.ref_id in %(node_ids)s
            or  rt.ref_key = 'LINK'    and rt.ref_id in %(link_ids)s)
            order by ref_key
        """% {
                'network_id' :network_id,
                'node_ids'   :make_param(node_ids),
                'link_ids'   :make_param(link_ids)
            }
        type_rs = HydraIface.execute(sql)
        current_resource_types = [(r.ref_key, r.ref_id, r.type_id) for r in type_rs]
        for resource in resources:
            new_attrs, new_resource_type = \
                resource.set_type(types_resources[(resource.ref_key,
                                                   resource.ref_id)],
                                  types)
            if (new_resource_type.db.ref_key, new_resource_type.db.ref_id,\
                new_resource_type.db.type_id) not in current_resource_types:
                all_new_resource_types.append(new_resource_type)

            all_new_attrs.extend(new_attrs)

        IfaceLib.bulk_insert(all_new_attrs, 'tResourceAttr')
        IfaceLib.bulk_insert(all_new_resource_types, 'tResourceType')

        ret_val = [get_as_complexmodel(ctx, t) for t in types.values()]
        return ret_val


    @rpc(Integer, String, Integer, _returns=TemplateType)
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

        resource_i.get_attributes()
        existing_attr_ids = []
        for attr in resource_i.attributes:
            existing_attr_ids.append(attr.db.attr_id)

        type_i = HydraIface.TemplateType(type_id=type_id)
        type_i.load_all()
        type_attrs = dict()
        for typeattr in type_i.typeattrs:
            type_attrs.update({typeattr.db.attr_id:
                               typeattr.db.attr_is_var})

        # check if attributes exist
        missing_attr_ids = set(type_attrs.keys()) - set(existing_attr_ids)

        # add attributes if necessary
        new_res_attrs = []
        for attr_id in missing_attr_ids:
            ra_i = resource_i.add_attribute(attr_id,
                                            attr_is_var=type_attrs[attr_id])
            new_res_attrs.append(ra_i)

        IfaceLib.bulk_insert(new_res_attrs, 'tResourceAttr')

        resource_i.save()

        # add type to tResourceType if it doesn't exist already
        resource_type = HydraIface.ResourceType(ref_key=resource_type,
                                                ref_id=res_id,
                                                type_id=type_id)
        if not resource_type.load():
            resource_type.save()

        templatetype = resource_type.get_type()

        return get_as_complexmodel(ctx, templatetype)

    @rpc(Template, _returns=Template)
    def add_template(ctx, template):
        """
            Add template and a type and typeattrs.
        """
        tmpl_i = HydraIface.Template()
        tmpl_i.db.template_name = template.name

        tmpl_i.save()

        for templatetype in template.types:
            rt_i = tmpl_i.add_type(templatetype.name)
            for typeattr in templatetype.typeattrs:
                ta = rt_i.add_typeattr(typeattr.attr_id)
                ta.save()
        tmpl_i.load_all()

        return get_as_complexmodel(ctx, tmpl_i)

    @rpc(Template, _returns=Template)
    def update_template(ctx, template):
        """
            Update template and a type and typeattrs.
        """
        tmpl_i = HydraIface.Template(template_id=template.id)
        tmpl_i.db.template_name = template.name
        tmpl_i.load_all()

        for templatetype in template.types:
            if templatetype.id is not None:
                for type_i in tmpl_i.templatetypes:
                    if type_i.db.template_id == templatetype.id:
                        type_i.db.type_name = templatetype.name
                        type_i.db.alias     = templatetype.alias
                        type_i.db.layout    = templatetype.layout
                        type_i.save()
                        type_i.load_all()
                        break
            else:
                type_i = tmpl_i.add_type(templatetype.name)

            for typeattr in templatetype.typeattrs:
                for typeattr_i in type_i.typeattrs:
                    if typeattr_i.db.attr_id == typeattr.attr_id:
                        break
                else:
                    type_i.add_typeattr(typeattr.attr_id)
                    type_i.save()

        tmpl_i.commit()
        return get_as_complexmodel(ctx, tmpl_i)

    @rpc(_returns=SpyneArray(Template))
    def get_templates(ctx):
        """
            Get all resource template templates.
        """

        templates = []

        sql = """
            select
                template_id
            from
                tTemplate
        """

        rs = HydraIface.execute(sql)

        for r in rs:
            tmpl_i = HydraIface.Template(template_id=r.template_id)
            tmpl_i.load_all()
            tmpl = get_as_complexmodel(ctx, tmpl_i)
            templates.append(tmpl)

        return templates

    @rpc(Integer, Integer, _returns=TemplateType)
    def remove_attr_from_type(ctx, type_id, attr_id):
        """

            Remove an attribute from a type
        """
        typeattr_i = HydraIface.TypeAttr(type_id=type_id, attr_id=attr_id)
        typeattr_i.delete()

    @rpc(Integer, _returns=Template)
    def get_template(ctx, template_id):
        """
            Get a specific resource template template, either by ID or name.
        """

        tmpl_i = HydraIface.Template(template_id=template_id)
        tmpl_i.load_all()
        tmpl = get_as_complexmodel(ctx, tmpl_i)

        return tmpl

    @rpc(String, _returns=Template)
    def get_template_by_name(ctx, name):
        """
            Get a specific resource template, either by ID or name.
        """
        sql = """
            select
                template_id
            from
                tTemplate
            where
                template_name = '%s'
        """ % name

        rs = HydraIface.execute(sql)

        if len(rs) != 1:
            logging.info("%s is not a valid identifier for a template",name)
            return None

        template_id = rs[0].template_id

        tmpl_i = HydraIface.Template(template_id=template_id)
        tmpl_i.load_all()
        tmpl = get_as_complexmodel(ctx, tmpl_i)

        return tmpl

    @rpc(TemplateType, _returns=TemplateType)
    def add_templatetype(ctx, templatetype):
        """
            Add a template type with typeattrs.
        """
        rt_i = HydraIface.TemplateType()
        rt_i.db.type_name  = templatetype.name
        rt_i.db.alias  = templatetype.alias
        rt_i.db.layout = templatetype.layout

        if templatetype.template_id is not None:
            rt_i.db.template_id = templatetype.template_id
        else:
            rt_i.db.template_id = 1 # 1 is always the default template

        rt_i.save()

        for typeattr in templatetype.typeattrs:
            typeattr_i = rt_i.add_typeattr(typeattr.attr_id)
            typeattr_i.save()

        return get_as_complexmodel(ctx, rt_i)

    @rpc(TemplateType, _returns=TemplateType)
    def update_templatetype(ctx, templatetype):
        """
            Update a resource type and its typeattrs.
            New typeattrs will be added. typeattrs not sent will be ignored.
            To delete typeattrs, call delete_typeattr
        """
        type_i = HydraIface.TemplateType(type_id = templatetype.id)
        type_i.db.type_name  = templatetype.name
        type_i.db.alias  = templatetype.alias
        type_i.db.layout = templatetype.layout
        type_i.load_all()

        if templatetype.template_id is not None:
            type_i.db.template_id = templatetype.template_id
        else:
            type_i.db.template_id = 1 # 1 is always the default template
        typeattrs = []
        for typeattr in templatetype.typeattrs:
            for typeattr_i in type_i.typeattrs:
                if typeattr_i.db.attr_id == typeattr.attr_id:
                    break
            else:
                typeattr_i = type_i.add_typeattr(typeattr.attr_id)
                typeattrs.append(typeattr_i)

        IfaceLib.bulk_insert(typeattrs, 'tTypeAttr')
        type_i.save()

        return get_as_complexmodel(ctx, type_i)

    @rpc(Integer, _returns=TemplateType)
    def get_templatetype(ctx, type_id):
        """
            Get a specific resource type by ID.
        """
        type_i = HydraIface.TemplateType(type_id=type_id)
        type_i.load_all()
        templatetype = get_as_complexmodel(ctx, type_i)

        return templatetype

    @rpc(Integer, String, _returns=TemplateType)
    def get_templatetype_by_name(ctx, template_id, type_name):
        """
            Get a specific resource type by name.
        """

        if template_id is None:
            logging.info("Template is empty, setting template to default template (template_id=1)")
            template_id = 1 # template 1 is the default

        sql = """
            select
                type.type_id
            from
                tTemplateType type,
                tTemplate tmpl
            where
                tmpl.template_id = %s
            and type.template_id = tmpl.template_id
            and type.type_name = '%s'
        """ % (template_id, type_name)

        rs = HydraIface.execute(sql)

        if len(rs) != 1:
            raise HydraError("%s is not a valid identifier for a type"%(type_name))

        type_id = rs[0].type_id

        type_i = HydraIface.TemplateType(type_id=type_id)
        type_i.load_all()
        tmpltype = get_as_complexmodel(ctx, type_i)

        return tmpltype

    @rpc(TypeAttr, _returns=TemplateType)
    def add_typeattr(ctx, typeattr):
        """
            Add an typeattr to an existing type.
        """
        rt_i = HydraIface.TemplateType(type_id = typeattr.type_id)
        rt_i.load_all()
        attr_i = rt_i.add_typeattr(typeattr.attr_id)
        attr_i.save()
        rt_i.save()
        return get_as_complexmodel(ctx, rt_i)


    @rpc(TypeAttr, _returns=TemplateType)
    def delete_typeattr(ctx, typeattr):
        """
            Remove an typeattr from an existing type
        """
        rt_i = HydraIface.TemplateType(type_id = typeattr.type_id)

        rt_i.load_all()
        rt_i.remove_typeattr(typeattr.attr_id)
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

        type_name = etree.SubElement(template_xml, "template_name")
        type_name.text = "TemplateType from Network %s"%(net_i.db.network_name)

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

        existing_types = {'NODE': [], 'LINK': []}
        for node_i in net_i.nodes:
            node_attributes = node_i.get_attributes()
            attr_ids = [attr.db.attr_id for attr in node_attributes]
            if attr_ids>0 and attr_ids not in existing_types['NODE']:

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

                existing_types['NODE'].append(attr_ids)
                resources.append(node_resource)

        for link_i in net_i.links:
            link_attributes = link_i.get_attributes()
            attr_ids = [attr.db.attr_id for attr in link_attributes]
            if attr_ids>0 and attr_ids not in existing_types['LINK']:
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

                existing_types['LINK'].append(attr_ids)
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

def get_layout_as_dict(layout_tree):
    """
    Convert something that looks like this:
    <layout>
        <item>
            <name>color</name>
            <value>red</value>
        </item>
        <item>
            <name>shapefile</name>
            <value>blah.shp</value>
        </item>
    </layout>
    Into something that looks like this:
    {
        'color' : ['red'],
        'shapefile' : ['blah.shp']
    }
    """
    layout_dict = dict()

    for item in layout_tree.findall('item'):
        name  = item.find('name').text
        value = item.find('value').text
        layout_dict[name] = [value]
    return layout_dict

