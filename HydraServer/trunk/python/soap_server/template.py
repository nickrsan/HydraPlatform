
from spyne.model.complex import Array as SpyneArray
from spyne.model.primitive import String, Integer, Unicode
from spyne.decorator import rpc
from hydra_complexmodels import Template, TemplateGroup, TemplateItem,ResourceGroupSummary, TemplateSummary

from db import HydraIface
from hydra_base import HydraService
from HydraLib.HydraException import HydraError
import logging
from HydraLib import config
from lxml import etree
import attributes
from decimal import Decimal

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
                " dimension (%s). Please rename your attribute.",
                (name, attr_i.db.dimension))

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
            Decimal(val)
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

        return grp_i.get_as_complexmodel()

    @rpc(String, Integer, _returns=SpyneArray(ResourceGroupSummary))
    def get_matching_resource_templates(ctx, resource_type, resource_id):
        """
            Get the possible types (templates) of a resource by checking its attributes 
            against all available templates.

            @returns A list of ResourceGroupSummary objects.
        """
        resource_i = None
        if resource_type == 'NETWORK':
            resource_i = HydraIface.Network(network_id=resource_id)
        elif resource_type == 'NODE':
            resource_i = HydraIface.Node(node_id=resource_id)
        elif resource_type == 'LINK':
            resource_i = HydraIface.Link(link_id=resource_id)

        template_groups = resource_i.get_templates_by_attr()
        template_list = []
        for group_id, group in template_groups.items():
            group_name = group['group_name']
            templates  = group['templates']

            group_summary = ResourceGroupSummary()
            group_summary.id   = group_id
            group_summary.name = group_name
            group_summary.templates = []
            
            for template_id, template_name in templates:
                template_summary = TemplateSummary()
                template_summary.id = template_id
                template_summary.name = template_name
                group_summary.templates.append(template_summary)
            
            template_list.append(group_summary)

        return template_list

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

        return grp_i.get_as_complexmodel()

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
        return grp_i.get_as_complexmodel()

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
            grp = grp_i.get_as_complexmodel()
            groups.append(grp)

        return groups

    @rpc(Integer, _returns=TemplateGroup)
    def get_templategroup(ctx, group_id):
        """
            Get a specific resource template group, either by ID or name.   
        """
        
        grp_i = HydraIface.TemplateGroup(group_id=group_id)
        grp_i.load_all()
        grp = grp_i.get_as_complexmodel()

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
            raise HydraError("%s is not a valid identifier for a group"%(name))

        group_id = rs[0].group_id

        grp_i = HydraIface.TemplateGroup(group_id=group_id)
        grp_i.load_all()
        grp = grp_i.get_as_complexmodel()

        return grp

    @rpc(Template, _returns=Template)
    def add_template(ctx, template):
        """
            Add a resource template with items.
        """
        rt_i = HydraIface.Template()
        rt_i.db.template_name  = template.name
        
        if template.group_id is not None:
            rt_i.db.group_id = template.group_id
        else:
            rt_i.db.group_id = 1 # 1 is always the default group
       
        rt_i.save()

        for item in template.templateitems:
            rt_i.add_item(item.attr_id)

        return rt_i.get_as_complexmodel()

    @rpc(Template, _returns=Template)
    def update_template(ctx, template):
        """
            Update a resource template and its items.
            New items will be added. Items not sent will be ignored.
            To delete items, call delete_templateitem
        """
        rt_i = HydraIface.Template(template_id = template.id)
        rt_i.db.template_name  = template.name
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

        return rt_i.get_as_complexmodel()

    @rpc(Integer, _returns=Template)
    def get_template(ctx, template_id):
        """
            Get a specific resource template by ID.   
        """
        tmpl_i = HydraIface.Template(template_id=template_id)
        tmpl_i.load_all()
        tmpl = tmpl_i.get_as_complexmodel()

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
        tmpl = tmpl_i.get_as_complexmodel()

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
        return rt_i.get_as_complexmodel()

  
    @rpc(TemplateItem, _returns=Template)
    def delete_templateitem(ctx, item):
        """
            Remove an item from an existing template
        """
        rt_i = HydraIface.Template(template_id = item.template_id)
 
        rt_i.load_all()
        rt_i.remove_item(item.attr_id) 
        rt_i.save()

        return rt_i.get_as_complexmodel()

