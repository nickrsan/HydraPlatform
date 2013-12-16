from spyne.model.primitive import Integer, Boolean, String
from spyne.decorator import rpc
from hydra_complexmodels import Attr
from db import HydraIface
from hydra_complexmodels import TemplateGroup,\
        Template, \
        Resource

from hydra_base import HydraService


from HydraLib import hdb

def get_resource(ref_key, ref_id):
    if ref_key == 'NODE':
        return HydraIface.Node(node_id = ref_id)
    elif ref_key == 'LINK':
        return HydraIface.Link(link_id = ref_id)
    elif ref_key == 'NETWORK':
        return HydraIface.Network(network_id = ref_id)
    elif ref_key == 'SCENARIO':
        return HydraIface.Scenario(scenario_id = ref_id)
    elif ref_key == 'PROJECT':
        return HydraIface.Project(project_id = ref_id)
    else:
        return None

class AttributeService(HydraService):
    """
        The attribute SOAP service
    """

    @rpc(Attr, _returns=Attr)
    def add_attribute(ctx, attr):
        """
            Add a generic attribute, which can then be used in creating
            a resource attribute, and put into a template.
            (Attr){
                id = 1020
                name = "Test Attr"
                dimen = "very big"
            }

        """
        x = HydraIface.Attr()
        x.db.attr_name = attr.name
        x.db.attr_dimen = attr.dimen
        x.save()
        hdb.commit()
        return x.get_as_complexmodel()

    @rpc(Integer, _returns=Boolean)
    def delete_attribute(ctx, attr_id):
        """
            Set the status of an attribute to 'X'
        """
        success = True
        x = HydraIface.Attr(attr_id = attr_id)
        x.db.status = 'X'
        x.save()
        hdb.commit()
        return success
        

    @rpc(String, Integer, Integer, Boolean, _returns=Resource)
    def add_resource_attribute(ctx,resource_type, resource_id, attr_id, is_var):
        """
            Add a resource attribute attribute to a resource.

            attr_is_var indicates whether the attribute is a variable or not --
            this is used in simulation to indicate that this value is expected
            to be filled in by the simulator.
        """
        resource_i = get_resource(resource_type, resource_id)
        resource_i.load()

        attr_is_var = 'Y' if is_var else 'N'

        resource_i.add_attribute(attr_id, attr_is_var)

        return resource_i.get_as_complexmodel()

    @rpc(TemplateGroup, _returns=TemplateGroup)
    def add_template_group(ctx, group):
        """
            Add a template group
        """

        g_i = HydraIface.TemplateGroup()
        g_i.db.group_name = group.name

        g_i.save()

        for template in group.templates:
            t_i = g_i.add_template(template.name)
            for item in template.templateitmes:
                t_i.add_item(item.attr_id)

        return g_i.get_as_complexmodel()

    @rpc(TemplateGroup, _returns=TemplateGroup)
    def update_template_group(ctx, group):
        """
            Add a template group
        """
        g_i = HydraIface.TemplateGroup(group_id=group.id)
        g_i.db.group_name = group.name

        g_i.save()

        for template in group.templates:
            t_i = g_i.add_template(template.name)
            for item in template.templateitmes:
                t_i.add_item(item.attr_id)

        return g_i.get_as_complexmodel()

    @rpc(Template, _returns=Template)
    def add_resource_template(ctx, template):
        """
            Add a resource template
            A group_id may or may not be specified in the
            resource template complex model, as the template
            does not necessarily need to be in a group.
        """
        template_i = Template()

        #Check if the template is in a group  
        if hasattr('group_id', template) and\
           template.group_id is not None:
            template_i.db.group_id = template.group_id

        template_i.db.template_name = template.name
        template_i.save()

        for item in template.templateitmes:
            template_i.add_item(item.attr_id)

        return template_i.get_as_complexmodel()
 

    @rpc(Template, _returns=Template)
    def update_resource_template(ctx, template):
        """
            Add a resource template
        """
        template_i = HydraIface.Template(template_id=template.id)
       
        #Check if the template is in a group  
        if hasattr('group_id', template) and\
           template.group_id is not None:
            template_i.db.group_id = template.group_id

        for item in template.templateitmes:
            template_i.add_item(item.attr_id)

        return template_i.get_as_complexmodel()

    @rpc(Integer, Integer, _returns=Template)
    def remove_attr_from_template(ctx, template_id, attr_id):
        """

            Remove an attribute from a template
        """
        templateitem_i = HydraIface.TemplateItem(template_id=template_id, attr_id=attr_id)
        templateitem_i.delete()

    @rpc(Integer, String, Integer, _returns=Resource)
    def add_node_attrs_from_template(ctx, template_id, resource_type, resource_id):
        """
            adds all the attributes defined by a template to a node.
        """
        template_i = HydraIface.Template(template_id)
        template_i.load()

        resource_i = get_resource(resource_type, resource_id)

        for item in template_i.templateitems:
            resource_i.add_attribute(item.db.attr_id)

        return resource_i.get_as_complexmodel()
