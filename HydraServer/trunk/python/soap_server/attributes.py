from spyne.model.primitive import Integer, Boolean, String
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import Attr
from db import HydraIface
from hydra_complexmodels import ResourceTemplateGroup,\
        ResourceTemplate, \
        Resource

from hydra_base import HydraService
from spyne.service import ServiceBase
from HydraLib.HydraException import HydraError

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
        return x.get_as_complexmodel()

    @rpc(SpyneArray(Attr), _returns=SpyneArray(Attr))
    def add_attributes(ctx, attrs):
        """
            Add a generic attribute, which can then be used in creating
            a resource attribute, and put into a template.
            (Attr){
                id = 1020
                name = "Test Attr"
                dimen = "very big"
            }

        """

        #Check to see if any of the attributs being added are already there.
        #If they are there already, don't add a new one. If an attribute
        #with the same name is there already but with a different dimension,
        #add a new attribute.
        sql = """
            select
                attr_id,
                attr_name,
                attr_dimen
            from
                tAttr
        """

        rs = HydraIface.execute(sql)

        attrs_to_add = []
        for potential_new_attr in attrs:
            for r in rs:
                if potential_new_attr.name == r.attr_name and \
                   potential_new_attr.dimen == r.attr_dimen:
                    #raise HydraError("Attribute %s already exists but "
                    #                    "with a different dimension: %s",\
                    #                    r.attr_name, r.attr_dimen)
                    break
            else:
                attrs_to_add.append(potential_new_attr)

        iface_attrs = []
        for attr in attrs_to_add:
            x = HydraIface.Attr()
            x.db.attr_name = attr.name
            x.db.attr_dimen = attr.dimen
            iface_attrs.append(x)

        HydraIface.bulk_insert(iface_attrs, 'tAttr')

        sql = """
            select
                attr_id,
                attr_name,
                attr_dimen
            from
                tAttr
        """

        rs = HydraIface.execute(sql)

        all_attrs = []
        for r in rs:
            x = Attr()
            x.name  = r.attr_name
            x.dimen = r.attr_dimen
            x.id    = r.attr_id
            all_attrs.append(x)

        new_attrs = []
        for attr in all_attrs:
            for new_attr in attrs:
                if new_attr.name == attr.name and new_attr.dimen == attr.dimen:
                    new_attrs.append(attr)
                    break

        return new_attrs

    @rpc(_returns=SpyneArray(Attr))
    def get_attributes(ctx):
        """
            Get all attributes
        """


        sql = """
            select
                attr_id,
                attr_name,
                attr_dimen
            from
                tAttr
        """

        rs = HydraIface.execute(sql)

        attrs = []
        for r in rs:
            x = Attr()
            x.name  = r.attr_name
            x.dimen = r.attr_dimen
            x.id    = r.attr_id
            attrs.append(x)

        return attrs

    @rpc(String, _returns=Attr)
    def get_attribute(ctx, name):
        """
            Get a specific attribute by its name or by its ID.
        """

        try:
            ID = int(name)
            sql = """
            select
                attr_id,
                attr_name,
                attr_dimen
            from
                tAttr
            where
                attr_id = %s
            """ % ID
        except ValueError:

            sql = """
                select
                    attr_id,
                    attr_name,
                    attr_dimen
                from
                    tAttr
                where
                    attr_name = '%s'
            """ % name

        rs = HydraIface.execute(sql)

        if len(rs) == 0:
           return None
        else:
            x = Attr()
            x.name  = rs[0].attr_name
            x.dimen = rs[0].attr_dimen
            x.id    = rs[0].attr_id
            return x


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

    @rpc(ResourceTemplateGroup, _returns=ResourceTemplateGroup)
    def add_template_group(ctx, group):
        """
            Add a template group
        """

        g_i = HydraIface.ResourceTemplateGroup()
        g_i.db.group_name = group.name

        g_i.save()

        for template in group.resourcetemplates:
            t_i = g_i.add_template(template.name)
            for item in template.resourcetemplateitmes:
                t_i.add_item(item.attr_id)

        return g_i.get_as_complexmodel()

    @rpc(ResourceTemplateGroup, _returns=ResourceTemplateGroup)
    def update_template_group(ctx, group):
        """
            Add a template group
        """
        g_i = HydraIface.ResourceTemplateGroup(group_id=group.id)
        g_i.db.group_name = group.name

        g_i.save()

        for template in group.resourcetemplates:
            t_i = g_i.add_template(template.name)
            for item in template.resourcetemplateitmes:
                t_i.add_item(item.attr_id)

        return g_i.get_as_complexmodel()

    @rpc(ResourceTemplate, _returns=ResourceTemplate)
    def add_resource_template(ctx, resourcetemplate):
        """
            Add a resource template
            A group_id may or may not be specified in the
            resource template complex model, as the template
            does not necessarily need to be in a group.
        """
        template_i = ResourceTemplate()

        #Check if the resourcetemplate is in a group
        if hasattr('group_id', resourcetemplate) and\
           resourcetemplate.group_id is not None:
            template_i.db.group_id = resourcetemplate.group_id

        template_i.db.template_name = resourcetemplate.name
        template_i.save()

        for item in resourcetemplate.resourcetemplateitmes:
            template_i.add_item(item.attr_id)

        return template_i.get_as_complexmodel()


    @rpc(ResourceTemplate, _returns=ResourceTemplate)
    def update_resource_template(ctx, resourcetemplate):
        """
            Add a resource template
        """
        template_i = HydraIface.ResourceTemplate(template_id=resourcetemplate.id)

        #Check if the resourcetemplate is in a group
        if hasattr('group_id', resourcetemplate) and\
           resourcetemplate.group_id is not None:
            template_i.db.group_id = resourcetemplate.group_id

        for item in resourcetemplate.resourcetemplateitmes:
            template_i.add_item(item.attr_id)

        return template_i.get_as_complexmodel()

    @rpc(Integer, Integer, _returns=ResourceTemplate)
    def remove_attr_from_template(ctx, template_id, attr_id):
        """

            Remove an attribute from a resourcetemplate
        """
        templateitem_i = HydraIface.ResourceTemplateItem(template_id=template_id, attr_id=attr_id)
        templateitem_i.delete()

    @rpc(Integer, String, Integer, _returns=Resource)
    def add_node_attrs_from_template(ctx, template_id, resource_type, resource_id):
        """
            adds all the attributes defined by a template to a node.
        """
        template_i = HydraIface.ResourceTemplate(template_id)
        template_i.load()

        resource_i = get_resource(resource_type, resource_id)

        for item in template_i.resourcetemplateitems:
            resource_i.add_attribute(item.db.attr_id)

        return resource_i.get_as_complexmodel()
