
from spyne.model.complex import Array as SpyneArray
from spyne.model.primitive import String, Integer
from spyne.decorator import rpc
from hydra_complexmodels import ResourceTemplate, ResourceTemplateGroup, ResourceTemplateItem

from db import HydraIface
from hydra_base import HydraService
from HydraLib.HydraException import HydraError
import logging

class TemplateService(HydraService):
    """
        The template SOAP service
    """

    @rpc(ResourceTemplateGroup, _returns=ResourceTemplateGroup)
    def add_resourcetemplategroup(ctx, group):
        """
            Add group and a template and items.
        """
        grp_i = HydraIface.ResourceTemplateGroup()
        grp_i.db.group_name = group.name

        grp_i.save()
        
        for template in group.resourcetemplates:
            rt_i = grp_i.add_template(template.name)
            for item in template.resourcetemplateitems:
                rt_i.add_item(item.attr_id)

        return grp_i.get_as_complexmodel()

    @rpc(ResourceTemplateGroup, _returns=ResourceTemplateGroup)
    def update_resourcetemplategroup(ctx, group):
        """
            Update group and a template and items.
        """
        grp_i = HydraIface.ResourceTemplateGroup(group_id=group.id)
        grp_i.db.group_name = group.name
        grp_i.load_all()

        for template in group.resourcetemplates:
            if template.id is not None:
                for tmpl_i in grp_i.resourcetemplates:
                    if tmpl_i.db.template_id == template.id:
                        tmpl_i.db.template_name = template.name
                        tmpl_i.save()
                        tmpl_i.load_all()
                        break
            else:
                tmpl_i = grp_i.add_template(template.name)

            for item in template.resourcetemplateitems:
                for item_i in tmpl_i.resourcetemplateitems:
                    if item_i.db.attr_id == item.attr_id:
                        break
                else:
                    tmpl_i.add_item(item.attr_id)
                    tmpl_i.save()

        grp_i.commit()
        return grp_i.get_as_complexmodel()

    @rpc(_returns=SpyneArray(ResourceTemplateGroup))
    def get_resourcetemplategroups(ctx):
        """
            Get all resource template groups.
        """

        groups = []
        
        sql = """
            select
                group_id
            from
                tResourceTemplateGroup
        """

        rs = HydraIface.execute(sql)
        
        for r in rs:
            grp_i = HydraIface.ResourceTemplateGroup(group_id=r.group_id)
            grp_i.load_all()
            grp = grp_i.get_as_complexmodel()
            groups.append(grp)

        return groups

    @rpc(Integer, _returns=ResourceTemplateGroup)
    def get_resourcetemplategroup(ctx, group_id):
        """
            Get a specific resource template group, either by ID or name.   
        """
        
        grp_i = HydraIface.ResourceTemplateGroup(group_id=group_id)
        grp_i.load_all()
        grp = grp_i.get_as_complexmodel()

        return grp

    @rpc(String, _returns=ResourceTemplateGroup)
    def get_resourcetemplategroup_by_name(ctx, name):
        """
            Get a specific resource template group, either by ID or name.   
        """
        sql = """
            select
                group_id
            from
                tResourceTemplateGroup
            where
                group_name = '%s'
        """ % name

        rs = HydraIface.execute(sql)

        if len(rs) != 1:
            raise HydraError("%s is not a valid identifier for a group"%(name))

        group_id = rs[0].group_id

        grp_i = HydraIface.ResourceTemplateGroup(group_id=group_id)
        grp_i.load_all()
        grp = grp_i.get_as_complexmodel()

        return grp

    @rpc(ResourceTemplate, _returns=ResourceTemplate)
    def add_resourcetemplate(ctx, template):
        """
            Add a resource template with items.
        """
        rt_i = HydraIface.ResourceTemplate()
        rt_i.db.template_name  = template.name
        
        if template.group_id is not None:
            rt_i.db.group_id = template.group_id
        else:
            rt_i.db.group_id = 1 # 1 is always the default group
       
        rt_i.save()

        for item in template.resourcetemplateitems:
            rt_i.add_item(item.attr_id)

        return rt_i.get_as_complexmodel()

    @rpc(ResourceTemplate, _returns=ResourceTemplate)
    def update_resourcetemplate(ctx, template):
        """
            Update a resource template and its items.
            New items will be added. Items not sent will be ignored.
            To delete items, call delete_resourcetemplateitem
        """
        rt_i = HydraIface.ResourceTemplate(template_id = template.id)
        rt_i.db.template_name  = template.name
        rt_i.load_all()

        if template.group_id is not None:
            rt_i.db.group_id = template.group_id
        else:
            rt_i.db.group_id = 1 # 1 is always the default group

        for item in template.resourcetemplateitems:
            for item_i in rt_i.resourcetemplateitems:
                if item_i.db.attr_id == item.attr_id:
                    break
            else:
                rt_i.add_item(item.attr_id)
        
        rt_i.save()

        return rt_i.get_as_complexmodel()

    @rpc(Integer, _returns=ResourceTemplate)
    def get_resourcetemplate(ctx, template_id):
        """
            Get a specific resource template by ID.   
        """
        tmpl_i = HydraIface.ResourceTemplate(template_id=template_id)
        tmpl_i.load_all()
        tmpl = tmpl_i.get_as_complexmodel()

        return tmpl

    @rpc(Integer, String, _returns=ResourceTemplate)
    def get_resourcetemplate_by_name(ctx, group_id, template_name):
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
                tResourceTemplate tmpl,
                tResourceTemplateGroup grp
            where
                grp.group_id = %s
            and tmpl.group_id = grp.group_id
            and tmpl.template_name = '%s'
        """ % (group_id, template_name)

        rs = HydraIface.execute(sql)

        if len(rs) != 1:
            raise HydraError("%s is not a valid identifier for a template"%(template_name))

        template_id = rs[0].template_id

        tmpl_i = HydraIface.ResourceTemplate(template_id=template_id)
        tmpl_i.load_all()
        tmpl = tmpl_i.get_as_complexmodel()

        return tmpl

    @rpc(ResourceTemplateItem, _returns=ResourceTemplate)
    def add_resourcetemplateitem(ctx, item):
        """
            Add an item to an existing template.
        """
        rt_i = HydraIface.ResourceTemplate(template_id = item.template_id)
        rt_i.load_all()
        rt_i.add_item(item.attr_id) 
        rt_i.save()
        return rt_i.get_as_complexmodel()

  
    @rpc(ResourceTemplateItem, _returns=ResourceTemplate)
    def delete_resourcetemplateitem(ctx, item):
        """
            Remove an item from an existing template
        """
        rt_i = HydraIface.ResourceTemplate(template_id = item.template_id)
 
        rt_i.load_all()
        rt_i.remove_item(item.attr_id) 
        rt_i.save()

        return rt_i.get_as_complexmodel()

