
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import ResourceTemplate, ResourceTemplateGroup, ResourceTemplateItem

from db import HydraIface
from hydra_base import HydraService

class TemplateService(HydraService):
    """
        The template SOAP service
    """

    @rpc(_returns=SpyneArray(ResourceTemplateGroup))
    def get_groups(ctx):
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
            grp_i.load()
            groups.append(grp_i)

        return groups

    @rpc(ResourceTemplate, _returns=ResourceTemplate)
    def add_resourcetemplate(ctx, template):
        """
            Add a resource template with items.
        """
        rt_i = HydraIface.ResourceTemplate()
        rt_i.db.template_name  = template.name
        
        if hasattr(template, 'group_id') and template.group_id is not None:
            rt_i.db.group_id = template.group_id
       
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

        if hasattr(template, 'group_id') and template.group_id is not None:
            rt_i.db.group_id = template.group_id

        for item in template.resourcetemplateitems:
            for item_i in rt_i.resourcetemplateitems:
                if item_i.db.attr_id == item.attr_id:
                    break
            else:
                rt_i.add_item(item.attr_id)
        
        rt_i.save()

        return rt_i.get_as_complexmodel()


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

