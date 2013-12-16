
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import Template, TemplateGroup, TemplateItem

from db import HydraIface
from hydra_base import HydraService

class TemplateService(HydraService):
    """
        The template SOAP service
    """

    @rpc(_returns=SpyneArray(TemplateGroup))
    def get_groups(ctx):
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
            grp_i.load()
            groups.append(grp_i)

        return groups

    @rpc(Template, _returns=Template)
    def add_template(ctx, template):
        """
            Add a resource template with items.
        """
        rt_i = HydraIface.Template()
        rt_i.db.template_name  = template.name
        
        if hasattr(template, 'group_id') and template.group_id is not None:
            rt_i.db.group_id = template.group_id
       
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

        if hasattr(template, 'group_id') and template.group_id is not None:
            rt_i.db.group_id = template.group_id

        for item in template.templateitems:
            for item_i in rt_i.templateitems:
                if item_i.db.attr_id == item.attr_id:
                    break
            else:
                rt_i.add_item(item.attr_id)
        
        rt_i.save()

        return rt_i.get_as_complexmodel()


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
            if hasattr(template, 'id') and template.id is not None:
                tmpl = HydraIface.Template(template_id=template.id)
                tmpl.db.name = template.name
            else:
                rt_i = grp_i.add_template(template.name)

            for item in template.templateitems:
                for item_i in rt_i.templateitems:
                    if item_i.db.attr_id == item.attr_id:
                        break
                else:
                    rt_i.add_item(item.attr_id)

        return grp_i.get_as_complexmodel()

