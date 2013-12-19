from spyne.model.primitive import Integer, String
from spyne.decorator import rpc
from db import HydraIface
from hydra_complexmodels import ResourceGroup, ResourceGroupItem, Scenario
from HydraLib.HydraException import HydraError
from hydra_base import HydraService

class ResourceGroupService(HydraService):
    """
        The resource group soap service
    """

    @rpc(ResourceGroup, Integer, _returns=ResourceGroup)
    def add_resourcegroup(ctx, group, network_id):
        """
            Add a new group to a network.
        """
        group_i                = HydraIface.ResourceGroup()
        group_i.db.group_name        = group.name
        group_i.db.group_description = group.description
        group_i.db.status            = group.status
        group_i.db.network_id        = network_id

        group_i.save()

        return group_i.get_as_complexmodel()

    @rpc(Integer, ResourceGroup, _returns=String)
    def delete_resourcegroup(ctx, group_id):
        """
            Add a new group to a scenario.
        """
        group_i = HydraIface.ResourceGroup(group_id=group_id)

        for item in group_i.resourcegroupitems:
            item.delete()
            item.save()

        group_i.delete()
        group_i.save()

        return 'OK'

    @rpc(ResourceGroup, _returns=ResourceGroup)
    def update_resourcegroup(ctx, group):
        """
            Add a new group to a network.
        """

        group_i                = HydraIface.ResourceGroup(group_id=group.id)
        group_i.db.group_name        = group.name
        group_i.db.group_description = group.description
        group_i.db.status            = group.status

        group_i.save()

        return group_i.get_as_complexmodel()


    @rpc(ResourceGroupItem, Integer, _returns=Scenario)
    def add_resourcegroupitem(ctx, group_item, scenario_id):
        #Check whether the ref_id is correct.
        sql = """
            select
                n.node_id,
                l.link_id
            from
                tScenario scen,
                tNetwork net
                left join tLink l on (
                    l.network_id=net.network_id
                and l.link_id   = %(ref_id)s
                )
                left join tNode n on (
                    n.network_id =net.network_id
                and n.node_id    = %(ref_id)s 
                )
            where
                scen.scenario_id = %(scenario_id)s
            and scen.network_id = net.network_id
        """ % dict(ref_id=group_item.ref_id, scenario_id=scenario_id)

        rs = HydraIface.execute(sql)

        if len(rs) != 1:
            raise HydraError("Invalid ref ID for group item!")

        if group_item.ref_key == 'NODE' and rs[0].node_id is None:
            raise HydraError("Invalid ref ID for group item!")

        if group_item.ref_key == 'LINK' and rs[0].link_id is None:
            raise HydraError("Invalid ref ID for group item!")

        group_item_i                = HydraIface.ResourceGroupItem()
        group_item_i.db.scenario_id = scenario_id
        group_item_i.db.group_id    = group_item.group_id

        group_item_i.db.ref_key = group_item.ref_key
        group_item_i.db.ref_id = group_item.ref_id
        group_item_i.save()
       
        return group_item_i.get_as_complexmodel()

    @rpc(Integer, _returns=String)
    def delete_resourcegroupitem(ctx, item_id):
        group_item_i = HydraIface.ResourceGroupItem(item_id=item_id)
        group_item_i.delete()
        group_item_i.save()
       
        return 'OK'
