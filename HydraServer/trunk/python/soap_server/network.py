from spyne.service import ServiceBase
import logging
from HydraLib.HydraException import HydraError
from spyne.model.primitive import Integer, Boolean
from spyne.decorator import rpc
from hydra_complexmodels import Network, Node, parse_value
from db import HydraIface
from HydraLib import hdb
import scenario

class NetworkService(ServiceBase):

    @rpc(Network, _returns=Network)
    def add_network(ctx, network):
        """
            Takes an entire network complex model and saves it to the DB.
            This complex model includes links & scenarios (with resource data).
            Returns the network's complex model.
        """
        x = HydraIface.Network()
        x.db.project_id          = network.project_id
        x.db.network_name        = network.network_name
        x.db.network_description = network.network_description
        x.save()
        x.commit()
        network.network_id = x.db.network_id
        
        logging.debug(network.links)
        for link in network.links:
            l = x.add_link(link.link_name, link.link_description, link.node_1_id, link.node_2_id)
            l.save()
            link.link_id = l.db.link_id

        if network.scenarios is not None:
            for s in network.scenarios:
                scen = HydraIface.Scenario()
                scen.db.scenario_name        = s.scenario_name
                scen.db.scenario_description = s.scenario_description
                scen.db.network_id           = x.db.network_id
                scen.save()

                for r_scen in s.resourcescenarios:
                   scenario._update_resourcescenario(scen.db.scenario_id, r_scen) 

        net = x.get_as_complexmodel()

        hdb.commit()

        return net

    @rpc(Integer, _returns=Network)
    def get_network(ctx, network_id):
        x = HydraIface.Network(network_id = network_id)

        net = x.get_as_complexmodel()

        return net

    @rpc(Network, _returns=Network)
    def update_network(ctx, network):
        x = HydraIface.Network(network_id = network.network_id)
        x.db.project_id          = network.project_id
        x.db.network_name        = network.network_name
        x.db.network_description = network.network_description

        for link in network.links:
            l = x.get_link(link.link_id)
            if l is None:
                l = x.add_link(link.link_name, link.link_description, link.node_1_id, link.node_2_id)
                l.save()
                l.commit()
                link.link_id = l.db.link_id
            else:
                l.db.link_name = link.link_name
                l.db.link_description = link.link_description
            l.save()
            l.commit()

        net = x.get_as_complexmodel()

        hdb.commit()
        
        return net

    @rpc(Integer, _returns=Boolean)
    def delete_network(ctx, network_id):
        """
            Deletes a network. This does not
            remove the network from the DB. It just
            sets the status to 'X', meaning it can no longer
            be seen by the user.
        """
        success = True
        try:
            x = HydraIface.Network(network_id = network_id)
            x.db.status = 'X'
            x.save()
            x.commit()
        except HydraError, e:
            logging.critical(e)
            success = False

        hdb.commit()
        return success

    @rpc(Node, _returns=Node)
    def add_node(ctx, node):
        """
            Add a node
        """
        x = HydraIface.Node()
        x.db.node_name = node.node_name
        x.db.node_x    = node.node_x
        x.db.node_y    = node.node_y
        x.db.node_description = node.node_description
        x.save()
        
        if node.attributes is not None:
            #ra is for ResourceAttr
            for ra in node.attributes:
                attr_is_var = 'N'
                if ra.attr_is_var is True:
                    attr_is_var = 'Y'
                x.add_attribute(ra.attr_id, attr_is_var)
        hdb.commit()
        return x.get_as_complexmodel()

    @rpc(Node, _returns=Node)
    def update_node(ctx, node):
        x = HydraIface.Node(node_id = node.node_id)
        x.db.node_name = node.node_name
        x.db.node_x    = node.node_x
        x.db.node_y    = node.node_y
        x.db.node_description = node.node_description
        x.save()
        x.commit()
        hdb.commit()
        return x.get_as_complexmodel()

    @rpc(Integer, _returns=Node)
    def delete_node(ctx, node_id):
        success = True
        try:
            x = HydraIface.Node(node_id = node_id)
            x.db.status = 'X'
            x.save()
            x.commit()
        except HydraError, e:
            logging.critical(e)
            success = False

        hdb.commit()
        return success

    @rpc(Integer, _returns=Boolean)
    def purge_node(ctx, node_id):
        x = HydraIface.Node(node_id = node_id)
        x.delete()
        x.save()
        x.commit()
        hdb.commit()
        return x.load()

