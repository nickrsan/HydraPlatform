from spyne.service import ServiceBase
import logging
from HydraLib.HydraException import HydraError
from spyne.model.primitive import Integer, Boolean
from spyne.decorator import rpc
from hydra_complexmodels import Network, Node
from db import HydraIface

class NetworkService(ServiceBase):

    @rpc(Network, _returns=Network)
    def add_network(ctx, network):
        x = HydraIface.Network()
        logging.debug(network)
        x.db.project_id          = network.project_id
        x.db.network_name        = network.network_name
        x.db.network_description = network.network_description
        x.save()
        x.commit()
        network.network_id = x.db.network_id

        for link in network.links:
            l = x.add_link(link.link_name, link.link_description, link.node_1_id, link.node_2_id)
            l.save()
            l.commit()
            link.link_id = l.db.link_id

        return x.get_as_complexmodel()

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

        x.save()
        x.commit()
        return x.get_as_complexmodel()

    @rpc(Integer, _returns=Boolean)
    def delete_network(ctx, network_id):
        success = True
        try:
            x = HydraIface.Network(network_id = network_id)
            x.db.status = 'X'
            x.save()
            x.commit()
        except HydraError, e:
            logging.critical(e)
            success = False

        return success

    @rpc(Node, _returns=Node)
    def add_node(ctx, node):
        x = HydraIface.Node()
        x.db.node_name = node.node_name
        x.db.node_x    = node.node_x
        x.db.node_y    = node.node_y
        x.db.node_description = node.node_description
        x.save()
        x.commit()
        n = x.get_as_complexmodel()
        logging.debug("%s or %s", n.node_x, node.node_x)
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

        return success

    @rpc(Integer, _returns=Boolean) 
    def purge_node(ctx, node_id):
        x = HydraIface.Node(node_id = node_id)
        x.delete()
        x.save()
        x.commit()
        return x.load()

