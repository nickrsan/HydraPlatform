from spyne.service import ServiceBase
import logging
from HydraLib.HydraException import HydraError
from spyne.model.primitive import Integer, Boolean
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import Network, Node, Link
from db import HydraIface
from HydraLib import hdb
import scenario
import sys, traceback

class NetworkService(ServiceBase):
    """
        The network SOAP service.
    """

    @rpc(Network, _returns=Network)
    def add_network(ctx, network):
        """
            Takes an entire network complex model and saves it to the DB.
            This complex model includes links & scenarios (with resource data).
            Returns the network's complex model.
        """
        net = None
        try:
            x = HydraIface.Network()
            x.db.project_id          = network.project_id
            x.db.network_name        = network.network_name
            x.db.network_description = network.network_description
            x.save()
            x.commit()
            network.network_id = x.db.network_id

             #First add all the nodes
            for node in network.nodes:
                n = x.add_node(node.node_name, node.node_description, node.node_x, node.node_y)
                n.save()
                node.node_id = n.db.node_id

            #Then add all the links.
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
        except Exception, e:
            logging.critical(e)
            traceback.print_exc(file=sys.stdout)
            hdb.rollback()

        return net

    @rpc(Integer, _returns=Network)
    def get_network(ctx, network_id):
        """
            Return a whole network as a complex model.
        """
        x = HydraIface.Network(network_id = network_id)

        net = x.get_as_complexmodel()

        return net

    @rpc(Network, _returns=Network)
    def update_network(ctx, network):
        """
            Update an entire network, excluding nodes.
        """
        net = None
        try:
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
                    l.db.link_descripion = link.link_description
                l.save()
                l.commit()

            if network.scenarios is not None:
                for s in network.scenarios:
                    if hasattr('scenrio_id', s):
                        scen = HydraIface.Scenario(scenario_id=s.scenario_id)
                    else:
                        scen = HydraIface.Scenario()
                    scen.db.scenario_name        = s.scenario_name
                    scen.db.scenario_description = s.scenario_description
                    scen.db.network_id           = x.db.network_id
                    scen.save()

                    for r_scen in s.resourcescenarios:
                       scenario._update_resourcescenario(scen.db.scenario_id, r_scen) 

            net = x.get_as_complexmodel()

            hdb.commit()
        except Exception, e:
            logging.critical(e)
            hdb.rollback()

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
            x.rollback()

        hdb.commit()
        return success

    @rpc(Network, Network, SpyneArray(Node), _returns=Boolean)
    def join_networks(ctx, network_1, network_2, joining_nodes):
        """
            Given two networks and a set of nodes, return
            a new network which is a combination of both networks, with
            the nodes as the joining nodes.
            
        """
        pass

    @rpc(Node, _returns=Node)
    def add_node(ctx, node):
        """
            Add a node
            (Node){
               node_id = 1027
               node_name = "Node 1"
               node_description = "Node Description"
               node_x = 0.0
               node_y = 0.0
               attributes = 
                  (ResourceAttrArray){
                     ResourceAttr[] = 
                        (ResourceAttr){
                           attr_id = 1234
                        },
                        (ResourceAttr){
                           attr_id = 4321
                        },
                  }
             }
        """
        node = None
        try:
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
            node = x.get_as_complexmodel()
        except Exception, e:
            logging.critical(e)
            hdb.rollback()

        return node

    @rpc(Node, _returns=Node)
    def update_node(ctx, node):
        """
            Update a node.
            If new attributes are present, they will be added to the node.
            The non-presence of attributes does not remove them.

            (Node){
               node_id = 1039
               node_name = "Node 1"
               node_description = "Node Description"
               node_x = 0.0
               node_y = 0.0
               status = "A"
               attributes = 
                  (ResourceAttrArray){
                     ResourceAttr[] = 
                        (ResourceAttr){
                           resource_attr_id = 850
                           attr_id = 1038
                           ref_id = 1039
                           ref_key = "NODE"
                           attr_is_var = True
                        },
                        (ResourceAttr){
                           resource_attr_id = 852
                           attr_id = 1040
                           ref_id = 1039
                           ref_key = "NODE"
                           attr_is_var = True
                        },
                  }
             }

        """
        node = None
        try:
            x = HydraIface.Node(node_id = node.node_id)
            x.db.node_name = node.node_name
            x.db.node_x    = node.node_x
            x.db.node_y    = node.node_y
            x.db.node_description = node.node_description
            
            if node.attributes is not None:
                #ra is for ResourceAttr
                for ra in node.attributes:
                    ra_i = HydraIface.ResourceAttr(resource_attr_id = ra.resource_attr_id)
                    ra_i

            x.save()
            x.commit()
            hdb.commit()
            node = x.get_as_complexmodel()
        except Exception, e:
            logging.critical(e)
            hdb.rollback()

        return node

    @rpc(Integer, _returns=Boolean)
    def delete_resourceattr(node_id, resource_attr_id):
        """
            Deletes a resource attribute and all associated data.

        """
        try:
            pass
        except Exception, e:
            logging.critical(e)
            hdb.rollback()


    @rpc(Integer, _returns=Node)
    def delete_node(ctx, node_id):
        """
            Set the status of a node to 'X'
        """
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
        """
            Remove node from DB completely
        """
        x = HydraIface.Node(node_id = node_id)
        try:
            x.delete()
            x.save()
            x.commit()
            hdb.commit()
        except Exception, e:
            logging.critical(e)
            hdb.rollback()
        return x.load()

    @rpc(Link, _returns=Link)
    def add_link(ctx, link):
        """
            Add a link
        """
        link = None
        try:
            x = HydraIface.Link()
            x.db.link_name = link.link_name
            x.db.node_1_id = link.node_1_id
            x.db.node_2_id = link.node_2_id
            x.db.link_description = link.link_description
            x.save()
        
            if link.attributes is not None:
                #ra is for ResourceAttr
                for ra in link.attributes:
                    attr_is_var = 'N'
                    if ra.attr_is_var is True:
                        attr_is_var = 'Y'
                    x.add_attribute(ra.attr_id, attr_is_var)
            hdb.commit()
            link = x.get_as_complexmodel()
        except Exception, e:
            logging.critical(e)
            hdb.rollback()
        return link 

    @rpc(Link, _returns=Link)
    def update_link(ctx, link):
        """
            Update a link.
        """
        try:
            x = HydraIface.Link(link_id = link.link_id)
            x.db.link_name = link.link_name
            x.db.node_1_id = link.node_1_id
            x.db.node_2_id = link.node_2_id
            x.db.link_description = link.link_description
            x.save()
            x.commit()
            hdb.commit()
            return x.get_as_complexmodel()
        except Exception, e:
            logging.critical(e)
            hdb.rollback()
            return None

    @rpc(Integer, _returns=Link)
    def delete_link(ctx, link_id):
        """
            Set the status of a link to 'X'
        """
        success = True
        try:
            x = HydraIface.Link(link_id = link_id)
            x.db.status = 'X'
            x.save()
            x.commit()
            hdb.commit()
        except HydraError, e:
            logging.critical(e)
            hdb.rollback()
            success = False

        return success

    @rpc(Integer, _returns=Boolean)
    def purge_link(ctx, link_id):
        """
            Remove link from DB completely
        """
        try:
            x = HydraIface.Link(link_id = link_id)
            x.delete()
            x.save()
            x.commit()
            hdb.commit()
            return x.load()
        except Exception, e:
            logging.critical(e)
            hdb.rollback()
            return None

