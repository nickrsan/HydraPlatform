import logging
from HydraLib.HydraException import HydraError
from spyne.model.primitive import Integer, Boolean
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import Network, Node, Link, Scenario
from db import HydraIface
from HydraLib import hdb
from hydra_base import HydraService
import scenario
import datetime

def _add_attributes(resource_i, attributes):
    resource_attr_id_map = dict()
    if attributes is None:
        return dict()
    #ra is for ResourceAttr
    for ra in attributes:
        attr_is_var = 'N'
        if ra.is_var is True:
            attr_is_var = 'Y'

        if ra.id < 0:
            ra_i = resource_i.add_attribute(ra.attr_id, attr_is_var)
        else:
            ra = HydraIface.ResourceAttr(resource_attr_id=ra.id)
            ra.db.attr_is_var = attr_is_var

    HydraIface.bulk_insert(resource_i.attributes, 'tResourceAttr')

    for ra_i in resource_i.get_attributes():
        for ra in attributes:
            if ra.attr_id == ra_i.db.attr_id:
                resource_attr_id_map[ra.id] = ra_i.db.resource_attr_id

    return resource_attr_id_map

class NetworkService(HydraService):
    """
        The network SOAP service.
    """

    @rpc(Network, _returns=Network)
    def add_network(ctx, network):
        """
            Takes an entire network complex model and saves it to the DB.
            This complex model includes links & scenarios (with resource data).
            Returns the network's complex model.

            As links connect two nodes using the node_ids, if the nodes are new
            they will not yet have node_ids. In this case, use negative ids
            as temporary IDS until the node has been given an permanent ID.

            All inter-object referencing of new objects
            should be done using negative IDs in the client.

            The returned object will have positive IDS

        """
        logging.info("Adding network")

        return_value = None
        insert_start = datetime.datetime.now()

        x = HydraIface.Network()
        x.db.project_id          = network.project_id
        x.db.network_name        = network.name
        x.db.network_description = network.description
        x.save()
        network.network_id = x.db.network_id

        resource_attr_id_map = _add_attributes(x, network.attributes)

        #Maps temporary node_ids to real node_ids
        node_ids = dict()

         #First add all the nodes
        for node in network.nodes:
            logging.info("Adding nodes to network")
            n = x.add_node(node.name, node.description, node.x, node.y)
            
        HydraIface.bulk_insert(x.nodes, 'tNode')
        x.load_all()

        for n in x.nodes:
            for node in network.nodes:
                if node.x == n.db.node_x and node.y == n.db.node_y and n.db.node_name == node.name:
                    node_attr_id_map = _add_attributes(n, node.attributes)
                    resource_attr_id_map.update(node_attr_id_map)
                    #If a temporary ID was given to the node
                    #store the mapping to the real node_id
                    if node.id is not None and node.id <= 0:
                            node_ids[node.id] = n.db.node_id
                            continue


        #Then add all the links.
        for link in network.links:
            logging.info("Adding links to network")
            node_1_id = link.node_1_id
            if link.node_1_id in node_ids:
                node_1_id = node_ids[link.node_1_id]

            node_2_id = link.node_2_id
            if link.node_2_id in node_ids:
                node_2_id = node_ids[link.node_2_id]

            if node_1_id is None or node_2_id is None:
                raise HydraError("Node IDS (%s, %s)are incorrect!"%(node_1_id, node_2_id))

            l = x.add_link(link.name, link.description, node_1_id, node_2_id)
            link_attr_id_map = _add_attributes(l, link.attributes)
            resource_attr_id_map.update(link_attr_id_map)

        if network.scenarios is not None:
            logging.info("Adding scenarios to network")
            for s in network.scenarios:
                scen = HydraIface.Scenario()
                scen.db.scenario_name        = s.name
                scen.db.scenario_description = s.description
                scen.db.network_id           = x.db.network_id
                scen.save()

                for r_scen in s.resourcescenarios:
                    r_scen.resource_attr_id = resource_attr_id_map[r_scen.resource_attr_id]
                    scenario._update_resourcescenario(scen.db.scenario_id, r_scen, new=True)
                x.scenarios.append(scen)
        
        logging.info("Insertion of network took: %s",(datetime.datetime.now()-insert_start))
        return_value = x.get_as_complexmodel()

        return return_value


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
        x = HydraIface.Network(network_id = network.id)
        x.load_all()
        x.db.project_id          = network.project_id
        x.db.network_name        = network.name
        x.db.network_description = network.description

        resource_attr_id_map = _add_attributes(x, network.attributes)

        #Maps temporary node_ids to real node_ids
        node_id_map = dict()

        #First add all the nodes
        for node in network.nodes:
            is_new = False

            #If we get a negative or null node id, we know
            #it is a new node.
            if node.id is not None and node.id > 0:
                n = x.get_node(node.id)
                n.db.node_name        = node.name
                n.db.node_description = node.description
                n.db.node_x           = node.x
                n.db.node_y           = node.y
                n.db.status           = node.status
            else:
                is_new = True
                n = x.add_node(node.name, node.description, node.x, node.y)
            n.save()

            node_attr_id_map = _add_attributes(n, node.attributes)
            resource_attr_id_map.update(node_attr_id_map)

            #If a temporary ID was given to the node
            #store the mapping to the real node_id
            if is_new is True:
                node_id_map[node.id] = n.db.node_id


        for link in network.links:
            node_1_id = link.node_1_id
            if link.node_1_id in node_id_map:
                node_1_id = node_id_map[link.node_1_id]

            node_2_id = link.node_2_id
            if link.node_2_id in node_id_map:
                node_2_id = node_id_map[link.node_2_id]

            if link.id is None or link.id < 0:
                l = x.add_link(link.name, link.description, node_1_id, node_2_id)
            else:
                l = x.get_link(link.id)
                l.load()
                l.db.link_name       = link.name
                l.db.link_descripion = link.description

            l.save()

            link_attr_id_map = _add_attributes(l, link.attributes)
            resource_attr_id_map.update(link_attr_id_map)


        if network.scenarios is not None:
            for s in network.scenarios:
                if s.id is not None and s.id > 0:
                    scen = HydraIface.Scenario(scenario_id=s.id)

                else:
                    scen = HydraIface.Scenario()
                scen.db.scenario_name        = s.name
                scen.db.scenario_description = s.description
                scen.db.network_id           = x.db.network_id
                scen.save()

                for r_scen in s.resourcescenarios:
                    r_scen.resource_attr_id = resource_attr_id_map[r_scen.resource_attr_id]

                    scenario._update_resourcescenario(scen.db.scenario_id, r_scen)

        net = x.get_as_complexmodel()

        hdb.commit()

        return net

    @rpc(Integer, Boolean, _returns=Boolean)
    def delete_network(ctx, network_id, purge_data):
        """
            Deletes a network. This does not
            remove the network from the DB. It just
            sets the status to 'X', meaning it can no longer
            be seen by the user.
        """
        success = True
        x = HydraIface.Network(network_id = network_id)
        x.db.status = 'X'
        x.save()
        x.commit()

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

    @rpc(Integer, Node, _returns=Node)
    def add_node(ctx, network_id, node):
        """
            Add a node
            (Node){
               id = 1027
               name = "Node 1"
               description = "Node Description"
               x = 0.0
               y = 0.0
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

        x = HydraIface.Node()
        x.db.network_id = network_id
        x.db.node_name = node.name
        x.db.node_x    = node.x
        x.db.node_y    = node.y
        x.db.node_description = node.description
        x.save()

        _add_attributes(x, node.attributes)

        hdb.commit()
        node = x.get_as_complexmodel()

        return node

    @rpc(Node, _returns=Node)
    def update_node(ctx, node):
        """
            Update a node.
            If new attributes are present, they will be added to the node.
            The non-presence of attributes does not remove them.

            (Node){
               id = 1039
               name = "Node 1"
               description = "Node Description"
               x = 0.0
               y = 0.0
               status = "A"
               attributes =
                  (ResourceAttrArray){
                     ResourceAttr[] =
                        (ResourceAttr){
                           id = 850
                           attr_id = 1038
                           ref_id = 1039
                           ref_key = "NODE"
                           attr_is_var = True
                        },
                        (ResourceAttr){
                           id = 852
                           attr_id = 1040
                           ref_id = 1039
                           ref_key = "NODE"
                           attr_is_var = True
                        },
                  }
             }

        """
        node = None

        x = HydraIface.Node(node_id = node.id)
        x.db.node_name = node.name
        x.db.node_x    = node.x
        x.db.node_y    = node.y
        x.db.node_description = node.description

        _add_attributes(x, node.attributes)

        x.save()
        x.commit()
        hdb.commit()
        node = x.get_as_complexmodel()

        return node

    @rpc(Integer, Boolean,  _returns=Boolean)
    def delete_resourceattr(ctx, resource_attr_id, purge_data):
        """
            Deletes a resource attribute and all associated data.


        """
        success = True
        ra = HydraIface.ResourceAttr(resource_attr_id = resource_attr_id)
        ra.load()
        ra.delete(purge_data)

        return success


    @rpc(Integer, _returns=Node)
    def delete_node(ctx, node_id):
        """
            Set the status of a node to 'X'
        """
        success = True
        x = HydraIface.Node(node_id = node_id)
        x.db.status = 'X'
        x.save()
        x.commit()

        hdb.commit()
        return success

    @rpc(Integer, Boolean, _returns=Boolean)
    def purge_node(ctx, node_id, purge_data):
        """
            Remove node from DB completely
            If there are attributes on the node, use purge_data to try to
            delete the data. If no other resources link to this data, it
            will be deleted.

        """
        x = HydraIface.Node(node_id = node_id)
        x.delete(purge_data=purge_data)
        x.save()
        x.commit()
        return x.load()

    @rpc(Integer, Link, _returns=Link)
    def add_link(ctx, network_id, link):
        """
            Add a link
        """
        link = None

        x = HydraIface.Link()
        x.db.network_id = network_id
        x.db.link_name = link.name
        x.db.node_1_id = link.node_1_id
        x.db.node_2_id = link.node_2_id
        x.db.link_description = link.description
        x.save()

        _add_attributes(x, link.attributes)
        x.commit()

        link = x.get_as_complexmodel()

        return link

    @rpc(Link, _returns=Link)
    def update_link(ctx, link):
        """
            Update a link.
        """
        x = HydraIface.Link(link_id = link.id)
        x.db.link_name = link.name
        x.db.node_1_id = link.node_1_id
        x.db.node_2_id = link.node_2_id
        x.db.link_description = link.description

        _add_attributes(x, link.attributes)

        x.save()
        x.commit()
        return x.get_as_complexmodel()

    @rpc(Integer, _returns=Link)
    def delete_link(ctx, link_id):
        """
            Set the status of a link to 'X'
        """
        success = True
        x = HydraIface.Link(link_id = link_id)
        x.db.status = 'X'
        x.save()
        x.commit()

        return success

    @rpc(Integer, Boolean, _returns=Boolean)
    def purge_link(ctx, link_id, purge_data):
        """
            Remove link from DB completely
            If there are attributes on the link, use purge_data to try to
            delete the data. If no other resources link to this data, it
            will be deleted.
        """
        x = HydraIface.Link(link_id = link_id)
        x.delete(purge_data=purge_data)
        x.save()
        x.commit()
        return x.load()

    @rpc(Integer, _returns=SpyneArray(Scenario))
    def get_scenarios(ctx, network_id):
        """
            Get all the scenarios in a given network.
        """
        net = HydraIface.Network(network_id=network_id)
        
        net.load_all()

        scenarios = []

        for scen in net.scenarios:
            scen.load()
            scenarios.append(scen.get_as_complexmodel())

        return scenarios
