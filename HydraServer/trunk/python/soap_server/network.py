import logging
from HydraLib.HydraException import HydraError
from spyne.model.primitive import String, Integer, Boolean
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import Network, Node, Link, Scenario
from db import HydraIface
from HydraLib import hdb
from hydra_base import HydraService, ObjectNotFoundError
import scenario
from constraints import ConstraintService
import datetime

def _add_attributes(resource_i, attributes):
    if attributes is None:
        return []
    #ra is for ResourceAttr
    for ra in attributes:

        if ra.id < 0:
            ra_i = resource_i.add_attribute(ra.attr_id, ra.attr_is_var)
        else:
            ra_i = HydraIface.ResourceAttr(resource_attr_id=ra.id)
            ra_i.db.attr_is_var = ra.attr_is_var

    return resource_i.attributes

def _update_attributes(resource_i, attributes):
    resource_attr_id_map = dict()
    if attributes is None:
        return dict()
    #ra is for ResourceAttr
    for ra in attributes:

        if ra.id < 0:
            ra_i = resource_i.add_attribute(ra.attr_id, ra.attr_is_var)
        else:
            ra_i = HydraIface.ResourceAttr(resource_attr_id=ra.id)
            ra_i.db.attr_is_var = ra.attr_is_var

        ra_i.save()

        resource_attr_id_map[ra.id] = ra_i.db.resource_attr_id

    return resource_attr_id_map

def update_constraint_refs(constraintgroup, resource_attr_map):
    for item in constraintgroup.constraintitems:
        if item.resource_attr_id is not None:
            item.resource_attr_id = resource_attr_map[item.resource_attr_id]

    for group in constraintgroup.constraintgroups:
        update_constraint_refs(group, resource_attr_map)

def get_scenario_by_name(network_id, scenario_name):
    sql = """
        select
            scenario_id
        from
            tScenario
        where
            network_id = %s
        and lower(scenario_name) = '%s'
    """ % (network_id, scenario_name.lower())

    rs = HydraIface.execute(sql)
    if len(rs) == 0:
        logging.info("No scenario in network %s with name %s" % (network_id, scenario_name))
        return None
    else:
        logging.info("Scenario with name %s found in network %s" % (scenario_name, network_id))
        return rs[0].scenario_id



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

        net_i = HydraIface.Network()

        net_i.db.project_id          = network.project_id
        net_i.db.network_name        = network.name
        net_i.db.network_description = network.description
        
        if network.layout is not None:
            net_i.validate_layout(network.layout)
            net_i.db.network_layout      = network.layout

        net_i.save()
        network.network_id = net_i.db.network_id

        resource_attrs = []

        network_attrs = _add_attributes(net_i, network.attributes)

        resource_attrs.extend(network_attrs)

        all_attributes     = []
        all_attributes.extend(network.attributes)

        #Maps temporary node_ids to real node_ids
        node_ids = dict()

         #First add all the nodes
        logging.info("Adding nodes to network")
        for node in network.nodes:
            n = net_i.add_node(node.name, node.description, node.layout, node.x, node.y)

        HydraIface.bulk_insert(net_i.nodes, 'tNode')
        net_i.load_all()

        for n_i in net_i.nodes:
            for node in network.nodes:
                if node.id not in node_ids and node.x == n_i.db.node_x and node.y == n_i.db.node_y and node.name == n_i.db.node_name:
                    node_attrs = _add_attributes(n_i, node.attributes)
                    resource_attrs.extend(node_attrs)
                    all_attributes.extend(node.attributes)
                    #If a temporary ID was given to the node
                    #store the mapping to the real node_id
                    if node.id is not None and node.id <= 0:
                            node_ids[node.id] = n_i.db.node_id
                    break

        #Then add all the links.
        logging.info("Adding links to network")
        for link in network.links:
            node_1_id = link.node_1_id
            if link.node_1_id in node_ids:
                node_1_id = node_ids[link.node_1_id]

            node_2_id = link.node_2_id
            if link.node_2_id in node_ids:
                node_2_id = node_ids[link.node_2_id]

            if node_1_id is None or node_2_id is None:
                raise HydraError("Node IDS (%s, %s)are incorrect!"%(node_1_id, node_2_id))

            l = net_i.add_link(link.name, link.description, link.layout, node_1_id, node_2_id)

        HydraIface.bulk_insert(net_i.links, 'tLink')
        net_i.load_all()
        for l_i in net_i.links:
            for link in network.links:
                node_1_from_map = node_ids[link.node_1_id]
                node_2_from_map = node_ids[link.node_2_id]
                if l_i.db.node_1_id == node_1_from_map and l_i.db.node_2_id == node_2_from_map:
                    resource_attrs.extend(_add_attributes(l_i, link.attributes))
                    all_attributes.extend(link.attributes)
                    break

        #insert all the resource attributes in one go!
        last_resource_attr_id = HydraIface.bulk_insert(resource_attrs, "tResourceAttr")

        #import pudb; pudb.set_trace()
        if last_resource_attr_id is not None:
            next_ra_id = last_resource_attr_id - len(all_attributes) + 1
            resource_attr_id_map = {}
            for attribute in all_attributes:
                resource_attr_id_map[attribute.id] = next_ra_id
                next_ra_id = next_ra_id + 1

        if network.scenarios is not None:
            logging.info("Adding scenarios to network")
            for s in network.scenarios:
                scen = HydraIface.Scenario()
                scen.db.scenario_name        = s.name
                scen.db.scenario_description = s.description
                scen.db.network_id           = net_i.db.network_id
                scen.save()

                for r_scen in s.resourcescenarios:
                    r_scen.resource_attr_id = resource_attr_id_map[r_scen.resource_attr_id]

                #extract the data from each resourcescenario
                data = [r.value for r in s.resourcescenarios]

                dataset_ids = scenario.DataService.bulk_insert_data(ctx, data)

                #record all the resource attribute ids
                resource_attr_ids = [r.resource_attr_id for r in s.resourcescenarios]

                resource_scenarios = []
                for i, ra_id in enumerate(resource_attr_ids):
                    rs_i = HydraIface.ResourceScenario()
                    rs_i.db.resource_attr_id = ra_id
                    rs_i.db.dataset_id       = dataset_ids[i]
                    rs_i.db.scenario_id      = scen.db.scenario_id
                    resource_scenarios.append(rs_i)

                HydraIface.bulk_insert(resource_scenarios, 'tResourceScenario')

                #This is to get the resource scenarios into the scenario
                #object, so they are included into the scenario's complex model
                scen.load_all()
                
                for constraint in s.constraints:
                    update_constraint_refs(constraint.constraintgroup, resource_attr_id_map)
                    ConstraintService.add_constraint(ctx, scen.db.scenario_id, constraint) 

                net_i.scenarios.append(scen)

        logging.info("Insertion of network took: %s",(datetime.datetime.now()-insert_start))
        return_value = net_i.get_as_complexmodel()

        return return_value

    @rpc(Integer, Integer, _returns=Network)
    def get_network(ctx, network_id, scenario_id=None):
        """
            Return a whole network as a complex model.
        """
        x = HydraIface.Network(network_id = network_id)

        if x.load_all() is False:
            raise ObjectNotFoundError("Network (network_id=%s) not found." %
                                      network_id)

        net = x.get_as_complexmodel()

        if scenario_id is not None:
            scenario = []
            for scen in net.scenarios:
                if scen.id == scenario_id:
                    x = HydraIface.Scenario(scenario_id=scenario_id)
                    x.load_all()
                    scenario = x.get_as_complexmodel()
                    break
            net.scenarios = []
            net.scenarios.append(scenario)

        return net

    @rpc(String, Integer, _returns=Network)
    def get_network_by_name(ctx, project_id, network_name):
        """
            Return a whole network as a complex model.
        """

        sql = """
            select
                network_id
            from
                tNetwork
            where
                project_id = %s
            and lower(network_name) like '%%%s%%'
        """

        rs = HydraIface.execute(sql)
        if len(rs) == 0:
            raise HydraError('No network named %s found in project %s'%(network_name, project_id))
        elif len(rs) > 1:
            logging.warning("Multiple networks names %s found in project %s. Choosing first network in rs(network_id=%s)"%(network_name, project_id, rs[0].network_id))
        
        network_id = rs[0].network_id


        x = HydraIface.Network(network_id = network_id)

        if x.load_all() is False:
            raise ObjectNotFoundError("Network (network_id=%s) not found." % \
                                      network_id)

        net = x.get_as_complexmodel()

        return net



    @rpc(Network, _returns=Network)
    def update_network(ctx, network):
        """
            Update an entire network, excluding nodes.
        """
        net = None
        net_i = HydraIface.Network(network_id = network.id)
        net_i.load_all()
        net_i.db.project_id          = network.project_id
        net_i.db.network_name        = network.name
        net_i.db.network_description = network.description
        net_i.db.network_layout      = network.layout

        resource_attr_id_map = _update_attributes(net_i, network.attributes)

        #Maps temporary node_ids to real node_ids
        node_id_map = dict()

        #First add all the nodes
        for node in network.nodes:
            is_new = False

            #If we get a negative or null node id, we know
            #it is a new node.
            if node.id is not None and node.id > 0:
                n = net_i.get_node(node.id)
                n.db.node_name        = node.name
                n.db.node_description = node.description
                n.db.node_x           = node.x
                n.db.node_y           = node.y
                n.db.status           = node.status
            else:
                is_new = True
                n = net_i.add_node(node.name, node.description, node.layout, node.x, node.y)
            n.save()

            node_attr_id_map = _update_attributes(n, node.attributes)
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
                l = net_i.add_link(link.name, link.description, link.layout, node_1_id, node_2_id)
            else:
                l = net_i.get_link(link.id)
                l.load()
                l.db.link_name       = link.name
                l.db.link_descripion = link.description

            l.save()

            link_attr_id_map = _update_attributes(l, link.attributes)
            resource_attr_id_map.update(link_attr_id_map)


        if network.scenarios is not None:
            for s in network.scenarios:
                if s.id is not None and s.id > 0:
                    scen = HydraIface.Scenario(scenario_id=s.id)

                else:
                    scenario_id = get_scenario_by_name(network.id, s.name) 
                    scen = HydraIface.Scenario(scenario_id = scenario_id)
                scen.db.scenario_name        = s.name
                scen.db.scenario_description = s.description
                scen.db.network_id           = net_i.db.network_id
                scen.save()

                for r_scen in s.resourcescenarios:
                    r_scen.resource_attr_id = resource_attr_id_map[r_scen.resource_attr_id]

                    scenario._update_resourcescenario(scen.db.scenario_id, r_scen)

                for constraint in s.constraints:
                    update_constraint_refs(constraint.constraintgroup, resource_attr_id_map)
                    ConstraintService.add_constraint(ctx, scen.db.scenario_id, constraint) 

        net = net_i.get_as_complexmodel()

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
