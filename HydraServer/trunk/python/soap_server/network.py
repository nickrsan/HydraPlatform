import logging
from HydraLib.HydraException import HydraError
from spyne.model.primitive import String, Integer, Boolean
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import Network,\
Node,\
Link,\
Scenario,\
ResourceGroup,\
get_as_complexmodel
from db import HydraIface
from db import hdb, IfaceLib
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

def _add_resource_types(resource_i, types):
    """
    Save a reference to the types used for this resource.

    Type references in the DB but not passed into this
    function are considered obsolete and are deleted.

    @returns a list of type_ids representing the type ids
    on the resource.

    """
    if types is None:
        return []

    existing_templates = resource_i.get_templates_and_types()

    existing_type_ids = []
    for template_id, template in existing_templates.items():
        for type_id, type_name in template['types']:
            existing_type_ids.append(type_id)

    new_type_ids = []
    for templatetype in types:
        new_type_ids.append(templatetype.id)

        if templatetype.id in existing_type_ids:
            continue


        rt_i = HydraIface.ResourceType()
        rt_i.db.type_id     = templatetype.id
        rt_i.db.ref_key     = resource_i.ref_key
        rt_i.db.ref_id      = resource_i.ref_id
        rt_i.save()

    return new_type_ids

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
        logging.info("No scenario in network %s with name %s"\
                     % (network_id, scenario_name))
        return None
    else:
        logging.info("Scenario with name %s found in network %s"\
                     % (scenario_name, network_id))
        return rs[0].scenario_id

def get_timing(time):
    return datetime.datetime.now() - time

def add_nodes(net_i, nodes):
    start_time = datetime.datetime.now()

    #List of HydraIface resource attributes
    resource_attrs = []
    #List of all complexmodel attributes
    attrs          = []

    #Maps temporary node_ids to real node_ids
    node_id_map = dict()

    if nodes is None or len(nodes) == 0:
        return node_id_map, resource_attrs, attrs

    #First add all the nodes
    logging.info("Adding nodes to network")
    for node in nodes:
        net_i.add_node(node.name, node.description, node.layout, node.x, node.y)

    last_node_idx = IfaceLib.bulk_insert(net_i.nodes, 'tNode')
    next_id = last_node_idx - len(net_i.nodes) + 1
    idx = 0
    while idx < len(net_i.nodes):
        net_i.nodes[idx].db.node_id = next_id
        net_i.nodes[idx].ref_id = next_id
        next_id          = next_id + 1
        idx              = idx     + 1

    iface_nodes = dict()
    for n_i in net_i.nodes:
        iface_nodes[(n_i.db.node_x, n_i.db.node_y, n_i.db.node_name)] = n_i

    for node in nodes:
        if node.id not in node_id_map:
            node_i = iface_nodes[(node.x,node.y,node.name)]
            node_attrs = _add_attributes(node_i, node.attributes)
            _add_resource_types(node_i, node.types)
            resource_attrs.extend(node_attrs)
            attrs.extend(node.attributes)
            #If a temporary ID was given to the node
            #store the mapping to the real node_id
            if node.id is not None and node.id <= 0:
                    node_id_map[node.id] = node_i.db.node_id

    logging.info("Nodes added in %s", get_timing(start_time))

    return node_id_map, resource_attrs, attrs

def add_links(net_i, links, node_id_map):
    start_time = datetime.datetime.now()

    #List of HydraIface resource attributes
    resource_attrs = []
    #List of all complexmodel attributes
    attrs          = []
    #Map negative IDS to their new, positive, counterparts.
    link_id_map = dict()

    if links is None or len(links) == 0:
        return link_id_map, resource_attrs, attrs

    #Then add all the links.
    logging.info("Adding links to network")
    for link in links:
        node_1_id = link.node_1_id
        if link.node_1_id in node_id_map:
            node_1_id = node_id_map[link.node_1_id]

        node_2_id = link.node_2_id
        if link.node_2_id in node_id_map:
            node_2_id = node_id_map[link.node_2_id]

        if node_1_id is None or node_2_id is None:
            raise HydraError("Node IDS (%s, %s)are incorrect!"%(node_1_id, node_2_id))

        net_i.add_link(link.name,
                    link.description,
                    link.layout,
                    node_1_id,
                    node_2_id)

    last_link_idx = IfaceLib.bulk_insert(net_i.links, 'tLink')
    start_time = datetime.datetime.now()
    next_id = last_link_idx - len(net_i.links) + 1
    idx = 0
    while idx < len(net_i.links):
        net_i.links[idx].db.link_id = next_id
        net_i.links[idx].ref_id = next_id
        next_id          = next_id + 1
        idx              = idx     + 1

    iface_links = {}

    for l_i in net_i.links:
        iface_links[(l_i.db.node_1_id, l_i.db.node_2_id)] = l_i

    for link in links:
        iface_link = iface_links[(node_id_map[link.node_1_id], node_id_map[link.node_2_id])]
        _add_resource_types(iface_link, link.types)
        resource_attrs.extend(_add_attributes(iface_link, link.attributes))
        attrs.extend(link.attributes)
        link_id_map[link.id] = iface_link.db.link_id

    logging.info("Links added in %s", get_timing(start_time))

    return link_id_map, resource_attrs, attrs

def add_resource_groups(net_i, resourcegroups):
    start_time = datetime.datetime.now()
    #List of HydraIface resource attributes
    resource_attrs = []
    #List of all complexmodel attributes
    attrs          = []
    #Map negative IDS to their new, positive, counterparts.
    group_id_map = dict()

    if resourcegroups is None or len(resourcegroups)==0:
        return group_id_map, resource_attrs, attrs
    #Then add all the groups.
    logging.info("Adding groups to network")
    if resourcegroups:
        for group in resourcegroups:
            net_i.add_group(group.name,
                        group.description,
                        group.status)

        last_grp_idx = IfaceLib.bulk_insert(net_i.resourcegroups, 'tResourceGroup')

        next_id = last_grp_idx - len(net_i.resourcegroups) + 1
        idx = 0
        while idx < len(net_i.resourcegroups):
            net_i.resourcegroups[idx].db.group_id = next_id
            net_i.resourcegroups[idx].ref_id = next_id
            next_id          = next_id + 1
            idx              = idx     + 1
        iface_groups = {}
        for g_i in net_i.resourcegroups:
            iface_groups[g_i.db.group_name] = g_i
        for group in resourcegroups:
            grp_i = iface_groups[group.name]
            resource_attrs.extend(_add_attributes(grp_i, group.attributes))
            attrs.extend(group.attributes)
            _add_resource_types(grp_i, group.types)
            group_id_map[group.id] = grp_i.db.group_id

        logging.info("Groups added in %s", get_timing(start_time))

    return group_id_map, resource_attrs, attrs


class NetworkService(HydraService):
    """
        The network SOAP service.
    """

    @rpc(Network, _returns=Network)
    def add_network(ctx, network):
        """
        Takes an entire network complex model and saves it to the DB.  This
        complex model includes links & scenarios (with resource data).  Returns
        the network's complex model.

        As links connect two nodes using the node_ids, if the nodes are new
        they will not yet have node_ids. In this case, use negative ids as
        temporary IDS until the node has been given an permanent ID.

        All inter-object referencing of new objects should be done using
        negative IDs in the client.

        The returned object will have positive IDS

        """
        start_time = datetime.datetime.now()
        logging.info("Adding network")

        return_value = None
        insert_start = datetime.datetime.now()

        net_i = HydraIface.Network()

        proj_i = HydraIface.Project(project_id = network.project_id)
        proj_i.check_write_permission(ctx.in_header.user_id)

        net_i.db.project_id          = network.project_id
        net_i.db.network_name        = network.name
        net_i.db.network_description = network.description
        net_i.db.created_by          = ctx.in_header.user_id

        if network.layout is not None:
            net_i.db.network_layout = str(network.layout)

        net_i.save()
        network.network_id = net_i.db.network_id

        #These two lists are used for comparison and lookup, so when
        #new attributes are added, these lists are extended.

        #List of all the HydraIface resource attributes
        resource_attrs = []
        #list of all the complex model resource attributes.
        all_attributes     = []

        network_attrs  = _add_attributes(net_i, network.attributes)
        _add_resource_types(net_i, network.types)

        resource_attrs.extend(network_attrs)
        all_attributes.extend(network.attributes)

        logging.info("Network attributes added in %s", get_timing(start_time))

        node_id_map, node_resource_attrs, node_cm_attrs = add_nodes(net_i, network.nodes)
        resource_attrs.extend(node_resource_attrs)
        all_attributes.extend(node_cm_attrs)

        link_id_map, link_resource_attrs, link_cm_attrs = add_links(net_i, network.links, node_id_map)

        resource_attrs.extend(link_resource_attrs)
        all_attributes.extend(link_cm_attrs)


        grp_id_map, grp_resource_attrs, grp_cm_attrs = add_resource_groups(net_i, network.resourcegroups)

        resource_attrs.extend(grp_resource_attrs)
        all_attributes.extend(grp_cm_attrs)


        start_time = datetime.datetime.now()

        #insert all the resource attributes in one go!
        last_resource_attr_id = IfaceLib.bulk_insert(resource_attrs, "tResourceAttr")

        if last_resource_attr_id is not None:
            next_ra_id = last_resource_attr_id - len(all_attributes) + 1
            resource_attr_id_map = {}
            idx = 0
            for attribute in all_attributes:
                resource_attr_id_map[attribute.id] = next_ra_id
                resource_attrs[idx].db.resource_attr_id = next_ra_id
                next_ra_id = next_ra_id + 1
                idx        = idx + 1

        logging.info("Resource attributes added in %s", get_timing(start_time))
        start_time = datetime.datetime.now()

        attr_ra_map = dict()
        for ra in resource_attrs:
            attr_ra_map[ra.db.resource_attr_id] = ra

        if network.scenarios is not None:
            logging.info("Adding scenarios to network")
            for s in network.scenarios:
                scen = HydraIface.Scenario(network=net_i)
                scen.db.scenario_name        = s.name
                scen.db.scenario_description = s.description
                scen.db.network_id           = net_i.db.network_id
                scen.save()

                #extract the data from each resourcescenario
                data = []
                #record all the resource attribute ids
                resource_attr_ids = []
                for r_scen in s.resourcescenarios:
                    ra_id = resource_attr_id_map[r_scen.resource_attr_id]
                    r_scen.resource_attr_id = ra_id
                    data.append(r_scen.value)
                    resource_attr_ids.append(ra_id)

                data_start_time = datetime.datetime.now()
                datasets = scenario.bulk_insert_data(data, ctx.in_header.user_id)
                logging.info("Data bulk insert took %s", get_timing(data_start_time))
                for i, ra_id in enumerate(resource_attr_ids):
                    rs_i = HydraIface.ResourceScenario()
                    rs_i.db.resource_attr_id = ra_id
                    rs_i.db.dataset_id       = datasets[i].db.dataset_id
                    rs_i.db.scenario_id      = scen.db.scenario_id
                    rs_i.dataset = datasets[i]
                    rs_i.resourceattr = attr_ra_map[ra_id]
                    scen.resourcescenarios.append(rs_i)

                IfaceLib.bulk_insert(scen.resourcescenarios, 'tResourceScenario')

                for constraint in s.constraints:
                    update_constraint_refs(constraint.constraintgroup, resource_attr_id_map)
                    ConstraintService.add_constraint(ctx, scen.db.scenario_id, constraint)

                item_start_time = datetime.datetime.now()
                group_items = []
                for group_item in s.resourcegroupitems:
                    group_item_i = HydraIface.ResourceGroupItem()
                    group_item_i.db.scenario_id = scen.db.scenario_id
                    group_item_i.db.group_id = grp_id_map[group_item.group_id]
                    group_item_i.db.ref_key = group_item.ref_key
                    if group_item.ref_key == 'NODE':
                        ref_id = node_id_map[group_item.ref_id]
                    elif group_item.ref_key == 'LINK':
                        ref_id = link_id_map[group_item.ref_id]
                    elif group_item.ref_key == 'GROUP':
                        ref_id = grp_id_map[group_item.ref_id]
                    else:
                        raise HydraError("A ref key of %s is not valid for a "
                                         "resource group item.",\
                                         group_item.ref_key)

                    group_item_i.db.ref_id = ref_id
                    group_items.append(group_item_i)

                IfaceLib.bulk_insert(group_items, 'tResourceGroupItem')

                logging.info("Group items insert took %s", get_timing(item_start_time))
                net_i.scenarios.append(scen)

        logging.info("Scenarios added in %s", get_timing(start_time))
        net_i.set_ownership(ctx.in_header.user_id)

        logging.info("Insertion of network took: %s",(datetime.datetime.now()-insert_start))
        start_time = datetime.datetime.now()
        logging.info("Network created. Creating complex model")

        return_value = get_as_complexmodel(ctx, net_i)
        logging.info("Network conversion took: %s",get_timing(start_time))

        logging.debug("Return value: %s", return_value)
        return return_value

    @rpc(Integer,
         String(pattern="[YN]", default='Y'),
         Integer(min_occurs="0", max_occurs="unbounded"),
         _returns=Network)
    def get_network(ctx, network_id, include_data, scenario_ids):
        """
            Return a whole network as a complex model.
        """
        net_i = HydraIface.Network(network_id = network_id)

        net_i.check_read_permission(ctx.in_header.user_id)

        if net_i.load_all(scenario_ids) is False:
            raise ObjectNotFoundError("Network (network_id=%s) not found." %
                                      network_id)

        net = get_as_complexmodel(ctx, net_i, **dict(include_data=include_data))

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


        net_i = HydraIface.Network(network_id = network_id)

        net_i.check_read_permission(ctx.in_header.user_id)

        if net_i.load_all() is False:
            raise ObjectNotFoundError("Network (network_id=%s) not found." % \
                                      network_id)

        net = get_as_complexmodel(ctx, net_i)

        return net

    @rpc(Network, _returns=Network)
    def update_network(ctx, network):
        """
            Update an entire network
        """
        net = None
        net_i = HydraIface.Network(network_id = network.id)
        net_i.check_write_permission(ctx.in_header.user_id)
        net_i.load_all()
        net_i.db.project_id          = network.project_id
        net_i.db.network_name        = network.name
        net_i.db.network_description = network.description
        net_i.db.network_layout      = network.layout

        resource_attr_id_map = _update_attributes(net_i, network.attributes)
        _add_resource_types(net_i, network.types)

        #Maps temporary node_ids to real node_ids
        node_id_map = dict()

        #First add all the nodes
        for node in network.nodes:

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
                n = net_i.add_node(node.name,
                                   node.description,
                                   node.layout,
                                   node.x,
                                   node.y)
            n.save()

            node_attr_id_map = _update_attributes(n, node.attributes)
            _add_resource_types(n, node.types)
            resource_attr_id_map.update(node_attr_id_map)

            node_id_map[node.id] = n.db.node_id

        link_id_map = dict()

        for link in network.links:
            node_1_id = link.node_1_id
            if link.node_1_id in node_id_map:
                node_1_id = node_id_map[link.node_1_id]

            node_2_id = link.node_2_id
            if link.node_2_id in node_id_map:
                node_2_id = node_id_map[link.node_2_id]

            if link.id is None or link.id < 0:
                l = net_i.add_link(link.name,
                                   link.description,
                                   link.layout,
                                   node_1_id,
                                   node_2_id)
            else:
                l = net_i.get_link(link.id)
                l.load()
                l.db.link_name       = link.name
                l.db.link_descripion = link.description

            l.save()

            link_attr_id_map = _update_attributes(l, link.attributes)
            _add_resource_types(l, link.types)
            resource_attr_id_map.update(link_attr_id_map)
            link_id_map[link.id] = l.db.link_id

        group_id_map = dict()

        #Next all the groups
        for group in network.resourcegroups:

            #If we get a negative or null group id, we know
            #it is a new group.
            if group.id is not None and group.id > 0:
                g_i = net_i.get_group(group.id)
                g_i.db.group_name        = group.name
                g_i.db.group_description = group.description
                g_i.db.status           = group.status
            else:
                g_i = net_i.add_group(group.name,
                                   group.description,
                                   group.status)
            g_i.save()

            group_attr_id_map = _update_attributes(g_i, group.attributes)
            _add_resource_types(g_i, group.types)
            resource_attr_id_map.update(group_attr_id_map)

            group_id_map[group.id] = g_i.db.group_id

        if network.scenarios is not None:
            for s in network.scenarios:
                if s.id is not None and s.id > 0:
                    scen = HydraIface.Scenario(network=net_i, scenario_id=s.id)

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

                for group_item in s.resourcegroupitems:

                    if group_item.id and group_item.id > 0:
                        group_item_i = HydraIface.ResourceGroupItem(item_id=group_item.id)
                    else:
                        group_item_i = HydraIface.ResourceGroupItem()
                        group_item_i.db.scenario_id = scen.db.scenario_id
                        group_item_i.db.group_id = group_id_map[group_item.group_id]

                    group_item_i.db.ref_key = group_item.ref_key
                    if group_item.ref_key == 'NODE':
                        ref_id = node_id_map.get(group_item.ref_id)
                    elif group_item.ref_key == 'LINK':
                        ref_id = link_id_map.get(group_item.ref_id)
                    elif group_item.ref_key == 'GROUP':
                        ref_id = group_id_map.get(group_item.ref_id)
                    else:
                        raise HydraError("A ref key of %s is not valid for a "
                                         "resource group item.",\
                                         group_item.ref_key)

                    if ref_id is None:
                        raise HydraError("Invalid ref ID for group item!")

                    group_item_i.db.ref_id = ref_id
                    group_item_i.save()

        net_i.load_all()
        net = get_as_complexmodel(ctx, net_i)

        hdb.commit()

        return net

    @rpc(Integer, Boolean, _returns=Boolean)
    def delete_network(ctx, network_id, purge_data):
        """
        Deletes a network. This does not remove the network from the DB. It
        just sets the status to 'X', meaning it can no longer be seen by the
        user.
        """
        success = True
        net_i = HydraIface.Network(network_id = network_id)
        net_i.check_read_permission(ctx.in_header.user_id)
        net_i.db.status = 'X'
        net_i.save()
        net_i.commit()

        hdb.commit()
        return success

    @rpc(Network, Network, SpyneArray(Node), _returns=Boolean)
    def join_networks(ctx, network_1, network_2, joining_nodes):
        """
        Given two networks and a set of nodes, return a new network which is a
        combination of both networks, with the nodes as the joining nodes.

        """
        pass

    @rpc(Integer, Node, _returns=Node)
    def add_node(ctx, network_id, node):
        """
        Add a node to a network:

        .. code-block:: python

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

        net_i = HydraIface.Network(network_id = network_id)
        net_i.check_write_permission(ctx.in_header.user_id)

        node_i = HydraIface.Node()
        node_i.db.network_id = network_id
        node_i.db.node_name = node.name
        node_i.db.node_x    = node.x
        node_i.db.node_y    = node.y
        node_i.db.node_description = node.description
        node_i.save()

        _add_attributes(node_i, node.attributes)
        _add_resource_types(node_i, node.types)

        hdb.commit()
        net = get_as_complexmodel(ctx, node_i)


        return node

    @rpc(Node, _returns=Node)
    def update_node(ctx, node):
        """
        Update a node.
        If new attributes are present, they will be added to the node.
        The non-presence of attributes does not remove them.

        .. code-block:: python

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

        net_i = HydraIface.Network(network_id = x.db.network_id)
        net_i.check_write_permission(ctx.in_header.user_id)

        x.db.node_name = node.name
        x.db.node_x    = node.x
        x.db.node_y    = node.y
        x.db.node_description = node.description

        _add_attributes(x, node.attributes)
        _add_resource_types(x, node.types)

        x.save()
        x.commit()
        hdb.commit()
        node = get_as_complexmodel(ctx, x)

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

        net_i = HydraIface.Network(network_id=x.db.network_id)
        net_i.check_write_permission(ctx.in_header.user_id)

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

        net_i = HydraIface.Network(network_id=x.db.network_id)
        net_i.check_write_permission(ctx.in_header.user_id)

        x.delete(purge_data=purge_data)
        x.save()
        x.commit()
        return x.load()

    @rpc(Integer, Link, _returns=Link)
    def add_link(ctx, network_id, link):
        """
            Add a link to a network
        """

        net_i = HydraIface.Network(network_id=network_id)
        net_i.check_write_permission(ctx.in_header.user_id)

        x = HydraIface.Link()

        x.db.network_id = network_id
        x.db.link_name = link.name
        x.db.node_1_id = link.node_1_id
        x.db.node_2_id = link.node_2_id
        x.db.link_description = link.description
        x.save()

        _add_attributes(x, link.attributes)
        _add_resource_types(x, link.types)
        x.commit()

        link = get_as_complexmodel(ctx, x)

        return link

    @rpc(Link, _returns=Link)
    def update_link(ctx, link):
        """
            Update a link.
        """
        x = HydraIface.Link(link_id = link.id)

        net_i = HydraIface.Network(network_id=x.db.network_id)
        net_i.check_write_permission(ctx.in_header.user_id)

        x.db.link_name = link.name
        x.db.node_1_id = link.node_1_id
        x.db.node_2_id = link.node_2_id
        x.db.link_description = link.description

        _add_attributes(x, link.attributes)
        _add_resource_types(x, link.types)

        x.save()
        x.commit()

        link = get_as_complexmodel(ctx, x)

        return link

    @rpc(Integer, _returns=Link)
    def delete_link(ctx, link_id):
        """
            Set the status of a link to 'X'
        """
        success = True
        x = HydraIface.Link(link_id = link_id)

        net_i = HydraIface.Network(network_id=x.db.network_id)
        net_i.check_write_permission(ctx.in_header.user_id)

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

        net_i = HydraIface.Network(network_id=x.db.network_id)
        net_i.check_write_permission(ctx.in_header.user_id)

        x.delete(purge_data=purge_data)
        x.save()
        x.commit()
        return x.load()

    @rpc(Integer, ResourceGroup, _returns=ResourceGroup)
    def add_group(ctx, network_id, group):
        """
            Add a resourcegroup to a network
        """

        net_i = HydraIface.Network(network_id=network_id)
        net_i.check_write_permission(ctx.in_header.user_id)

        res_grp_i = HydraIface.ResourceGroup()
        res_grp_i.db.network_id = network_id
        res_grp_i.db.group_name = group.name
        res_grp_i.db.group_description = group.description
        res_grp_i.db.status = group.status
        res_grp_i.save()

        _add_attributes(res_grp_i, group.attributes)
        _add_resource_types(res_grp_i, group.types)
        res_grp_i.commit()

        group = get_as_complexmodel(ctx, res_grp_i)

        return group

    @rpc(Integer, _returns=SpyneArray(Scenario))
    def get_scenarios(ctx, network_id):
        """
            Get all the scenarios in a given network.
        """
        net = HydraIface.Network(network_id=network_id)

        net_i = HydraIface.Network(network_id=network_id)
        net_i.check_read_permission(ctx.in_header.user_id)

        net.load_all()

        scenarios = []

        for scen in net.scenarios:
            scen.load()
            s_complex = get_as_complexmodel(ctx, scen)
            scenarios.append(s_complex)

        return scenarios

    @rpc(Integer, _returns=String)
    def validate_network_topology(ctx, network_id):
        """
            Check for the presence of orphan nodes in a network.
        """

        net_i = HydraIface.Network(network_id=network_id)
        net_i.check_read_permission(ctx.in_header.user_id)

        net_i = HydraIface.Network(network_id=network_id)
        net_i.load_all()

        nodes = []
        for node_i in net_i.nodes:
            nodes.append(node_i.db.node_id)

        link_nodes = []
        for link_i in net_i.links:
            if link_i.db.node_1_id not in link_nodes:
                link_nodes.append(link_i.db.node_1_id)

            if link_i.db.node_2_id not in link_nodes:
                link_nodes.append(link_i.db.node_2_id)

        nodes = set(nodes)
        link_nodes = set(link_nodes)

        isolated_nodes = nodes - link_nodes
        if len(isolated_nodes) > 0:
            return "Orphan nodes are present."

        return "OK"
