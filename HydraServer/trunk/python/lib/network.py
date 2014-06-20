# (c) Copyright 2013, 2014, University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
import logging
from HydraLib.HydraException import HydraError, ResourceNotFoundError
import scenario
import datetime
import data

from util.permissions import check_perm
import template
from db.HydraAlchemy import Project, Network, Scenario, Node, Link, ResourceGroup, ResourceAttr, ResourceType, ResourceGroupItem, Dataset, DatasetOwner, ResourceScenario
from sqlalchemy.orm import noload, joinedload, contains_eager
from sqlalchemy.sql import exists
from db import DBSession
from sqlalchemy import func, or_, and_
from sqlalchemy.orm.exc import NoResultFound
from HydraLib.util import timestamp_to_ordinal

log = logging.getLogger(__name__)

def _add_attributes(resource_i, attributes):
    if attributes is None:
        return {}
    resource_attrs = {}
    #ra is for ResourceAttr
    for ra in attributes:

        if ra.id < 0:
            ra_i = resource_i.add_attribute(ra.attr_id, ra.attr_is_var)
        else:
            ra_i = DBSession.query(ResourceAttr).filter(ResourceAttr.resource_attr_id==ra.id).one()
            ra_i.attr_is_var = ra.attr_is_var

        resource_attrs[ra.id] = ra_i

    return resource_attrs

def _add_resource_types(resource_i, types):
    """
    Save a reference to the types used for this resource.

    @returns a list of type_ids representing the type ids
    on the resource.

    """
    if types is None:
        return []

    existing_type_ids = []
    if resource_i.types:
        for t in resource_i.types:
            existing_type_ids.append(t.type_id)

    new_type_ids = []
    for templatetype in types:

        if templatetype.id in existing_type_ids:
            continue

        rt_i = ResourceType()
        rt_i.type_id     = templatetype.id
        rt_i.ref_key     = resource_i.ref_key
        if resource_i.ref_key == 'NODE':
            rt_i.node_id      = resource_i.node_id
        elif resource_i.ref_key == 'LINK':
            rt_i.link_id      = resource_i.link_id
        elif resource_i.ref_key == 'GROUP':
            rt_i.group_id     = resource_i.group_id
        resource_i.types.append(rt_i)
        new_type_ids.append(templatetype.id)

    return new_type_ids

def _update_attributes(resource_i, attributes):
    if attributes is None:
        return dict()
    attrs = {}
    #ra is for ResourceAttr
    for ra in attributes:

        if ra.id < 0:
            ra_i = resource_i.add_attribute(ra.attr_id, ra.attr_is_var)
        else:
            ra_i = DBSession.query(ResourceAttr).filter(ResourceAttr.resource_attr_id==ra.id).one()
            ra_i.attr_is_var = ra.attr_is_var
        attrs[ra.id] = ra_i

    return attrs 

def get_scenario_by_name(network_id, scenario_name,**kwargs):
    try:
        scen = DBSession.query(Scenario).filter(and_(Scenario.network_id==network_id, func.lower(Scenario.scenario_id.lower) == scenario_name.lower())).one()
        return scen.scenario_id
    except NoResultFound:
        log.info("No scenario in network %s with name %s"\
                     % (network_id, scenario_name))
        return None

def get_timing(time):
    return datetime.datetime.now() - time

def _get_all_attributes(network):
    """
        Get all the complex mode attributes in the network so that they
        can be used for mapping to resource scenarios later.
    """
    attrs = network.attributes
    for n in network.nodes:
        attrs.extend(n.attributes)
    for l in network.links:
        attrs.extend(l.attributes)
    for g in network.resourcegroups:
        attrs.extend(g.attributes)

    return attrs

def _add_nodes(net_i, nodes):

    #check_perm(user_id, 'edit_topology')

    start_time = datetime.datetime.now()

    #List of HydraIface resource attributes
    node_attrs = {}

    #Maps temporary node_ids to real node_ids
    node_id_map = dict()

    if nodes is None or len(nodes) == 0:
        return node_id_map, node_attrs

    #First add all the nodes
    log.info("Adding nodes to network")
    for node in nodes:
        net_i.add_node(node.name, node.description, node.layout, node.x, node.y)

    iface_nodes = dict()
    for n_i in net_i.nodes:
        iface_nodes[n_i.node_name] = n_i

    for node in nodes:
        if node.id not in node_id_map:
            node_i = iface_nodes[node.name]
            node_attrs.update(_add_attributes(node_i, node.attributes))
            _add_resource_types(node_i, node.types)
            node_id_map[node.id] = node_i

    log.info("Nodes added in %s", get_timing(start_time))

    return node_id_map, node_attrs

def _add_links(net_i, links, node_id_map):

    #check_perm(user_id, 'edit_topology')

    start_time = datetime.datetime.now()

    #List of HydraIface resource attributes
    link_attrs = {}
    #Map negative IDS to their new, positive, counterparts.
    link_id_map = dict()

    if links is None or len(links) == 0:
        return link_id_map, link_attrs

    #Then add all the links.
    log.info("Adding links to network")
    for link in links:
        if link.node_1_id in node_id_map:
            node_1 = node_id_map[link.node_1_id]

        if link.node_2_id in node_id_map:
            node_2 = node_id_map[link.node_2_id]

        if node_1 is None or node_2 is None:
            raise HydraError("Node IDS (%s, %s)are incorrect!"%(node_1, node_2))

        net_i.add_link(link.name,
                    link.description,
                    link.layout,
                    node_1,
                    node_2)

    iface_links = {}

    for l_i in net_i.links:
        iface_links[l_i.link_name] = l_i

    for link in links:
        iface_link = iface_links[link.name]
        _add_resource_types(iface_link, link.types)
        link_attrs.update(_add_attributes(iface_link, link.attributes))
        link_id_map[link.id] = iface_link

    log.info("Links added in %s", get_timing(start_time))

    return link_id_map, link_attrs

def _add_resource_groups(net_i, resourcegroups):
    start_time = datetime.datetime.now()
    #List of HydraIface resource attributes
    group_attrs = {}
    #Map negative IDS to their new, positive, counterparts.
    group_id_map = dict()

    if resourcegroups is None or len(resourcegroups)==0:
        return group_id_map, group_attrs
    #Then add all the groups.
    log.info("Adding groups to network")
    if resourcegroups:
        for group in resourcegroups:
            net_i.add_group(group.name,
                        group.description,
                        group.status)

        iface_groups = {}
        for g_i in net_i.resourcegroups:
            iface_groups[g_i.group_name] = g_i

        for group in resourcegroups:
            grp_i = iface_groups[group.name]
            group_attrs.update(_add_attributes(grp_i, group.attributes))
            _add_resource_types(grp_i, group.types)
            group_id_map[group.id] = grp_i

        log.info("Groups added in %s", get_timing(start_time))

    return group_id_map, group_attrs


def add_network(network,**kwargs):
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
    DBSession.autoflush = False
    user_id = kwargs.get('user_id')

    #check_perm('add_network')

    start_time = datetime.datetime.now()
    log.debug("Adding network")

    insert_start = datetime.datetime.now()

    proj_i = DBSession.query(Project).filter(Project.project_id == network.project_id).one()
    proj_i.check_write_permission(user_id)

    net_i = Network()
    net_i.project_id          = network.project_id
    net_i.network_name        = network.name
    net_i.network_description = network.description
    net_i.created_by          = user_id

    if network.layout is not None:
        net_i.network_layout = str(network.layout)

    network.network_id = net_i.network_id

    #These two lists are used for comparison and lookup, so when
    #new attributes are added, these lists are extended.

    #List of all the HydraIface resource attributes
    all_resource_attrs = {}

    network_attrs  = _add_attributes(net_i, network.attributes)
    _add_resource_types(net_i, network.types)

    all_resource_attrs.update(network_attrs)

    log.info("Network attributes added in %s", get_timing(start_time))

    node_id_map, node_attrs = _add_nodes(net_i, network.nodes)
    all_resource_attrs.update(node_attrs)

    link_id_map, link_attrs = _add_links(net_i, network.links, node_id_map)
    all_resource_attrs.update(link_attrs)

    grp_id_map, grp_attrs = _add_resource_groups(net_i, network.resourcegroups)
    all_resource_attrs.update(grp_attrs)

    start_time = datetime.datetime.now()

    DBSession.add(net_i)
    DBSession.flush()
    
    if network.scenarios is not None:
        log.info("Adding scenarios to network")
        for s in network.scenarios:
            scen = Scenario()
            scen.scenario_name        = s.name
            scen.scenario_description = s.description
            scen.start_time           = timestamp_to_ordinal(s.start_time)
            scen.end_time             = timestamp_to_ordinal(s.end_time)
            scen.time_step            = s.time_step
            net_i.scenarios.append(scen)

            #extract the data from each resourcescenario
            datasets = []
            scenario_resource_attrs = []
            for r_scen in s.resourcescenarios:
                ra = all_resource_attrs[r_scen.resource_attr_id]
                datasets.append(r_scen.value)
                scenario_resource_attrs.append(ra)

            data_start_time = datetime.datetime.now()
            datasets = data.bulk_insert_data(datasets, user_id)
            log.info("Data bulk insert took %s", get_timing(data_start_time))
            for i, ra in enumerate(scenario_resource_attrs):
                scen.add_resource_scenario(ra, datasets[i])

            item_start_time = datetime.datetime.now()
            for group_item in s.resourcegroupitems:
                group_item_i = ResourceGroupItem()
                group_item_i.group = grp_id_map[group_item.group_id]
                group_item_i.ref_key  = group_item.ref_key
                if group_item.ref_key == 'NODE':
                    group_item_i.node = node_id_map[group_item.ref_id]
                elif group_item.ref_key == 'LINK':
                    group_item_i.link = link_id_map[group_item.ref_id]
                elif group_item.ref_key == 'GROUP':
                    group_item_i.subgroup = grp_id_map[group_item.ref_id]
                else:
                    raise HydraError("A ref key of %s is not valid for a "
                                     "resource group item.",\
                                     group_item.ref_key)
                
                scen.resourcegroupitems.append(group_item_i)
            log.info("Group items insert took %s", get_timing(item_start_time))
            net_i.scenarios.append(scen)

    log.info("Scenarios added in %s", get_timing(start_time))
    net_i.set_owner(user_id)

    DBSession.flush()
    log.info("Insertion of network took: %s",(datetime.datetime.now()-insert_start))

    return net_i


def get_network(network_id, include_data='N', scenario_ids=None,**kwargs):
    """
        Return a whole network as a dictionary.
    """
    log.debug("getting network %s"%network_id)
    user_id = kwargs.get('user_id')
    try:
        qry = DBSession.query(Network).filter(Network.network_id == network_id).options(noload('scenarios'))
        net_i = qry.one()
         
        scen_qry = DBSession.query(Scenario).filter(Scenario.network_id == net_i.network_id)
        if scenario_ids:
            logging.info("Filtering by scenario_ids %s",scenario_ids)
            scen_qry = scen_qry.join(Network.scenarios).filter(Scenario.scenario_id.in_(scenario_ids))

        if include_data == 'N':
            logging.info("Not returning data")
            scen_qry = scen_qry.options(noload('resourcescenarios'))

        scens = scen_qry.all()
        net_i.scenarios = scens
    except NoResultFound:
        raise ResourceNotFoundError("Network (network_id=%s) not found." %
                                  network_id)
 
    net_i.check_read_permission(user_id)

    disallowed_datasets = DBSession.query(Dataset.data_type,
                                          Dataset.data_dimen,
                                          Dataset.data_units,
                                          Dataset.locked,
                                          Dataset.data_name).outerjoin(DatasetOwner).filter(and_(DatasetOwner.user_id==user_id, DatasetOwner.view=='N')).all()
    
    disallowed_dataset_dict = dict([(dataset.dataset_id, dataset) for dataset in disallowed_datasets])

    all_resourcescenario_datasets = DBSession.query(Dataset).filter(Dataset.dataset_id==ResourceScenario.dataset_id, ResourceScenario.scenario_id == Scenario.scenario_id, Scenario.network_id == Network.network_id).all()
    allowed_dataset_dict = dict([(dataset.dataset_id, dataset) for dataset in all_resourcescenario_datasets])

    for s in net_i.scenarios:
        for rs in s.resourcescenarios:
            if rs.dataset_id in disallowed_dataset_dict.keys():
                rs.dataset = disallowed_dataset_dict[rs.dataset_id]
            else:
                rs.dataset = allowed_dataset_dict[rs.dataset_id]

    return net_i


def get_node(node_id,**kwargs): 
    try:
        n = DBSession.query(Node).filter(Node.node_id==node_id).one() 
        return n
    except NoResultFound:
        raise ResourceNotFoundError("Node %s not found"%(node_id,))
 
def get_link(link_id,**kwargs): 
    try:
        l = DBSession.query(Link).filter(Link.link_id==link_id).one()
        return l
    except NoResultFound:
        raise ResourceNotFoundError("Link %s not found"%(link_id,))

def get_network_by_name(project_id, network_name,**kwargs):
    """
    Return a whole network as a complex model.
    """

    try:
        network_id = DBSession.query(Network.network_id).filter(func.lower(Network.network_name).like(network_name.lower()), Network.project_id == project_id).one()
        net = get_network(network_id, 'Y', None, **kwargs)
        return net
    except NoResultFound:
        raise ResourceNotFoundError("Network with name %s not found"%(network_name))


def network_exists(project_id, network_name,**kwargs):
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
    """ % (project_id, network_name)

    rs = HydraIface.execute(sql)
    if len(rs) == 0:
        return 'N'
    else:
        return 'Y'

def update_network(network,**kwargs):
    """
        Update an entire network
    """

    user_id = kwargs.get('user_id')
    #check_perm('update_network')

    try:
        net_i = DBSession.query(Network).filter(Network.network_id == network.id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Network with id %s not found"%(network.id))

    net_i.project_id          = network.project_id
    net_i.network_name        = network.name
    net_i.network_description = network.description
    net_i.network_layout      = str(network.layout)

    all_resource_attrs = {} 
    all_resource_attrs.update(_update_attributes(net_i, network.attributes))
    _add_resource_types(net_i, network.types)

    #Maps temporary node_ids to real node_ids
    node_id_map = dict()

    #First add all the nodes
    for node in network.nodes:

        #If we get a negative or null node id, we know
        #it is a new node.
        if node.id is not None and node.id > 0:
            n = DBSession.query(Node).filter(Node.node_id==node.id).one()
            n.node_name        = node.name
            n.node_description = node.description
            n.node_x           = node.x
            n.node_y           = node.y
            n.status           = node.status
        else:
            n = net_i.add_node(node.name,
                               node.description,
                               node.layout,
                               node.x,
                               node.y)
            net_i.nodes.append(n)
        all_resource_attrs.update(_update_attributes(n, node.attributes))
        _add_resource_types(n, node.types)

        node_id_map[node.id] = n

    link_id_map = dict()

    for link in network.links:
        node_1 = node_id_map[link.node_1_id]

        node_2 = node_id_map[link.node_2_id]

        if link.id is None or link.id < 0:
            l = net_i.add_link(link.name,
                               link.description,
                               link.layout,
                               node_1,
                               node_2)
            net_i.links.append(l)
        else:
            l = DBSession.query(Link).filter(Link.link_id==link.id).one()
            l.link_name       = link.name
            l.link_descripion = link.description
            l.node_a          = node_1
            l.node_b          = node_2

        all_resource_attrs.update(_update_attributes(l, link.attributes))
        _add_resource_types(l, link.types)
        link_id_map[link.id] = l

    group_id_map = dict()

    #Next all the groups
    for group in network.resourcegroups:

        #If we get a negative or null group id, we know
        #it is a new group.
        if group.id is not None and group.id > 0:
            g_i = DBSession.query(ResourceGroup).filter(ResourceGroup.group_id==group.id).one()
            g_i.group_name        = group.name
            g_i.group_description = group.description
            g_i.status           = group.status
        else:
            g_i = net_i.add_group(group.name,
                               group.description,
                               group.status)
            net_i.resourcegroups.append(net_i)

        all_resource_attrs.update(_update_attributes(g_i, group.attributes))
        _add_resource_types(g_i, group.types)

        group_id_map[group.id] = g_i
    errors = []
    if network.scenarios is not None:
        for s in network.scenarios:
            if s.id is not None and s.id > 0:
                try:
                    scen = DBSession.query(Scenario).filter(Scenario.scenario_id==s.id).one()
                except NoResultFound:
                    raise ResourceNotFoundError("Scenario %s not found"%(s.id))

                if scen.locked == 'Y':
                    errors.append('Scenario %s was not updated as it is locked'%(s.id)) 
                    continue
            else:
                scen = Scenario()
                net_i.scenarios.append(scen)

            scen.scenario_name        = s.name
            scen.scenario_description = s.description
            scen.start_time           = timestamp_to_ordinal(s.start_time)
            scen.end_time             = timestamp_to_ordinal(s.end_time)
            scen.time_step            = s.time_step
            scen.network_id           = net_i.network_id

            for r_scen in s.resourcescenarios:
                r_scen.resourceattr = all_resource_attrs[r_scen.resource_attr_id]
                scenario._update_resourcescenario(scen, r_scen)

            for group_item in s.resourcegroupitems:

                if group_item.id and group_item.id > 0:
                    group_item_i = DBSession.query(ResourceGroupItem).filter(ResourceGroupItem.item_id==group_item.id).one()
                else:
                    group_item_i = ResourceGroupItem()
                    group_item_i.group_id = group_id_map[group_item.group_id]
                    scenario.resourcegroupitems.append(group_item_i)

                group_item_i.ref_key = group_item.ref_key
                if group_item.ref_key == 'NODE':
                    group_item_i.node = node_id_map.get(group_item.ref_id)
                elif group_item.ref_key == 'LINK':
                    group_item_i.link = link_id_map.get(group_item.ref_id)
                elif group_item.ref_key == 'GROUP':
                    group_item_i.subgroup = group_id_map.get(group_item.ref_id)
                else:
                    raise HydraError("A ref key of %s is not valid for a "
                                     "resource group item."%group_item.ref_key)

    DBSession.flush()

    return net_i

def delete_network(network_id, purge_data,**kwargs):
    """
    Deletes a network. This does not remove the network from the DB. It
    just sets the status to 'X', meaning it can no longer be seen by the
    user.
    """
    user_id = kwargs.get('user_id')
    #check_perm(user_id, 'delete_network')
    try:
        net_i = DBSession.query(Network).filter(Network.network_id == network_id).one()
        net_i.check_write_permission(user_id)
        net_i.status = 'X'
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))
    DBSession.flush()
    return 'OK'

def get_network_extents(network_id,**kwargs):
    """
    Given a network, return its maximum extents.
    This would be the minimum x value of all nodes,
    the minimum y value of all nodes,
    the maximum x value of all nodes and
    maximum y value of all nodes.

    @returns NetworkExtents object
    """
    rs = DBSession.query(Node.node_x, Node.node_y).filter(Node.network_id==network_id).all()
    x_values = []
    y_values = []
    for r in rs:
        x_values.append(r.node_x)
        y_values.append(r.node_y)

    x_values.sort()
    y_values.sort()

    ne = dict(
        network_id = network_id,
        min_x = x_values[0],
        max_x = x_values[-1],
        min_y = y_values[0],
        max_y = y_values[-1],
    )
    return ne

def add_node(network_id, node,**kwargs):

    """
    Add a node to a network:
    """

    user_id = kwargs.get('user_id')
    try:
        net_i = DBSession.query(Network).filter(Network.network_id == network_id).one()
        net_i.check_write_permission(user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))


    new_node = net_i.add_node(node.name, node.description, node.layout, node.x, node.y)

    _add_attributes(new_node, node.attributes)
    for typesummary in node.types:
        template.set_resource_type(new_node,
                                        typesummary.id,
                                         **kwargs)
    DBSession.flush()

    return new_node

def update_node(node,**kwargs):
    """
    Update a node.
    If new attributes are present, they will be added to the node.
    The non-presence of attributes does not remove them.

    """
    user_id = kwargs.get('user_id')
    try:
        node_i = DBSession.query(Node).filter(Node.node_id == node.id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Node %s not found"%(node.id))

    node_i.network.check_write_permission(user_id)

    node_i.node_name = node.name
    node_i.node_x    = node.x
    node_i.node_y    = node.y
    node_i.node_description = node.description

    _update_attributes(node_i, node.attributes)

    _add_resource_types(node_i, node.types)
    DBSession.flush()

    return node_i

def delete_resourceattr(resource_attr_id, purge_data,**kwargs):
    """
        Deletes a resource attribute and all associated data.
    """
    try:
        ra = DBSession.query(ResourceAttr).filter(ResourceScenario.resource_attr_id == resource_attr_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Resource Attribute %s not found"%(resource_attr_id))
    DBSession.delete(ra)
    DBSession.flush()
    return 'OK'

def delete_node(node_id,**kwargs):
    """
        Set the status of a node to 'X'
    """
    user_id = kwargs.get('user_id')
    try:
        node_i = DBSession.query(Node).filter(Node.node_id == node_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Node %s not found"%(node_id))

    node_i.network.check_write_permission(user_id)

    node_i.status = 'X'
    DBSession.flush()

    return node_i

def purge_node(node_id, purge_data,**kwargs):
    """
        Remove node from DB completely
        If there are attributes on the node, use purge_data to try to
        delete the data. If no other resources link to this data, it
        will be deleted.

    """
    user_id = kwargs.get('user_id')
    try:
        node_i = DBSession.query(Node).filter(Node.node_id == node_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Node %s not found"%(node_id))

    node_i.network.check_write_permission(user_id)
    DBSession.delete(node_i)
    DBSession.flush()
    return 'OK'

def add_link(network_id, link,**kwargs):
    """
        Add a link to a network
    """
    user_id = kwargs.get('user_id')

    #check_perm(user_id, 'edit_topology')
    try:
        net_i = DBSession.query(Network).filter(Network.network_id == network_id).one()
        net_i.check_write_permission(user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))

    try:
        node_1 = DBSession.query(Node).filter(Node.node_id==link.node_1_id).one()
        node_2 = DBSession.query(Node).filter(Node.node_id==link.node_2_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Nodes for link not found")
    
    link_i = net_i.add_link(link.name, link.description, link.layout, node_1, node_2)

    _add_attributes(link_i, link.attributes)

    for resource_type in link.types:
        template.set_resource_type(link_i,
                                   resource_type.id,
                                   **kwargs)
    return link_i

def update_link(link,**kwargs):
    """
        Update a link.
    """
    user_id = kwargs.get('user_id')
    #check_perm(user_id, 'edit_topology')
    try:
        link_i = DBSession.query(Link).filter(Link.link_id == link.id).one()
        link_i.network.check_write_permission(user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Link %s not found"%(link.id))

    link_i.link_name = link.name
    link_i.node_1_id = link.node_1_id
    link_i.node_2_id = link.node_2_id
    link_i.link_description = link.description

    _add_attributes(link_i, link.attributes)
    _add_resource_types(link_i, link.types)
    DBSession.flush()
    return link_i

def delete_link(link_id,**kwargs):
    """
        Set the status of a link to 'X'
    """
    user_id = kwargs.get('user_id')
    #check_perm(user_id, 'edit_topology')
    try:
        link_i = DBSession.query(Link).filter(Link.link_id == link_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Link %s not found"%(link_id))

    link_i.network.check_write_permission(user_id)

    link_i.status = 'X'
    DBSession.flush()

def purge_link(link_id, purge_data,**kwargs):
    """
        Remove link from DB completely
        If there are attributes on the link, use purge_data to try to
        delete the data. If no other resources link to this data, it
        will be deleted.
    """
    user_id = kwargs.get('user_id')
    try:
        link_i = DBSession.query(Link).filter(Link.link_id == link_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Link %s not found"%(link_id))

    link_i.network.check_write_permission(user_id)
    DBSession.delete(link_i)
    DBSession.flush()

def add_group(network_id, group,**kwargs):
    """
        Add a resourcegroup to a network
    """

    user_id = kwargs.get('user_id')
    try:
        net_i = DBSession.query(Network).filter(Network.network_id == network_id).one()
        net_i.check_write_permission(user_id=user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))

    res_grp_i = net_i.add_group(group.name, group.description, group.status)

    _add_attributes(res_grp_i, group.attributes)
    _add_resource_types(res_grp_i, group.types)
    
    DBSession.flush()

    return res_grp_i

def get_scenarios(network_id,**kwargs):
    """
        Get all the scenarios in a given network.
    """

    user_id = kwargs.get('user_id')
    try:
        net_i = DBSession.query(Network).filter(Network.network_id == network_id).one()
        net_i.check_write_permission(user_id=user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))
    
    return net_i.scenarios

def validate_network_topology(network_id,**kwargs):
    """
        Check for the presence of orphan nodes in a network.
    """

    user_id = kwargs.get('user_id')
    try:
        net_i = DBSession.query(Network).filter(Network.network_id == network_id).one()
        net_i.check_write_permission(user_id=user_id)
    except NoResultFound:
        raise ResourceNotFoundError("Network %s not found"%(network_id))

    nodes = []
    for node_i in net_i.nodes:
        nodes.append(node_i.node_id)

    link_nodes = []
    for link_i in net_i.links:
        if link_i.node_1_id not in link_nodes:
            link_nodes.append(link_i.node_1_id)

        if link_i.node_2_id not in link_nodes:
            link_nodes.append(link_i.node_2_id)

    nodes = set(nodes)
    link_nodes = set(link_nodes)

    isolated_nodes = nodes - link_nodes

    return isolated_nodes
