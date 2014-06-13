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
from db import HydraIface
from db import hdb, IfaceLib
from db.hdb import make_param
import scenario
import datetime
from util.permissions import check_perm
import template

from HydraLib.util import timestamp_to_ordinal

log = logging.getLogger(__name__)

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
    for template_id, tmpl in existing_templates.items():
        for type_id, type_name in tmpl['types']:
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

def update_constraint_refs(constraintgroup, resource_attr_map,**kwargs):
    for item in constraintgroup.constraintitems:
        if item.resource_attr_id is not None:
            item.resource_attr_id = resource_attr_map[item.resource_attr_id]

    for group in constraintgroup.constraintgroups:
        update_constraint_refs(group, resource_attr_map)

def get_scenario_by_name(network_id, scenario_name,**kwargs):
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
        log.info("No scenario in network %s with name %s"\
                     % (network_id, scenario_name))
        return None
    else:
        log.info("Scenario with name %s found in network %s"\
                     % (scenario_name, network_id))
        return rs[0].scenario_id

def get_timing(time):
    return datetime.datetime.now() - time

def _add_nodes(net_i, nodes):

    #check_perm(user_id, 'edit_topology')

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
    log.info("Adding nodes to network")
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

    log.info("Nodes added in %s", get_timing(start_time))

    return node_id_map, resource_attrs, attrs

def _add_links(net_i, links, node_id_map):

    #check_perm(user_id, 'edit_topology')

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
    log.info("Adding links to network")
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
        iface_links[(l_i.db.node_1_id, l_i.db.node_2_id, l_i.db.link_name)] \
            = l_i

    for link in links:
        iface_link = iface_links[(node_id_map[link.node_1_id],
                                  node_id_map[link.node_2_id],
                                  link.name)]
        _add_resource_types(iface_link, link.types)
        resource_attrs.extend(_add_attributes(iface_link, link.attributes))
        attrs.extend(link.attributes)
        link_id_map[link.id] = iface_link.db.link_id

    log.info("Links added in %s", get_timing(start_time))

    return link_id_map, resource_attrs, attrs

def _add_resource_groups(net_i, resourcegroups):
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
    log.info("Adding groups to network")
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

        log.info("Groups added in %s", get_timing(start_time))

    return group_id_map, resource_attrs, attrs


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
    user_id = kwargs.get('user_id')

    #check_perm('add_network')

    start_time = datetime.datetime.now()
    log.debug("Adding network")

    insert_start = datetime.datetime.now()

    net_i = HydraIface.Network()

    proj_i = HydraIface.Project(project_id = network.project_id)
    proj_i.check_write_permission(user_id)

    net_i.db.project_id          = network.project_id
    net_i.db.network_name        = network.name
    net_i.db.network_description = network.description
    net_i.db.created_by          = user_id

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

    log.info("Network attributes added in %s", get_timing(start_time))

    node_id_map, node_resource_attrs, node_cm_attrs = _add_nodes(net_i, network.nodes)
    resource_attrs.extend(node_resource_attrs)
    all_attributes.extend(node_cm_attrs)

    link_id_map, link_resource_attrs, link_cm_attrs = _add_links(net_i, network.links, node_id_map)

    resource_attrs.extend(link_resource_attrs)
    all_attributes.extend(link_cm_attrs)


    grp_id_map, grp_resource_attrs, grp_cm_attrs = _add_resource_groups(net_i, network.resourcegroups)

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

    log.info("Resource attributes added in %s", get_timing(start_time))
    start_time = datetime.datetime.now()

    attr_ra_map = dict()
    for ra in resource_attrs:
        attr_ra_map[ra.db.resource_attr_id] = ra

    if network.scenarios is not None:
        log.info("Adding scenarios to network")
        for s in network.scenarios:
            scen = HydraIface.Scenario(network=net_i)
            scen.db.scenario_name        = s.name
            scen.db.scenario_description = s.description
            scen.db.start_time           = timestamp_to_ordinal(s.start_time)
            scen.db.end_time             = timestamp_to_ordinal(s.end_time)
            scen.db.time_step            = s.time_step
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
            datasets = scenario.bulk_insert_data(data, user_id)
            log.info("Data bulk insert took %s", get_timing(data_start_time))
            for i, ra_id in enumerate(resource_attr_ids):
                rs_i = HydraIface.ResourceScenario()
                rs_i.db.resource_attr_id = ra_id
                rs_i.db.dataset_id       = datasets[i].db.dataset_id
                rs_i.db.scenario_id      = scen.db.scenario_id
                rs_i.dataset = datasets[i]
                rs_i.resourceattr = attr_ra_map[ra_id]
                scen.resourcescenarios.append(rs_i)

            IfaceLib.bulk_insert(scen.resourcescenarios, 'tResourceScenario')

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

            log.info("Group items insert took %s", get_timing(item_start_time))
            net_i.scenarios.append(scen)

    log.info("Scenarios added in %s", get_timing(start_time))
    net_i.set_ownership(user_id)

    log.info("Insertion of network took: %s",(datetime.datetime.now()-insert_start))
    start_time = datetime.datetime.now()
    log.info("Network created. Creating complex model")

    log.info("Network conversion took: %s",get_timing(start_time))

    #log.debug("Return value: %s", return_value)
    return net_i.get_as_dict(**{'user_id':user_id}) 

def get_network(network_id, include_data='N', scenario_ids=None,**kwargs):
    """
        Return a whole network as a dictionary.
    """
    log.debug("getting network %s"%network_id)
    user_id = kwargs.get('user_id')
    net_i = HydraIface.Network(network_id = network_id)
    if net_i.load() is False:
        raise ResourceNotFoundError("Network (network_id=%s) not found." %
                                  network_id)

    net_i.check_read_permission(user_id)

    net = net_i.get_as_dict(include_attrs=False, **{'user_id':user_id})

    """
        Get The nodes & links.
    """

    nodes = net_i.get_nodes(as_dict=True)
    links = net_i.get_links(as_dict=True)
    groups = net_i.get_resourcegroups(as_dict=True)

    """
        Get all resource arrtibutes
    """

    if len(groups) == 0:
        group_string = ""
    else:
        group_string = "or  ref_key = 'GROUP'   and ref_id in %s" \
            % make_param(groups.keys())

    sql = """
        select
            resource_attr_id,
            attr_id,
            attr_is_var,
            ref_key,
            ref_id
        from
            tResourceAttr
        where
            ref_key = 'NETWORK' and ref_id = %(network_id)s
        or  ref_key = 'NODE'    and ref_id in %(node_ids)s
        or  ref_key = 'LINK'    and ref_id in %(link_ids)s
        %(group_ids)s
        order by ref_key
    """ % {
            'network_id' :network_id,
            'node_ids'   :make_param(nodes.keys()),
            'link_ids'   :make_param(links.keys()),
            'group_ids'  :group_string
    }
    ra_rs = HydraIface.execute(sql)

    for r in ra_rs:
        ra = dict(
            object_type = 'ResourceAttr',
            resource_attr_id = r.resource_attr_id,
            attr_id          = r.attr_id,
            attr_is_var      = r.attr_is_var,
            ref_key          = r.ref_key,
            ref_id           = r.ref_id,
        )
        if r.ref_key == 'NETWORK':
            net['attributes'].append(ra)
        elif r.ref_key == 'NODE':
            nodes[r.ref_id]['attributes'].append(ra)
        elif r.ref_key == 'LINK':
            links[r.ref_id]['attributes'].append(ra)
        elif r.ref_key == 'GROUP':
            groups[r.ref_id]['attributes'].append(ra)

    sql = """
        select
            rt.type_id,
            rt.ref_id,
            rt.ref_key,
            type.type_name,
            tmpl.template_name,
            tmpl.template_id
        from
            tResourceType rt,
            tTemplateType type,
            tTemplate tmpl
        where
        rt.type_id       = type.type_id
        and tmpl.template_id = type.template_id
        and (rt.ref_key = 'NETWORK' and rt.ref_id = %(network_id)s
        or  rt.ref_key = 'NODE'    and rt.ref_id in %(node_ids)s
        or  rt.ref_key = 'LINK'    and rt.ref_id in %(link_ids)s)
        order by ref_key
    """% {
            'network_id' :network_id,
            'node_ids'   :make_param(nodes.keys()),
            'link_ids'   :make_param(links.keys())
        }

    type_rs = HydraIface.execute(sql)

    for r in type_rs:
        type_summary = dict(
            object_type   = "TypeSummary",
            template_id   = r.template_id,
            template_name = r.template_name,
            type_id       = r.type_id,
            type_name     = r.type_name,
        )
        if r.ref_key == 'NETWORK':
            pass
            #net.types.append(TypeSummary(type_summary))
        elif r.ref_key == 'NODE':
            nodes[r.ref_id]['types'].append(type_summary)
        elif r.ref_key == 'LINK':
            links[r.ref_id]['types'].append(type_summary)

    net['nodes']          = nodes.values()
    net['links']          = links.values()
    net['resourcegroups'] = groups.values()

    """
        Get scenarios & resource scenarios.
    """
    scenario_dicts = net_i.get_all_scenarios(as_dict=True)
    if scenario_ids:
        restricted_scenarios = {}
        for scenario_id in scenario_ids:
            if scenario_dicts.get(scenario_id) is None:
                raise HydraError("Scenario ID %s not found", scenario_id)
            else:
                restricted_scenarios[scenario_id] = scenario_dicts[scenario_id]

        scenario_dicts = restricted_scenarios
    if len(scenario_dicts) > 0 and include_data.upper() == 'Y':

        sql = """
            select
                rs.dataset_id,
                rs.resource_attr_id,
                rs.scenario_id,
                ra.attr_id
            from
                tResourceScenario rs,
                tResourceAttr ra
            where
                rs.scenario_id in %(scenario_ids)s
                and ra.resource_attr_id = rs.resource_attr_id
        """ % {'scenario_ids' : make_param(scenario_dicts.keys())}

        res_scen_rs = HydraIface.execute(sql)
        all_dataset_ids = {}
        for res_scen_r in res_scen_rs:
            scenario = scenario_dicts[res_scen_r.scenario_id]
            rs = dict(
                object_type      = 'ResourceScenario',
                scenario_id      = res_scen_r.scenario_id,
                dataset_id       = res_scen_r.dataset_id,
                resource_attr_id = res_scen_r.resource_attr_id,
                attr_id          = res_scen_r.attr_id,
                value            = None,
            )
            scenario['resourcescenarios'].append(rs)
            if all_dataset_ids.get(res_scen_r.dataset_id) is None:
               all_dataset_ids[res_scen_r.dataset_id] = [rs]
            else:
                all_dataset_ids[res_scen_r.dataset_id].append(rs)

    if len(scenario_dicts) > 0 and include_data.upper() == 'Y':
        log.info("Getting Data")
        """
            Get data
        """

        sql = """
            select
                dataset_id,
                metadata_name,
                metadata_val
            from
                tMetadata
            where
                dataset_id in %(dataset_ids)s
        """ %  {'dataset_ids':make_param(all_dataset_ids.keys())}

        metadata_rs = HydraIface.execute(sql)
        metadata_dict = {}
        for mr in metadata_rs:
            m_dict = dict(
                object_type   = 'Metadata',
                metadata_name = mr.metadata_name,
                metadata_val  = mr.metadata_val,
                dataset_id    = mr.dataset_id
            )
            if mr.dataset_id in metadata_dict.keys():
                metadata_dict[mr.dataset_id].append(m_dict)
            else:
                metadata_dict[mr.dataset_id] = [m_dict]

        sql = """
            select
                d.dataset_id,
                d.data_id,
                d.data_type,
                d.data_units,
                d.data_dimen,
                d.data_name,
                d.data_hash,
                d.locked
            from
                tDataset d
            where
                d.dataset_id in %(dataset_ids)s
            """ % {'dataset_ids':make_param(all_dataset_ids.keys())}

        data_rs = HydraIface.execute(sql)
        for dr in data_rs:
            dataset = HydraIface.Dataset()
            dataset.db.dataset_id = dr.dataset_id
            dataset.db.data_id = dr.data_id
            dataset.db.data_type = dr.data_type
            dataset.db.data_units = dr.data_units
            dataset.db.data_dimen = dr.data_dimen
            dataset.db.data_name  = dr.data_name
            dataset.db.data_hash  = dr.data_hash
            dataset.db.locked     = dr.locked
            d = dataset.get_as_dict(user_id=user_id)
            d['metadatas']       = metadata_dict.get(dr.dataset_id, [])

            for rs in all_dataset_ids[dr.dataset_id]:
                rs['value'] = d

    """
        Resource Group Items
    """
    if len(scenario_dicts) > 0:
        sql = """
            select
               *
            from
                tResourceGroupItem
            where
                scenario_id in %(scenario_ids)s
            """%{'scenario_ids' : make_param(scenario_dicts.keys())}

        item_rs = HydraIface.execute(sql)

        for r in item_rs:
            r = r.get_as_dict()
            r['types'] = []
            r['object_type'] = 'ResourceGroupItem'
            scenario_dicts[r['scenario_id']]['resourcegroupitems'].append(r)

    net['scenarios'] = scenario_dicts.values()

    return net


def get_node(node_id,**kwargs): 
    user_id = kwargs.get('user_id')
    n = HydraIface.Node(node_id=node_id) 
    n.load_all() 
    return n.get_as_dict(**{'user_id':user_id}) 
 
def get_link(link_id,**kwargs): 
    user_id = kwargs.get('user_id')
    l = HydraIface.Link(link_id=link_id) 
    l.load_all() 
    return l.get_as_dict(**{'user_id':user_id})

def get_network_by_name(project_id, network_name,**kwargs):
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
        raise ResourceNotFoundError('No network named %s found in project %s'%(network_name, project_id))
    elif len(rs) > 1:
        log.warning("Multiple networks names %s found in project %s. Choosing first network in rs(network_id=%s)"%(network_name, project_id, rs[0].network_id))

    network_id = rs[0].network_id

    net = get_network(network_id, 'N', None, **kwargs)

    return net

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

    net_i = HydraIface.Network(network_id = network.id)
    net_i.check_write_permission(user_id)
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
    errors = []
    if network.scenarios is not None:
        for s in network.scenarios:
            if s.id is not None:
                if s.id > 0:
                    scen = HydraIface.Scenario(network=net_i, scenario_id=s.id)
                    if scen.db.locked == 'Y':
                        errors.append('Scenario %s was not updated as it is locked'%(s.id)) 
                        continue
                else:
                    scen = HydraIface.Scenario(network=net_i)
                    scenario_id = get_scenario_by_name(network.id, s.name)
                    s.name = s.name + "update" + str(datetime.datetime.now())
            else:
                scenario_id = get_scenario_by_name(network.id, s.name)
                scen = HydraIface.Scenario(scenario_id = scenario_id)

            scen.db.scenario_name        = s.name
            scen.db.scenario_description = s.description
            scen.db.start_time           = timestamp_to_ordinal(s.start_time)
            scen.db.end_time             = timestamp_to_ordinal(s.end_time)
            scen.db.time_step            = s.time_step
            scen.db.network_id           = net_i.db.network_id
            scen.save()

            for r_scen in s.resourcescenarios:
                r_scen.resource_attr_id = resource_attr_id_map[r_scen.resource_attr_id]

                scenario._update_resourcescenario(scen.db.scenario_id, r_scen)

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
                                     "resource group item."%group_item.ref_key)

                if ref_id is None:
                    raise HydraError("Invalid ref ID for group item!")

                group_item_i.db.ref_id = ref_id
                group_item_i.save()

    net_i.load_all()

    net = net_i.get_as_dict(**{'user_id':user_id})
    net['error'] = errors
    return net

def delete_network(network_id, purge_data,**kwargs):
    """
    Deletes a network. This does not remove the network from the DB. It
    just sets the status to 'X', meaning it can no longer be seen by the
    user.
    """
    user_id = kwargs.get('user_id')
    #check_perm(user_id, 'delete_network')
    net_i = HydraIface.Network(network_id = network_id)
    net_i.check_read_permission(user_id)
    net_i.db.status = 'X'
    net_i.save()
    net_i.commit()

    hdb.commit()

def get_network_extents(network_id,**kwargs):
    """
    Given a network, return its maximum extents.
    This would be the minimum x value of all nodes,
    the minimum y value of all nodes,
    the maximum x value of all nodes and
    maximum y value of all nodes.

    @returns NetworkExtents object
    """
    sql = """
        select
            node_x,
            node_y
        from
            tNode
        where
            network_id=%s
    """%network_id

    rs = HydraIface.execute(sql)
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
    net_i = HydraIface.Network(network_id = network_id)
    net_i.check_write_permission(user_id)

    node_i = HydraIface.Node()
    node_i.db.network_id = network_id
    node_i.db.node_name = node.name
    node_i.db.node_x    = node.x
    node_i.db.node_y    = node.y
    node_i.db.node_description = node.description
    node_i.save()
    node_i.load()

    _add_attributes(node_i, node.attributes)
    for resource_type in node.types:
        template.assign_type_to_resource(resource_type.id,
                                         'NODE',
                                         node_i.db.node_id,
                                         **kwargs)

    return node_i.get_as_dict(**{'user_id':user_id})

def update_node(node,**kwargs):
    """
    Update a node.
    If new attributes are present, they will be added to the node.
    The non-presence of attributes does not remove them.

    """
    user_id = kwargs.get('user_id')

    node_i = HydraIface.Node(node_id = node.id)

    net_i = HydraIface.Network(network_id = node_i.db.network_id)
    net_i.check_write_permission(user_id)

    node_i.db.node_name = node.name
    node_i.db.node_x    = node.x
    node_i.db.node_y    = node.y
    node_i.db.node_description = node.description

    _add_attributes(node_i, node.attributes)

    for resource_type in node.types:
        template.assign_type_to_resource(resource_type.id,
                                         'NODE',
                                         node_i.db.node_id,
                                        **kwargs)

    node_i.save()

    return node_i.get_as_dict(**{'user_id':user_id}) 

def delete_resourceattr(resource_attr_id, purge_data,**kwargs):
    """
        Deletes a resource attribute and all associated data.
    """
    ra = HydraIface.ResourceAttr(resource_attr_id = resource_attr_id)
    ra.load()
    ra.delete(purge_data)


def delete_node(node_id,**kwargs):
    """
        Set the status of a node to 'X'
    """
    user_id = kwargs.get('user_id')
    #check_perm(user_id, 'edit_topology')
    node_i = HydraIface.Node(node_id = node_id)

    net_i = HydraIface.Network(network_id=node_i.db.network_id)
    net_i.check_write_permission(user_id)

    node_i.db.status = 'node_i'
    node_i.save()
    node_i.commit()

    hdb.commit()

def purge_node(node_id, purge_data,**kwargs):
    """
        Remove node from DB completely
        If there are attributes on the node, use purge_data to try to
        delete the data. If no other resources link to this data, it
        will be deleted.

    """
    user_id = kwargs.get('user_id')
    node_i = HydraIface.Node(node_id = node_id)

    net_i = HydraIface.Network(network_id=node_i.db.network_id)
    net_i.check_write_permission(user_id)

    node_i.delete(purge_data=purge_data)
    node_i.save()
    node_i.commit()

def add_link(network_id, link,**kwargs):
    """
        Add a link to a network
    """
    user_id = kwargs.get('user_id')

    #check_perm(user_id, 'edit_topology')
    net_i = HydraIface.Network(network_id=network_id)
    net_i.check_write_permission(user_id)

    link_i = HydraIface.Link()

    link_i.db.network_id = network_id
    link_i.db.link_name = link.name
    link_i.db.node_1_id = link.node_1_id
    link_i.db.node_2_id = link.node_2_id
    link_i.db.link_description = link.description
    link_i.save()
    link_i.load()

    _add_attributes(link_i, link.attributes)
    for resource_type in link.types:
        template.assign_type_to_resource(resource_type.id,
                                         'LINK',
                                         link_i.db.link_id,
                                         **kwargs)

    return link_i.get_as_dict(**{'user_id':user_id})

def update_link(link,**kwargs):
    """
        Update a link.
    """
    user_id = kwargs.get('user_id')
    #check_perm(user_id, 'edit_topology')
    link_i = HydraIface.Link(link_id = link.id)

    net_i = HydraIface.Network(network_id=link_i.db.network_id)
    net_i.check_write_permission(user_id)

    link_i.db.link_name = link.name
    link_i.db.node_1_id = link.node_1_id
    link_i.db.node_2_id = link.node_2_id
    link_i.db.link_description = link.description

    _add_attributes(link_i, link.attributes)
    for resource_type in link.types:
        template.assign_type_to_resource(resource_type.id,
                                         'LINK',
                                         link_i.db.node_id,
                                         **kwargs)

    link_i.save()

    return link_i.get_as_dict(**{'user_id':user_id})

def delete_link(link_id,**kwargs):
    """
        Set the status of a link to 'X'
    """
    user_id = kwargs.get('user_id')
    #check_perm(user_id, 'edit_topology')
    link_i = HydraIface.Link(link_id = link_id)

    net_i = HydraIface.Network(network_id=link_i.db.network_id)
    net_i.check_write_permission(user_id)

    link_i.db.status = 'link_i'
    link_i.save()
    link_i.commit()

def purge_link(link_id, purge_data,**kwargs):
    """
        Remove link from DB completely
        If there are attributes on the link, use purge_data to try to
        delete the data. If no other resources link to this data, it
        will be deleted.
    """
    user_id = kwargs.get('user_id')
    link_i = HydraIface.Link(link_id = link_id)

    net_i = HydraIface.Network(network_id=link_i.db.network_id)
    net_i.check_write_permission(user_id)

    link_i.delete(purge_data=purge_data)
    link_i.save()
    link_i.commit()

def add_group(network_id, group,**kwargs):
    """
        Add a resourcegroup to a network
    """

    user_id = kwargs.get('user_id')
    net_i = HydraIface.Network(network_id=network_id)
    net_i.check_write_permission(user_id)

    res_grp_i = HydraIface.ResourceGroup()
    res_grp_i.db.network_id = network_id
    res_grp_i.db.group_name = group.name
    res_grp_i.db.group_description = group.description
    res_grp_i.db.status = group.status
    res_grp_i.save()

    _add_attributes(res_grp_i, group.attributes)
    _add_resource_types(res_grp_i, group.types)
    res_grp_i.commit()

    return res_grp_i.get_as_dict(**{'user_id':user_id}) 

def get_scenarios(network_id,**kwargs):
    """
        Get all the scenarios in a given network.
    """
    user_id = kwargs.get('user_id')
    net = HydraIface.Network(network_id=network_id)

    net_i = HydraIface.Network(network_id=network_id)
    net_i.check_read_permission(user_id)

    net.load_all()

    scenarios = []

    for scen in net.scenarios:
        scen.load()
        scenarios.append(scen.get_as_dict(**{'user_id':user_id}))

    return scenarios

def validate_network_topology(network_id,**kwargs):
    """
        Check for the presence of orphan nodes in a network.
    """

    net_i = HydraIface.Network(network_id=network_id)
    net_i.check_read_permission(kwargs.get('user_id'))

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
