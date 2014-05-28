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
from HydraLib.HydraException import HydraError, PermissionError
from db import HydraIface
from HydraLib import units
from db import IfaceLib
from db.hdb import make_param
from data import bulk_insert_data, parse_value, create_dict
from HydraLib.util import timestamp_to_ordinal

log = logging.getLogger(__name__)

def clone_constraint_group(constraint_id, grp_i):
    """
        This will clone not only the group in question
        but also any sub-groups and items.
    """

    grp_i.load_all()

    new_grp_i = HydraIface.ConstraintGroup()
    new_grp_i.db.constraint_id = constraint_id
    new_grp_i.db.op        = grp_i.db.op
    new_grp_i.db.ref_key_1 = grp_i.db.ref_key_1
    new_grp_i.db.ref_key_2 = grp_i.db.ref_key_2

    for subgroup in grp_i.get_groups():
        new_subgroup = clone_constraint_group(constraint_id, subgroup)
        if grp_i.db.ref_key_1 == 'GRP' and grp_i.db.ref_id_1 == subgroup.db.group_id:
            new_grp_i.db.ref_id_1 = new_subgroup.db.group_id
        if grp_i.db.ref_key_2 == 'GRP' and grp_i.db.ref_id_1 == subgroup.db.group_id:
            new_grp_i.db.ref_id_2 = new_subgroup.db.group_id

    for subitem in grp_i.get_items():
        new_subitem_i = HydraIface.ConstraintItem()
        new_subitem_i.db.constraint_id = constraint_id
        new_subitem_i.db.resource_attr_id = subitem.db.resource_attr_id
        new_subitem_i.db.constant         = subitem.db.constant
        new_subitem_i.save()
        new_subitem_i.load()
        if grp_i.db.ref_key_1 == 'ITEM' and grp_i.db.ref_id_1 == subitem.db.item_id:
            new_grp_i.db.ref_id_1 = new_subitem_i.db.item_id
        if grp_i.db.ref_key_2 == 'ITEM' and grp_i.db.ref_id_2 == subitem.db.item_id:
            new_grp_i.db.ref_id_2 = new_subitem_i.db.item_id

        new_grp_i.items.append(new_subitem_i)

    new_grp_i.save()
    new_grp_i.load()

    return new_grp_i

def get_scenario(scenario_id,**kwargs):
    """
        Get the specified scenario
    """

    user_id = kwargs.get('user_id')
    scen = HydraIface.Scenario(scenario_id=scenario_id)
    scen.load_all()
    ownership = scen.network.check_ownership(user_id) 
    if ownership['view'] == 'N':
        raise PermissionError("Permission denied."
                              " User %s cannot view scenario %s"%(user_id, scenario_id))
    data_args = {'include_data':'Y', 'user_id':user_id}
    return scen.get_as_dict(**data_args)

def add_scenario(network_id, scenario,**kwargs):
    """
        Add a scenario to a specified network.
    """
    user_id = kwargs.get('user_id')
    log.info("Adding scenarios to network")
    
    net_i = HydraIface.Network(network_id=network_id)
    ownership = net_i.check_ownership(user_id) 
    if ownership['edit'] == 'N':
        raise PermissionError("Permission denied."
                              " User %s cannot edit network %s"%(user_id, scenario.id))

    scen = HydraIface.Scenario()
    scen.db.scenario_name        = scenario.name
    scen.db.scenario_description = scenario.description
    scen.db.network_id           = network_id
    scen.db.start_time           = timestamp_to_ordinal(scenario.start_time)
    scen.db.end_time             = timestamp_to_ordinal(scenario.end_time)
    scen.db.time_step            = scenario.time_step

    #Just in case someone puts in a negative ID for the scenario.
    if scenario.id < 0:
        scenario.id = None

    scen.save()

    #extract the data from each resourcescenario so it can all be
    #inserted in one go, rather than one at a time
    all_data = [r.value for r in scenario.resourcescenarios]

    datasets = bulk_insert_data(all_data, user_id=user_id)

    #record all the resource attribute ids
    resource_attr_ids = [r.resource_attr_id for r in scenario.resourcescenarios]

    #get all the resource scenarios into a list and bulk insert them
    for i, ra_id in enumerate(resource_attr_ids):
        rs_i = HydraIface.ResourceScenario()
        rs_i.db.resource_attr_id = ra_id
        rs_i.db.dataset_id       = datasets[i].db.dataset_id
        rs_i.db.scenario_id      = scen.db.scenario_id
        rs_i.dataset = datasets[i]
        scen.resourcescenarios.append(rs_i)

    IfaceLib.bulk_insert(scen.resourcescenarios, 'tResourceScenario')

    #Again doing bulk insert.
    for group_item in scenario.resourcegroupitems:

        group_item_i = HydraIface.ResourceGroupItem()
        group_item_i.db.scenario_id = scen.db.scenario_id
        group_item_i.db.group_id    = group_item.group_id
        group_item_i.db.ref_key     = group_item.ref_key
        group_item_i.db.ref_id      = group_item.ref_id
        scen.resourcegroupitems.append(group_item_i)

    IfaceLib.bulk_insert(scen.resourcegroupitems, 'tResourceGroupItem')

    return scen

def update_scenario(scenario,**kwargs):
    """
        Update a single scenario
        as all resources already exist, there is no need to worry
        about negative IDS
    """
    user_id = kwargs.get('user_id')
    scen = HydraIface.Scenario(scenario_id=scenario.id)
   
    ownership = scen.network.check_ownership(user_id) 
    if ownership['edit'] == 'N':
        raise PermissionError("Permission denied."
                              " User %s cannot edit scenario %s"%(user_id, scenario.id))

    if scen.db.locked == 'Y':
        raise PermissionError('Scenario is locked. Unlock before editing.')
    
    scen.db.scenario_name = scenario.name
    scen.db.scenario_description = scenario.description
    scen.db.start_time           = timestamp_to_ordinal(scenario.start_time)
    scen.db.end_time             = timestamp_to_ordinal(scenario.end_time)
    scen.db.time_step            = scenario.time_step
    scen.save()

    for r_scen in scenario.resourcescenarios:
        _update_resourcescenario(scen.db.scenario_id, r_scen, user_id=user_id)

    #Get all the exiting resource group items for this scenario.
    #THen process all the items sent to this handler.
    #Any in the DB that are not passed in here are removed.
    sql = """
        select
            item_id
            from
            tResourceGroupItem
        where
            scenario_id = %s
    """ % (scenario.id)
    rs = HydraIface.execute(sql)
    all_items_before = [r.item_id for r in rs]

    #a list of all resourcegroupitems sent to the server.
    all_items_after = []

    for group_item in scenario.resourcegroupitems:

        if group_item.id and group_item.id > 0:
            group_item_i = HydraIface.ResourceGroupItem(item_id=group_item.id)
        else:
            group_item_i = HydraIface.ResourceGroupItem()
            group_item_i.db.scenario_id = scen.db.scenario_id
            group_item_i.db.group_id = group_item.group_id

        group_item_i.db.ref_key = group_item.ref_key
        group_item_i.db.ref_id =group_item.ref_id
        group_item_i.save()
        all_items_after.append(group_item_i.db.item_id)

    #remove any obsolete resource group items
    items_to_delete = set(all_items_before) - set(all_items_after)
    for item_id in items_to_delete:
        group_item_i = HydraIface.ResourceGroupItem(item_id=item_id)
        group_item_i.delete()
        group_item_i.save()

    scen.save()
    scen.load_all()
    return scen

def delete_scenario(scenario_id,**kwargs):
    """
        Set the status of a scenario to 'X'.
    """

    success = True
    
    _check_can_edit_scenario(scenario_id, kwargs['user_id'])

    scenario_i = HydraIface.Scenario(scenario_id = scenario_id)

    if scenario_i.load() is False:
        raise HydraError("Scenario %s does not exist."%(scenario_id))

    scenario_i.db.status = 'X'
    scenario_i.save()

    return success


def clone_scenario(scenario_id,**kwargs):
    scen_i = HydraIface.Scenario(scenario_id = scenario_id)
    if scen_i.load_all() is False:
        raise HydraError("Scenario %s does not exist."%(scenario_id))

    cloned_scen = HydraIface.Scenario()
    cloned_scen.db.network_id           = scen_i.db.network_id
    cloned_scen.db.scenario_name        = "%s (clone)"%(scen_i.db.scenario_name)
    cloned_scen.db.scenario_description = scen_i.db.scenario_description

    cloned_scen.db.start_time           = scen_i.db.start_time
    cloned_scen.db.end_time             = scen_i.db.end_time
    cloned_scen.db.time_step            = scen_i.db.time_step

    cloned_scen.save()
    cloned_scen.load()

    for rs in scen_i.resourcescenarios:
        new_rs = HydraIface.ResourceScenario()
        new_rs.db.scenario_id = cloned_scen.db.scenario_id
        new_rs.db.resource_attr_id = rs.db.resource_attr_id
        new_rs.db.dataset_id       = rs.db.dataset_id
        cloned_scen.resourcescenarios.append(new_rs)

    IfaceLib.bulk_insert(cloned_scen.resourcescenarios, "tResourceScenario")

    for constraint_i in scen_i.constraints:
        new_constraint = HydraIface.Constraint()
        new_constraint.db.scenario_id = cloned_scen.db.scenario_id
        new_constraint.db.op          = constraint_i.db.op
        new_constraint.db.constant    = constraint_i.db.constant
        new_constraint.save()
        new_constraint.load()

        grp_i = HydraIface.ConstraintGroup(constraint=constraint_i,
                                           group_id=constraint_i.db.group_id)

        new_grp = clone_constraint_group(new_constraint.db.constraint_id, grp_i)

        new_constraint.db.group_id = new_grp.db.group_id
        new_constraint.save()

    for resourcegroupitem_i in scen_i.resourcegroupitems:
        new_resourcegroupitem_i = HydraIface.ResourceGroupItem()
        new_resourcegroupitem_i.db.scenario_id = cloned_scen.db.scenario_id
        new_resourcegroupitem_i.db.ref_key     = resourcegroupitem_i.db.ref_key
        new_resourcegroupitem_i.db.ref_id      = resourcegroupitem_i.db.ref_id
        new_resourcegroupitem_i.db.group_id    = resourcegroupitem_i.db.group_id
        new_resourcegroupitem_i.save()

    cloned_scen.load_all()

    return cloned_scen

def compare_scenarios(scenario_id_1, scenario_id_2,**kwargs):
    user_id = kwargs.get('user_id')
    scenario_1 = HydraIface.Scenario(scenario_id=scenario_id_1)
    scenario_1.load_all()

    scenario_2 = HydraIface.Scenario(scenario_id=scenario_id_2)
    scenario_2.load_all()

    if scenario_1.db.network_id != scenario_2.db.network_id:
        raise HydraIface("Cannot compare scenarios that are not"
                         " in the same network!")

    scenariodiff = dict(
       object_type = 'ScenarioDiff' 
    ) 
    resource_diffs = []

    #Make a list of all the resource scenarios (aka data) that are unique
    #to scenario 1 and that are in both scenarios, but are not the same.

    #For efficiency, build a dictionary of the data in scenarios and refer
    #them rather than nesting for loops.
    r_scen_1_dict = dict()
    r_scen_2_dict = dict()
    for s1_rs in scenario_1.resourcescenarios:
        r_scen_1_dict[s1_rs.db.resource_attr_id] = s1_rs
    for s2_rs in scenario_2.resourcescenarios:
        r_scen_2_dict[s2_rs.db.resource_attr_id] = s2_rs

    rscen_1_dataset_ids = set([r_scen.db.dataset_id for r_scen in scenario_1.resourcescenarios])
    rscen_2_dataset_ids = set([r_scen.db.dataset_id for r_scen in scenario_2.resourcescenarios])

    log.info("In 1 not in 2: %s"%(rscen_1_dataset_ids - rscen_2_dataset_ids))
    log.info("In 2 not in 1: %s"%(rscen_2_dataset_ids - rscen_1_dataset_ids))

    for ra_id, s1_rs in r_scen_1_dict.items():
        s2_rs = r_scen_2_dict.get(ra_id)
        if s2_rs is not None:
            log.debug("Is %s == %s?"%(s1_rs.db.dataset_id, s2_rs.db.dataset_id))
            if s1_rs.db.dataset_id != s2_rs.db.dataset_id:
                resource_diff = dict( 
                    object_type      = 'ResourceScenarioDiff',
                    resource_attr_id = s1_rs.db.resource_attr_id,
                    scenario_1_dataset = s1_rs.get_dataset().get_as_dict(**{'user_id':user_id}),
                    scenario_2_dataset = s2_rs.get_dataset().get_as_dict(**{'user_id':user_id}),
                )
                resource_diffs.append(resource_diff)

            continue
        else:
            resource_diff = dict( 
                object_type      = 'ResourceScenarioDiff',
                resource_attr_id = s1_rs.db.resource_attr_id,
                scenario_1_dataset = s1_rs.get_dataset().get_as_dict(**{'user_id':user_id}),
                scenario_2_dataset = None,
            )
            resource_diffs.append(resource_diff)

    #make a list of all the resource scenarios (aka data) that are unique
    #in scenario 2.
    for ra_id, s2_rs in r_scen_2_dict.items():
        s1_rs = r_scen_1_dict.get(ra_id)
        if s1_rs is None:
            resource_diff = dict( 
                object_type      = 'ResourceScenarioDiff',
                resource_attr_id = s1_rs.db.resource_attr_id,
                scenario_1_dataset = None,
                scenario_2_dataset = s2_rs.get_dataset().get_as_dict(**{'user_id':user_id}),
            )
            resource_diffs.append(resource_diff)

    scenariodiff['resourcescenarios'] = resource_diffs

    #Now compare groups.
    #Return list of group items in scenario 1 not in scenario 2 and vice versa
    s1_items = []
    for s1_item in scenario_1.resourcegroupitems:
        s1_items.append((s1_item.db.group_id, s1_item.db.ref_key, s1_item.db.ref_id))
    s2_items = []
    for s2_item in scenario_2.resourcegroupitems:
        s2_items.append((s2_item.db.group_id, s2_item.db.ref_key, s2_item.db.ref_id))

    groupdiff = dict()
    scenario_1_items = []
    scenario_2_items = []
    for s1_only_item in set(s1_items) - set(s2_items):
        item = dict(
            group_id = s1_only_item[0],
            ref_key  = s1_only_item[1],
            ref_id   = s1_only_item[2],
        )
        scenario_1_items.append(item)
    for s2_only_item in set(s2_items) - set(s1_items):
        item = dict(
            group_id = s2_only_item[0],
            ref_key  = s2_only_item[1],
            ref_id   = s2_only_item[2],
        )
        scenario_2_items.append(item)

    groupdiff['scenario_1_items'] = scenario_1_items
    groupdiff['scenario_2_items'] = scenario_2_items
    scenariodiff['groups'] = groupdiff

    return scenariodiff

def lock_scenario(scenario_id, **kwargs):
    #user_id = kwargs.get('user_id')
    #check_perm(user_id, 'edit_network')
   
    scenario_i = HydraIface.Scenario(scenario_id=scenario_id)
    ownership = scenario_i.network.check_ownership(kwargs['user_id']) 
    if ownership['edit'] == 'Y':
        scenario_i.db.locked = 'Y'
        scenario_i.save()
    else:
        raise PermissionError('User %s cannot lock scenario %s' % (kwargs['user_id'], scenario_id))

    return 'OK'

def unlock_scenario(scenario_id, **kwargs):
    #user_id = kwargs.get('user_id')
    #check_perm(user_id, 'edit_network')
   
    scenario_i = HydraIface.Scenario(scenario_id=scenario_id)
    ownership = scenario_i.network.check_ownership(kwargs['user_id']) 
    if ownership['edit'] == 'Y':
        scenario_i.db.locked = 'N'
        scenario_i.save()
    else:
        raise PermissionError('User %s cannot unlock scenario %s' % (kwargs['user_id'], scenario_id))

    return 'OK'



def update_resourcedata(scenario_id, resource_scenario,**kwargs):
    """
        Update the data associated with a scenario.
        Data missing from the resource scenario will not be removed
        from the scenario. Use the remove_resourcedata for this task.
    """
    user_id = kwargs.get('user_id')
    res = None
    
    _check_can_edit_scenario(scenario_id, kwargs['user_id'])

    if resource_scenario.value is not None:
        res = _update_resourcescenario(scenario_id, resource_scenario, user_id=user_id)
        if res is None:
            raise HydraError("Could not update resource data. No value "
                "sent with data. Check privilages.")

    return res

def delete_resourcedata(scenario_id, resource_scenario,**kwargs):
    """
        Remove the data associated with a resource in a scenario.
    """

    _check_can_edit_scenario(scenario_id, kwargs['user_id'])

    _delete_resourcescenario(scenario_id, resource_scenario)

def get_dataset(dataset_id,**kwargs):
    """
        Get a single dataset, by ID
    """

    if dataset_id is None:
        return None

    sd_i = HydraIface.Dataset(dataset_id=dataset_id)
    sd_i.load()
    dataset = sd_i

    return dataset

def _delete_resourcescenario(scenario_id, resource_scenario):

    ra_id = resource_scenario.resource_attr_id
    sd = HydraIface.ResourceScenario(scenario_id=scenario_id, resource_attr_id=ra_id)
    sd.delete()

def _update_resourcescenario(scenario_id, resource_scenario, new=False, user_id=None):
    """
        Insert or Update the value of a resource's attribute by first getting the
        resource, then parsing the input data, then assigning the value.

        returns a HydraIface.ResourceScenario object.
    """
    ra_id = resource_scenario.resource_attr_id
    r_scen_i = HydraIface.ResourceScenario(scenario_id=scenario_id,
                                           resource_attr_id=ra_id)

    data_type = resource_scenario.value.type.lower()

    value = parse_value(resource_scenario.value)

    if value is None:
        log.info("Cannot set data on resource attribute %s",ra_id)
        return None

    metadata  = {}
    for m in resource_scenario.value.metadata:
        metadata[m.name]  = m.value
    name      = resource_scenario.value.name

    dimension = resource_scenario.value.dimension
    data_unit = resource_scenario.value.unit

    unit = units.Units()

    # Assign dimension if necessary
    # It happens that dimension is and empty string. We set it to
    # None to achieve consistency in the DB.
    if data_unit is not None and dimension is None or \
            data_unit is not None and len(dimension) == 0:
        dimension = unit.get_dimension(data_unit)
    else:
        if dimension is None or len(dimension) == 0:
            dimension = None

    r_scen_i.assign_value(data_type, value, data_unit, name, dimension, 
                          metadata=metadata, new=new, user_id=user_id)

    return r_scen_i 


def add_data_to_attribute(scenario_id, resource_attr_id, dataset,**kwargs):
    """
            Add data to a resource scenario outside of a network update
    """
    user_id = kwargs.get('user_id')

    _check_can_edit_scenario(scenario_id, user_id)

    r_scen_i = HydraIface.ResourceScenario(scenario_id=scenario_id,
                                           resource_attr_id=resource_attr_id)
    data_type = dataset.type.lower()

    value = parse_value(dataset)

    dataset_metadata = {}
    if dataset.metadata is not None:
        for m in dataset.metadata:
            dataset_metadata[m.name] = m.value

    if value is None:
        raise HydraError("Cannot set value to attribute. "
            "No value was sent with dataset %s", dataset.id)

    user_id = user_id

    r_scen_i.assign_value(data_type, value, dataset.unit, dataset.name, dataset.dimension,
                          metadata=dataset_metadata, new=False, user_id=user_id)

    r_scen_i.load_all()

    return r_scen_i

def get_scenario_data(scenario_id,**kwargs):
    """
        Get all the datasets from the group with the specified name
        @returns a list of dictionaries
    """
    user_id = kwargs.get('user_id')
    scenario_data = []

    sql = """
        select
            d.dataset_id,
            d.data_id,
            d.data_type,
            d.data_units,
            d.data_dimen,
            d.data_name,
            d.data_hash,
            d.locked,
            case when a.arr_data is not null then a.arr_data
                 when eq.arr_data is not null then eq.arr_data
                 when ds.desc_val is not null then ds.desc_val
                 when sc.param_value is not null then sc.param_value
            else null
            end as value,
            eq.frequency,
            eq.start_time
        from
            tDataset d
            left join tArray a on (
                d.data_type = 'array'
            and a.data_id   = d.data_id
            )
            left join tDescriptor ds on (
                d.data_type = 'descriptor'
            and ds.data_id  = d.data_id
            )
            left join tScalar sc on (
                d.data_type = 'scalar'
            and sc.data_id  = d.data_id
            )
            left join tEqTimeSeries eq on (
                d.data_type = 'eqtimeseries'
            and eq.data_id  = d.data_id
            )
        where
            d.dataset_id  in (
                select
                    distinct(dataset_id)
                from
                    tResourceScenario
                where
                    scenario_id = %s
                )
    """ % scenario_id


    data_rs = HydraIface.execute(sql)

    sql = """
        select
            metadata_name,
            metadata_val,
            dataset_id
        from
            tMetadata
        where
            dataset_id  in (
                select
                    distinct(dataset_id)
                from
                    tResourceScenario
                where
                    scenario_id = %s
                )
    """ % scenario_id

    metadata_rs = HydraIface.execute(sql)
    metadata = {}
    for m in metadata_rs:
        metadata_dict = dict(
            object_type   = 'Metadata',
            metadata_name = m.metadata_name,
            metadata_val  = m.metadata_val,
            dataset_id    = m.dataset_id
        )
        if metadata.get(m.dataset_id):
            metadata[m.dataset_id].append(metadata_dict)
        else:
            metadata[m.dataset_id] = [metadata_dict]

    for dr in data_rs:
        if dr.data_type in ('scalar', 'array', 'descriptor'):
            if dr.data_type == 'scalar':
                val = {
                    'object_type' : 'Scalar',
                    'param_value' : dr.value,
                    'metadatas'  : metadata.get(dr.dataset_id, {}),
                }
            elif dr.data_type == 'descriptor':
                val = {
                    'object_type': 'Descriptor',
                    'desc_val'   : dr.value,
                    'metadatas'  : metadata.get(dr.dataset_id, {}),
                }
            elif dr.data_type == 'array':
                val = {
                    'object_type': 'Array',
                    'arr_data'      : create_dict(eval(dr.value)),
                    'metadatas'  : metadata.get(dr.dataset_id, {}),
                }
            d = dict(
                dataset_id = dr.dataset_id,
                data_id = dr.data_id,
                data_type = dr.data_type,
                data_units = dr.data_units,
                data_dimen = dr.data_dimen,
                data_name  = dr.data_name,
                data_hash  = dr.data_hash,
                locked     = dr.locked,
                value      = val,
            )
        else:
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
            d['metadatas']     = metadata.get(dr.dataset_id, [])

        scenario_data.append(d)
    log.info("Retrieved %s datasets", len(scenario_data))
    return scenario_data


def get_resource_data(ref_key, ref_id, scenario_id, type_id,**kwargs):
    """
        Get all the resource scenarios for a given resource 
        in a given scenario. If type_id is specified, only
        return the resource scenarios for the attributes
        within the type.
    """

    user_id = kwargs.get('user_id')
    attr_string = ''
    
    if type_id is not None:
        sql = """
            select
                attr_id
            from
                tTypeAttr
            where
                type_id=%(type_id)s
                """%{'type_id':type_id}
        rs = HydraIface.execute(sql)
        type_attrs = [r.attr_id for r in rs]
        attr_string = "and ra.attr_id in %s"%(make_param(type_attrs),)

    sql = """
        select
            rs.dataset_id,
            rs.resource_attr_id,
            rs.scenario_id,
            ra.attr_id
        from
            tResourceAttr ra,
            tResourceScenario rs
        where
            ra.ref_key = '%(ref_key)s'
            and ra.ref_id  = %(ref_id)s
            and rs.resource_attr_id = ra.resource_attr_id
            and rs.scenario_id = %(scenario_id)s
            %(attr_filter)s
        """ % {'scenario_id' : scenario_id,
               'ref_key'     : ref_key,
               'ref_id'     : ref_id,
               'attr_filter' :attr_string
              }

    res_scen_rs = HydraIface.execute(sql)
    all_dataset_ids = {}
    for res_scen_r in res_scen_rs:
        rs = dict(
            object_type      = 'ResourceScenario',
            scenario_id      = res_scen_r.scenario_id,
            dataset_id       = res_scen_r.dataset_id,
            resource_attr_id = res_scen_r.resource_attr_id,
            attr_id          = res_scen_r.attr_id,
            value            = None,
        )
        if all_dataset_ids.get(res_scen_r.dataset_id) is None:
           all_dataset_ids[res_scen_r.dataset_id] = [rs]
        else:
            all_dataset_ids[res_scen_r.dataset_id].append(rs)

    sql = """
        select
            d.dataset_id,
            d.data_id,
            d.data_type,
            d.data_units,
            d.data_dimen,
            d.data_name,
            d.data_hash,
            d.locked,
            case when a.arr_data is not null then a.arr_data
                 when eq.arr_data is not null then eq.arr_data
                 when ds.desc_val is not null then ds.desc_val
                 when sc.param_value is not null then sc.param_value
            else null
            end as value,
            eq.frequency,
            eq.start_time
        from
            tDataset d
            left join tArray a on (
                d.data_type = 'array'
            and a.data_id   = d.data_id
            )
            left join tDescriptor ds on (
                d.data_type = 'descriptor'
            and ds.data_id  = d.data_id
            )
            left join tScalar sc on (
                d.data_type = 'scalar'
            and sc.data_id  = d.data_id
            )
            left join tEqTimeSeries eq on (
                d.data_type = 'eqtimeseries'
            and eq.data_id  = d.data_id
            )
        where
            d.dataset_id  in (
                select
                    distinct(rs.dataset_id)
                from
                    tResourceScenario rs,
                    tResourceAttr ra
                where
                    ra.ref_key = '%(ref_key)s'
                    and ra.ref_id = %(ref_id)s 
                    and rs.scenario_id = %(scenario_id)s
                    and rs.resource_attr_id = ra.resource_attr_id
                    %(attr_filter)s
                )
    """ % {
            'scenario_id' : scenario_id,
            'ref_key'     : ref_key,
            'ref_id'      : ref_id,
            'attr_filter' : attr_string,
        }


    data_rs = HydraIface.execute(sql)


    sql = """
        select
            metadata_name,
            metadata_val,
            dataset_id
        from
            tMetadata
        where
            dataset_id  in (
                select
                    distinct(rs.dataset_id)
                from
                    tResourceScenario rs,
                    tResourceAttr ra
                where
                    ra.ref_key = '%(ref_key)s'
                    and ra.ref_id = %(ref_id)s 
                    and rs.scenario_id = %(scenario_id)s
                    and rs.resource_attr_id = ra.resource_attr_id
                    %(attr_filter)s
                )
    """ % {
            'scenario_id' : scenario_id,
            'ref_key'     : ref_key,
            'ref_id'      : ref_id,
            'attr_filter' : attr_string,
        } 

    metadata_rs = HydraIface.execute(sql)
    metadata = {}
    for m in metadata_rs:
        metadata_dict = dict(
            object_type   = 'Metadata',
            metadata_name = m.metadata_name,
            metadata_val  = m.metadata_val,
            dataset_id    = m.dataset_id
        )
        if metadata.get(m.dataset_id):
            metadata[m.dataset_id].append(metadata_dict)
        else:
            metadata[m.dataset_id] = [metadata_dict]


    resource_scenarios = [] 
    for dr in data_rs:
        if dr.data_type in ('scalar', 'array', 'descriptor'):
            if dr.data_type == 'scalar':
                val = {
                    'object_type' : 'Scalar',
                    'param_value' : dr.value,
                    'metadatas'  : metadata.get(dr.dataset_id, {}),
                }
            elif dr.data_type == 'descriptor':
                val = {
                    'object_type': 'Descriptor',
                    'desc_val'   : dr.value,
                    'metadatas'  : metadata.get(dr.dataset_id, {}),
                }
            elif dr.data_type == 'array':
                val = {
                    'object_type': 'Array',
                    'arr_data'      : dr.value,
                    'metadatas'  : metadata.get(dr.dataset_id, {}),
                }
            d = dict(
                dataset_id = dr.dataset_id,
                data_id = dr.data_id,
                data_type = dr.data_type,
                data_units = dr.data_units,
                data_dimen = dr.data_dimen,
                data_name  = dr.data_name,
                data_hash  = dr.data_hash,
                locked     = dr.locked,
                value      = val 
            )
        else:
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
        for res_scen in all_dataset_ids[dr.dataset_id]:
            res_scen['value'] = d 
            resource_scenarios.append(res_scen)

    log.info("Retrieved %s resource scenarios for %s %s", 
                 len(resource_scenarios), ref_key,ref_id)

    return resource_scenarios 

def _check_can_edit_scenario(scenario_id, user_id):
    scenario_i = HydraIface.Scenario(scenario_id=scenario_id)
    
    scenario_i.network.check_write_permission(user_id)

    if scenario_i.db.locked == 'Y':
        raise PermissionError('Cannot update scenario %s as it is locked.'%(scenario_id))



