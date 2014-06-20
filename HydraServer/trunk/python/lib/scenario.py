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
from HydraLib.HydraException import HydraError, PermissionError, ResourceNotFoundError
from HydraLib import units
from db import DBSession
from db.model import Scenario, ResourceGroupItem, ResourceScenario, TypeAttr, ResourceAttr, NetworkOwner, Dataset
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import or_
from sqlalchemy.orm import joinedload_all
import data
from HydraLib.util import timestamp_to_ordinal
from collections import namedtuple
from copy import deepcopy

log = logging.getLogger(__name__)

def _check_network_ownership(network_id, user_id):
    try:
        netowner = DBSession.query(NetworkOwner).filter(NetworkOwner.network_id==network_id
                                                         and NetworkOwner.user_id==user_id).one()
        if netowner.edit == 'N':
            raise PermissionError("Permission denied."
                                " User %s cannot edit network %s"%(user_id,network_id))
    except NoResultFound:
        raise PermissionError("Permission denied."
                            " User %s is not an owner of network"%(user_id, network_id))

def _get_scenario(scenario_id):
    try:
        scenario = DBSession.query(Scenario).filter(Scenario.scenario_id==scenario_id).one()
        return scenario
    except NoResultFound:
        raise ResourceNotFoundError("Scenario %s does not exist."%(scenario_id))

def get_scenario(scenario_id,**kwargs):
    """
        Get the specified scenario
    """

    user_id = kwargs.get('user_id')

    scen = _get_scenario(scenario_id)
    owner = _check_network_owner(scen.network, user_id) 
    if owner.view == 'N':
        raise PermissionError("Permission denied."
                              " User %s cannot view scenario %s"%(user_id, scenario_id))
    #TODO: need to filter out data here...
    return scen

def add_scenario(network_id, scenario,**kwargs):
    """
        Add a scenario to a specified network.
    """
    user_id = int(kwargs.get('user_id'))
    log.info("Adding scenarios to network")

    _check_network_ownership(network_id, user_id) 

    scen = Scenario()
    scen.scenario_name        = scenario.name
    scen.scenario_description = scenario.description
    scen.network_id           = network_id
    scen.start_time           = str(timestamp_to_ordinal(scenario.start_time)) if scenario.start_time else None
    scen.end_time             = str(timestamp_to_ordinal(scenario.end_time)) if scenario.end_time else None
    scen.time_step            = scenario.time_step

    #Just in case someone puts in a negative ID for the scenario.
    if scenario.id < 0:
        scenario.id = None

    #extract the data from each resourcescenario so it can all be
    #inserted in one go, rather than one at a time
    all_data = [r.value for r in scenario.resourcescenarios]

    datasets = data._bulk_insert_data(all_data, user_id=user_id)

    #record all the resource attribute ids
    resource_attr_ids = [r.resource_attr_id for r in scenario.resourcescenarios]

    #get all the resource scenarios into a list and bulk insert them
    for i, ra_id in enumerate(resource_attr_ids):
        rs_i = ResourceScenario()
        rs_i.resource_attr_id = ra_id
        rs_i.dataset_id       = datasets[i].dataset_id
        rs_i.scenario_id      = scen.scenario_id
        rs_i.dataset = datasets[i]
        scen.resourcescenarios.append(rs_i)

    #Again doing bulk insert.
    for group_item in scenario.resourcegroupitems:
        group_item_i = ResourceGroupItem()
        group_item_i.scenario_id = scen.scenario_id
        group_item_i.group_id    = group_item.group_id
        group_item_i.ref_key     = group_item.ref_key
        if group_item.ref_key == 'NODE':
            group_item_i.node_id      = group_item.ref_id
        elif group_item.ref_key == 'LINK':
            group_item_i.link_id      = group_item.ref_id
        elif group_item.ref_key == 'GROUP':
            group_item_i.subgroup_id  = group_item.ref_id
        scen.resourcegroupitems.append(group_item_i)

    DBSession.flush()
    return scen

def update_scenario(scenario,**kwargs):
    """
        Update a single scenario
        as all resources already exist, there is no need to worry
        about negative IDS
    """
    user_id = kwargs.get('user_id')
    scen = _get_scenario(scenario.id)
   
    _check_network_ownership(scenario.network_id, user_id) 

    if scen.locked == 'Y':
        raise PermissionError('Scenario is locked. Unlock before editing.')
    
    scen.scenario_name        = scenario.name
    scen.scenario_description = scenario.description
    scen.start_time           = str(timestamp_to_ordinal(scenario.start_time)) if scenario.start_time else None
    scen.end_time             = str(timestamp_to_ordinal(scenario.end_time)) if scenario.end_time else None
    scen.time_step            = scenario.time_step

    for r_scen in scenario.resourcescenarios:
        _update_resourcescenario(scen, r_scen, user_id=user_id)
        DBSession.flush()

    #Get all the exiting resource group items for this scenario.
    #THen process all the items sent to this handler.
    #Any in the DB that are not passed in here are removed.
    for group_item in scenario.resourcegroupitems:

        if group_item.id and group_item.id > 0:
            try:
                group_item_i = DBSession.query(ResourceGroupItem).filter(ResourceGroupItem.item_id==group_item.id).one()
            except NoResultFound:
                raise ResourceNotFoundError("ResourceGroupItem %s not found" % (group_item.id))

        else:
            group_item_i = ResourceGroupItem()
            group_item_i.group_id = group_item.group_id

        ref_key = group_item.ref_key
        group_item_i.ref_key = ref_key
        if ref_key == 'NODE':
            group_item_i.node_id =group_item.ref_id
        elif ref_key == 'LINK':
            group_item_i.link_id =group_item.ref_id
        elif ref_key == 'GROUP':
            group_item_i.subgroup_id =group_item.ref_id

        if group_item.id is None or group_item.id < 0:
            scenario.resourcegroupitems.append(group_item_i)
    DBSession.flush()
    return scen

def delete_scenario(scenario_id,**kwargs):
    """
        Set the status of a scenario to 'X'.
    """

    success = True
    
    _check_can_edit_scenario(scenario_id, kwargs['user_id'])
    scenario_i = _get_scenario(scenario_id)

    scenario_i.status = 'X'
    DBSession.flush()
    return success


def clone_scenario(scenario_id,**kwargs):
    scen_i = _get_scenario(scenario_id)

    cloned_scen = Scenario()
    cloned_scen.network_id           = scen_i.network_id
    cloned_scen.scenario_name        = "%s (clone)"%(scen_i.scenario_name)
    cloned_scen.scenario_description = scen_i.scenario_description

    cloned_scen.start_time           = scen_i.start_time
    cloned_scen.end_time             = scen_i.end_time
    cloned_scen.time_step            = scen_i.time_step

    for rs in scen_i.resourcescenarios:
        new_rs = ResourceScenario()
        new_rs.resource_attr_id = rs.resource_attr_id
        new_rs.dataset_id       = rs.dataset_id
        cloned_scen.resourcescenarios.append(new_rs)

    for resourcegroupitem_i in scen_i.resourcegroupitems:
        new_resourcegroupitem_i = ResourceGroupItem()
        new_resourcegroupitem_i.ref_key     = resourcegroupitem_i.ref_key
        new_resourcegroupitem_i.link_id      = resourcegroupitem_i.link_id
        new_resourcegroupitem_i.node_id      = resourcegroupitem_i.node_id
        new_resourcegroupitem_i.subgroup_id      = resourcegroupitem_i.subgroup_id
        new_resourcegroupitem_i.group_id    = resourcegroupitem_i.group_id
        cloned_scen.resourcegroupitems.append(new_resourcegroupitem_i)

    DBSession.add(cloned_scen)
    DBSession.flush()

    return cloned_scen

def _get_dataset_as_dict(rs, user_id):
    if rs.dataset is None:
        return None
    
    dataset = deepcopy(rs.dataset.__dict__)


    del dataset['_sa_instance_state']
    
    try:
        rs.dataset.check_read_permission(user_id)
    except PermissionError:
           dataset['value']      = None
           dataset['frequency']  = None
           dataset['start_time'] = None
           if dataset['data_type'] == 'timeseries':
               dataset['timeseriesdata'] = []
           dataset['metadata'] = []
    
    dataset['metadata'] = [_get_as_obj(m.__dict__, 'Metadata') for m in rs.dataset.metadata]
    dataset['timeseriesdata'] = [_get_as_obj(m.__dict__, 'TimeSeriesData') for m in rs.dataset.timeseriesdata]
    
    return dataset

def _get_as_obj(obj_dict, name):
    """
        Turn a dictionary into a named tuple so it can be
        passed into the constructor of a complex model generator.
    """
    if obj_dict.get('_sa_instance_state'):
        del obj_dict['_sa_instance_state']
    obj = namedtuple(name, tuple(obj_dict.keys()))
    for k, v in obj_dict.items():
        setattr(obj, k, v)
        log.info("%s = %s",k,getattr(obj,k))
    return obj


def compare_scenarios(scenario_id_1, scenario_id_2,**kwargs):
    user_id = kwargs.get('user_id')

    scenario_1 = _get_scenario(scenario_id_1)
    scenario_2 = _get_scenario(scenario_id_2)

    if scenario_1.network_id != scenario_2.network_id:
        raise HydraError("Cannot compare scenarios that are not"
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
        r_scen_1_dict[s1_rs.resource_attr_id] = s1_rs
    for s2_rs in scenario_2.resourcescenarios:
        r_scen_2_dict[s2_rs.resource_attr_id] = s2_rs

    rscen_1_dataset_ids = set([r_scen.dataset_id for r_scen in scenario_1.resourcescenarios])
    rscen_2_dataset_ids = set([r_scen.dataset_id for r_scen in scenario_2.resourcescenarios])

    log.info("Datasets In 1 not in 2: %s"%(rscen_1_dataset_ids - rscen_2_dataset_ids))
    log.info("Datasets In 2 not in 1: %s"%(rscen_2_dataset_ids - rscen_1_dataset_ids))

    for ra_id, s1_rs in r_scen_1_dict.items():
        s2_rs = r_scen_2_dict.get(ra_id)
        if s2_rs is not None:
            log.debug("Is %s == %s?"%(s1_rs.dataset_id, s2_rs.dataset_id))
            if s1_rs.dataset_id != s2_rs.dataset_id:
                resource_diff = dict( 
                    resource_attr_id = s1_rs.resource_attr_id,
                    scenario_1_dataset = _get_as_obj(_get_dataset_as_dict(s1_rs, user_id), 'Dataset'),
                    scenario_2_dataset = _get_as_obj(_get_dataset_as_dict(s2_rs, user_id), 'Dataset'),
                )
                resource_diffs.append(resource_diff)

            continue
        else:
            resource_diff = dict( 
                resource_attr_id = s1_rs.resource_attr_id,
                scenario_1_dataset = _get_as_obj(_get_dataset_as_dict(s1_rs, user_id), 'Dataset'),
                scenario_2_dataset = None,
            )
            resource_diffs.append(resource_diff)

    #make a list of all the resource scenarios (aka data) that are unique
    #in scenario 2.
    for ra_id, s2_rs in r_scen_2_dict.items():
        s1_rs = r_scen_1_dict.get(ra_id)
        if s1_rs is None:
            resource_diff = dict( 
                resource_attr_id = s1_rs.resource_attr_id,
                scenario_1_dataset = None,
                scenario_2_dataset = _get_as_obj(_get_dataset_as_dict(s2_rs, user_id), 'Dataset'),
            )
            resource_diffs.append(resource_diff)

    scenariodiff['resourcescenarios'] = resource_diffs

    #Now compare groups.
    #Return list of group items in scenario 1 not in scenario 2 and vice versa
    s1_items = []
    for s1_item in scenario_1.resourcegroupitems:
        s1_items.append((s1_item.group_id, s1_item.ref_key, s1_item.node_id, s1_item.link_id, s1_item.subgroup_id))
    s2_items = []
    for s2_item in scenario_2.resourcegroupitems:
        s2_items.append((s2_item.group_id, s2_item.ref_key, s2_item.node_id, s2_item.link_id, s2_item.subgroup_id))

    groupdiff = dict()
    scenario_1_items = []
    scenario_2_items = []
    for s1_only_item in set(s1_items) - set(s2_items):
        
        item = ResourceGroupItem(
            group_id = s1_only_item[0],
            ref_key  = s1_only_item[1],
            node_id   = s1_only_item[2],
            link_id   = s1_only_item[3],
            subgroup_id   = s1_only_item[4],
        )
        scenario_1_items.append(item)
    for s2_only_item in set(s2_items) - set(s1_items):
        item = ResourceGroupItem(
            group_id = s2_only_item[0],
            ref_key  = s2_only_item[1],
            node_id   = s2_only_item[2],
            link_id   = s2_only_item[3],
            subgroup_id   = s2_only_item[4],
        )
        scenario_2_items.append(item)

    groupdiff['scenario_1_items'] = scenario_1_items
    groupdiff['scenario_2_items'] = scenario_2_items
    scenariodiff['groups'] = groupdiff

    return scenariodiff

def _check_network_owner(network, user_id):
    for owner in network.owners:
        if owner.user_id == int(user_id):
            return owner
    
    raise PermissionError('User %s is not the owner of network %s' % (user_id,network.network_id))
        


def lock_scenario(scenario_id, **kwargs):
    #user_id = kwargs.get('user_id')
    #check_perm(user_id, 'edit_network')
    scenario_i = _get_scenario(scenario_id)
    owner = _check_network_owner(scenario_i.network, kwargs['user_id'])
        
    if owner.edit == 'Y':
        scenario_i.locked = 'Y'
    else:
        raise PermissionError('User %s cannot lock scenario %s' % (kwargs['user_id'], scenario_id))
    DBSession.flush()
    return 'OK'

def unlock_scenario(scenario_id, **kwargs):
    #user_id = kwargs.get('user_id')
    #check_perm(user_id, 'edit_network')
    scenario_i = _get_scenario(scenario_id)

    owner = _check_network_owner(scenario_i.network, kwargs['user_id'])
    if owner.edit == 'Y':
        scenario_i.locked = 'N'
    else:
        raise PermissionError('User %s cannot unlock scenario %s' % (kwargs['user_id'], scenario_id))
    DBSession.flush()
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

    scen_i = _get_scenario(scenario_id)

    if resource_scenario.value is not None:
        res = _update_resourcescenario(scen_i, resource_scenario, user_id=user_id)
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


def _delete_resourcescenario(scenario_id, resource_scenario):

    ra_id = resource_scenario.resource_attr_id
    try:
        sd_i = DBSession.query(ResourceScenario).filter(ResourceScenario.scenario_id==scenario_id, ResourceScenario.resource_attr_id==ra_id).one()
    except NoResultFound:
        raise HydraError("ResourceAttr %s does not exist in scenario %s."%(ra_id, scenario_id))
    DBSession.delete(sd_i)

def _update_resourcescenario(scenario, resource_scenario, new=False, user_id=None):
    """
        Insert or Update the value of a resource's attribute by first getting the
        resource, then parsing the input data, then assigning the value.

        returns a ResourceScenario object.
    """

    if scenario is None:
        scenario = DBSession.query(Scenario).filter(Scenario.scenario_id==1).one()

    ra_id = resource_scenario.resource_attr_id
    logging.info("Assigning resource attribute: %s",ra_id)
    for rs in scenario.resourcescenarios:
        if rs.resource_attr_id == ra_id:
            r_scen_i = rs
            break
    else:
        r_scen_i = ResourceScenario()
        scenario.resourcescenarios.append(r_scen_i)

    data_type = resource_scenario.value.type.lower()

    value = resource_scenario.value.parse_value()

    if value is None:
        log.info("Cannot set data on resource attribute %s",ra_id)
        return None

    metadata  = {}
    if resource_scenario.value.metadata:
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

    data_hash = resource_scenario.value.get_hash()

    assign_value(r_scen_i, data_type, value, data_unit, name, dimension, 
                          metadata=metadata, data_hash=data_hash, user_id=user_id)
   
    return r_scen_i 

def assign_value(rs, data_type, val,
                 units, name, dimension, metadata={}, data_hash=None, user_id=None):
    """
        Insert or update a piece of data in a scenario. the 'new' flag
        indicates that the data is new, thus allowing us to avoid unnecessary
        queries for non-existant data. If this flag is not True, a check
        will be performed in the DB for its existance.
    """

    if rs.scenario.locked == 'Y':
        raise PermissionError("Cannot assign value. Scenario %s is locked"
                             %(rs.scenario_id))


    if rs.dataset is not None:
        
        #Has this dataset changed?
        if rs.dataset.data_hash == data_hash:
            return

    dataset = data.add_dataset(data_type,
                            val,
                            units,
                            dimension,
                            metadata=metadata,
                            name=name,
                            **dict(user_id=user_id))
    rs.dataset = dataset

def add_data_to_attribute(scenario_id, resource_attr_id, dataset,**kwargs):
    """
            Add data to a resource scenario outside of a network update
    """
    user_id = kwargs.get('user_id')

    _check_can_edit_scenario(scenario_id, user_id)

    try:
        r_scen_i = DBSession.query(ResourceScenario).filter(
                                ResourceScenario.scenario_id==scenario_id,
                                ResourceScenario.resource_attr_id==resource_attr_id).one()
    except NoResultFound:
        raise ResourceNotFoundError("Resource Attr %s not found in scenario %s")\
                                            %(scenario_id, resource_attr_id)

    data_type = dataset.type.lower()

    value = dataset.parse_value()

    dataset_metadata = {}
    if dataset.metadata is not None:
        for m in dataset.metadata:
            dataset_metadata[m.name] = m.value

    if value is None:
        raise HydraError("Cannot set value to attribute. "
            "No value was sent with dataset %s", dataset.id)

    assign_value(r_scen_i, data_type, value, dataset.unit, dataset.name, dataset.dimension,
                          metadata=dataset_metadata, data_hash=dataset.get_hash(), user_id=user_id)

    DBSession.flush()
    return r_scen_i

def get_scenario_data(scenario_id,**kwargs):
    """
        Get all the datasets from the group with the specified name
        @returns a list of dictionaries
    """
    user_id = kwargs.get('user_id')

    scenario_data = DBSession.query(Dataset).filter(Dataset.dataset_id==ResourceScenario.dataset_id, ResourceScenario.scenario_id==scenario_id).options(joinedload_all('timeseriesdata')).options(joinedload_all('metadata')).distinct().all()
    
    for sd in scenario_data:
       if sd.locked == 'Y':
           try:
                sd.check_read_permission(user_id)
           except:
               sd.value      = None
               sd.frequency  = None
               sd.start_time = None
               if sd.data_type == 'timeseries':
                   sd.timeseriesdata = []
               sd.metadata = []

    DBSession.expunge_all()

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
    
    if type_id is not None:
        attr_ids = []
        rs = DBSession.query(TypeAttr).filter(TypeAttr.type_id==type_id).all()
        for r in rs:
            attr_ids.append(rs.attr_id)

        resource_data = DBSession.query(Dataset).filter(
            ResourceScenario.dataset_id   == Dataset.dataset_id,
            ResourceAttr.resource_attr_id == ResourceScenario.resource_attr_id,
            ResourceScenario.scenario_id == scenario_id,
            ResourceAttr.ref_key == ref_key,
            or_(
                ResourceAttr.network_id==ref_id,
                ResourceAttr.node_id==ref_id,
                ResourceAttr.link_id==ref_id,
                ResourceAttr.group_id==ref_id
            ),
            ResourceAttr.attr_id in attr_ids).options(joinedload_all('timeseriesdata')).options(joinedload_all('metadata')).distinct().all()
    else:
        resource_data = DBSession.query(Dataset).filter(
        ResourceScenario.dataset_id   == Dataset.dataset_id,
        ResourceAttr.resource_attr_id == ResourceScenario.resource_attr_id,
        ResourceScenario.scenario_id == scenario_id,
        ResourceAttr.ref_key == ref_key,
        or_(
            ResourceAttr.network_id==ref_id,
            ResourceAttr.node_id==ref_id,
            ResourceAttr.link_id==ref_id,
            ResourceAttr.group_id==ref_id
        )).distinct().options(joinedload_all('timeseriesdata')).options(joinedload_all('metadata')).all()


    for datum in resource_data:
       if datum.locked == 'Y':
           try:
                datum.check_read_permission(user_id)
           except:
               datum.value      = None
               datum.frequency  = None
               datum.start_time = None
               if datum.data_type == 'timeseries':
                   datum.timeseriesdata = []


    return resource_data 

def _check_can_edit_scenario(scenario_id, user_id):
    scenario_i = _get_scenario(scenario_id)
    
    scenario_i.network.check_write_permission(user_id)

    if scenario_i.locked == 'Y':
        raise PermissionError('Cannot update scenario %s as it is locked.'%(scenario_id))



