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
from HydraLib.HydraException import HydraError
from db import HydraIface
from HydraLib import units
from db import IfaceLib
from db.hdb import make_param
from HydraLib.util import timestamp_to_ordinal, get_datetime
import data
import datetime

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

def bulk_insert_data(bulk_data, user_id=None):
    get_timing = lambda x: datetime.datetime.now() - x

    start_time=datetime.datetime.now()
    logging.info("Starting data insert (%s datasets) %s", len(bulk_data), get_timing(start_time))
    datasets, iface_data  = process_incoming_data(bulk_data, user_id)
    existing_data = get_existing_data(iface_data.keys())

    sql = """
        select
            max(dataset_id) as max_dataset_id
        from
            tDataset
    """

    rs = HydraIface.execute(sql)
    dataset_id = rs[0].max_dataset_id
    if dataset_id is None:
        dataset_id = 0

    #A list of all the dataset objects
    all_datset_objects = []

    #Lists of all the data objects
    descriptors  = []
    timeseries   = []
    timeseriesdata = []
    eqtimeseries = []
    scalars      = []
    arrays       = []

    #Lists which keep track of what index in the overall
    #data list that a type is in. For example, if the data looks like:
    #[descriptor, ts, ts, descriptor], that translates to [0, 3] in descriptor_idx
    #and [1, 2] in timeseries_idx
    descriptor_idx   = []
    timeseries_idx   = []
    eqtimeseries_idx = []
    scalar_idx       = []
    array_idx        = []

    #This is what gets returned.
    dataset_hashes = []
    idx = 0
    logging.debug("Processing data %s", get_timing(start_time)) 
    for d in bulk_data:
        dataset_i = iface_data[d.data_hash]
        val = dataset_i.val
        current_hash = d.data_hash

        #if this piece of data is already in the DB, then
        #there is no need to insert it!
        if  existing_data.get(current_hash):
            dataset_hashes.append(existing_data[current_hash])

            all_datset_objects.append(existing_data[current_hash])
            idx = idx + 1
            continue
        elif current_hash in dataset_hashes:
            dataset_hashes.append(current_hash)
            continue
        else:
            #set a placeholder for a dataset_id we don't know yet.
            #The placeholder is the hash, which is unique to this object and
            #therefore easily identifiable.
            dataset_hashes.append(current_hash)
            all_datset_objects.append(dataset_i)

        data_type = dataset_i.db.data_type
        if data_type == 'descriptor':
            datum = HydraIface.Descriptor()
            datum.db.desc_val = val

            descriptors.append(datum)
            descriptor_idx.append(idx)

        elif data_type == 'scalar':
            datum = HydraIface.Scalar()
            datum.db.param_value = val

            scalars.append(datum)
            scalar_idx.append(idx)

        elif data_type == 'array':
            datum = HydraIface.Array()
            datum.db.arr_data = val

            arrays.append(datum)
            array_idx.append(idx)

        elif data_type == 'timeseries':

            datum = HydraIface.TimeSeries()
            datum.set_ts_values(val)
            timeseries.append(datum)
            timeseries_idx.append(idx)

        elif data_type == 'eqtimeseries':
            datum = HydraIface.EqTimeSeries()
            datum.db.start_time = val[0]
            datum.db.frequency  = val[1]
            datum.db.arr_data   = val[2]

            eqtimeseries.append(datum)
            eqtimeseries_idx.append(idx)

        dataset_i.datum = datum
        idx = idx + 1

    logging.debug("Finished Processessing data.")
    logging.debug("Inserting descriptors %s", get_timing(start_time))
    last_descriptor_id = IfaceLib.bulk_insert(descriptors, 'tDescriptor')
    #assign the data_ids to the correct data objects.
    #We will need this later to ensure the correct dataset_id / data_id mappings
    if last_descriptor_id:
        next_id = last_descriptor_id - len(descriptors) + 1
        idx = 0
        while idx < len(descriptors):
            descriptors[idx].db.data_id = next_id
            next_id          = next_id + 1
            idx              = idx     + 1

    logging.debug("Inserting scalars %s", get_timing(start_time))
    last_scalar_id     = IfaceLib.bulk_insert(scalars, 'tScalar')

    if last_scalar_id:
        next_id = last_scalar_id - len(scalars) + 1
        idx = 0
        while idx < len(scalars):
            scalars[idx].db.data_id = next_id
            next_id                 = next_id + 1
            idx                     = idx     + 1

    logging.debug("Inserting arrays %s", get_timing(start_time))
    last_array_id      = IfaceLib.bulk_insert(arrays, 'tArray')

    if last_array_id:
        next_id = last_array_id - len(arrays) + 1
        idx = 0
        while idx < len(arrays):
            arrays[idx].db.data_id = next_id
            next_id                = next_id + 1
            idx                    = idx     + 1

    logging.debug("Inserting timeseries")
    last_ts_id         = IfaceLib.bulk_insert(timeseries, 'tTimeSeries')

    if last_ts_id:
        next_id = last_ts_id - len(timeseries) + 1
        idx = 0
        while idx < len(timeseries):
            timeseries[idx].db.data_id      = next_id

            for d in timeseries[idx].timeseriesdatas:
                d.db.data_id = next_id

            next_id       = next_id + 1
            idx              = idx  + 1

        #Now that the data_ids have been generated, we need to add the actual
        #timeseries data, which is stored in a separate table.
        timeseriesdata = []
        for idx, ts in enumerate(timeseries):
            timeseriesdata.extend(ts.timeseriesdatas)

        IfaceLib.bulk_insert(timeseriesdata, 'tTimeSeriesData')

    logging.debug("Inserting eqtimeseries")
    last_eq_id         = IfaceLib.bulk_insert(eqtimeseries, 'tEqTimeSeries')

    if last_eq_id:
        next_id = last_eq_id - len(eqtimeseries) + 1
        idx = 0
        while idx < len(eqtimeseries):
            eqtimeseries[idx].db.data_id = next_id
            next_id        = next_id + 1
            idx            = idx  + 1

    logging.debug("Updating data IDs")
    #Now fill in the final piece of data before inserting the new
    #scenario data rows -- the data ids generated from the data inserts.
    for i, idx in enumerate(descriptor_idx):
        all_datset_objects[idx].db.data_id = descriptors[i].db.data_id
        all_datset_objects[idx].datum = descriptors[i]
    for i, idx in enumerate(scalar_idx):
        all_datset_objects[idx].db.data_id = scalars[i].db.data_id
        all_datset_objects[idx].datum = scalars[i]
    for i, idx in enumerate(array_idx):
        all_datset_objects[idx].db.data_id = arrays[i].db.data_id
        all_datset_objects[idx].datum = arrays[i]
    for i, idx in enumerate(timeseries_idx):
        all_datset_objects[idx].db.data_id = timeseries[i].db.data_id
        all_datset_objects[idx].datum = timeseries[i]
    for i, idx in enumerate(eqtimeseries_idx):
        all_datset_objects[idx].db.data_id = eqtimeseries[i].db.data_id
        all_datset_objects[idx].datum = eqtimeseries[i]

    logging.debug("Isolating new data")
    #Isolate only the new datasets and insert them
    new_scenario_data = []
    for sd in all_datset_objects:
        if sd.db.dataset_id is None:
            new_scenario_data.append(sd)

    if len(new_scenario_data) > 0:
        logging.debug("Inserting new datasets")
        last_dataset_id = IfaceLib.bulk_insert(new_scenario_data, 'tDataset')

        #set the dataset ids on the new scenario data objects
        next_id = last_dataset_id - len(new_scenario_data) + 1
        idx = 0

        logging.debug("Updating datset IDS")
        while idx < len(new_scenario_data):
            dataset_id     = next_id
            new_scenario_data[idx].db.dataset_id = dataset_id
            next_id        = next_id + 1
            idx            = idx     + 1

        #using the hash of the new scenario data, find the placeholders in dataset_ids
        #and replace it with the dataset_id.
        logging.debug("Putting new data with existing data to complete function")
        sd_dict = dict([(sd.db.data_hash, sd) for sd in new_scenario_data])
        for idx, d in enumerate(dataset_hashes):
            if type(d) == int:
                dataset_hashes[idx] = sd_dict[d]
    logging.info("Done bulk inserting data. %s datasets", len(dataset_hashes))
    return dataset_hashes 

def add_scenario(user_id, network_id, scenario):
    """
        Add a scenario to a specified network.
    """
    logging.info("Adding scenarios to network")
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

def update_scenario(user_id, scenario):
    """
        Update a single scenario
        as all resources already exist, there is no need to worry
        about negative IDS
    """
    scen = HydraIface.Scenario(scenario_id=scenario.id)
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

def delete_scenario(user_id, scenario_id):
    """
        Set the status of a scenario to 'X'.
    """

    success = True
    scen_i = HydraIface.Scenario(scenario_id = scenario_id)

    if scen_i.load() is False:
        raise HydraError("Scenario %s does not exist."%(scenario_id))

    scen_i.db.status = 'X'
    scen_i.save()

    return success


def clone_scenario(user_id, scenario_id):
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

def compare_scenarios(user_id, scenario_id_1, scenario_id_2):
    scenario_1 = HydraIface.Scenario(scenario_id=scenario_id_1)
    scenario_1.load_all()

    scenario_2 = HydraIface.Scenario(scenario_id=scenario_id_2)
    scenario_2.load_all()

    if scenario_1.db.network_id != scenario_2.db.network_id:
        raise HydraIface("Cannot compare scenarios that are not"
                         " in the same network!")

    scenariodiff = dict() 
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

    for ra_id, s1_rs in r_scen_1_dict.items():
        s2_rs = r_scen_2_dict.get(ra_id)
        if s2_rs is not None:
            if s1_rs.db.dataset_id != s2_rs.db.dataset_id:
                resource_diff = dict( 
                    resource_attr_id = s1_rs.db.resource_attr_id,
                    scenario_1_dataset = s1_rs.get_dataset().get_as_dict(),
                    scenario_2_dataset = s2_rs.get_dataset().get_as_dict(),
                )
                resource_diffs.append(resource_diff)

            break
        else:
            resource_diff = dict( 
                resource_attr_id = s1_rs.db.resource_attr_id,
                scenario_1_dataset = s1_rs.get_dataset().get_as_dict(),
                scenario_2_dataset = None,
            )
            resource_diffs.append(resource_diff)

    #make a list of all the resource scenarios (aka data) that are unique
    #in scenario 2.
    for ra_id, s2_rs in r_scen_2_dict.items():
        s1_rs = r_scen_1_dict.get(ra_id)
        if s1_rs is None:
            resource_diff = dict( 
                resource_attr_id = s1_rs.db.resource_attr_id,
                scenario_1_dataset = None,
                scenario_2_dataset = s2_rs.get_dataset().get_as_dict(),
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


def update_resourcedata(user_id,scenario_id, resource_scenario):
    """
        Update the data associated with a scenario.
        Data missing from the resource scenario will not be removed
        from the scenario. Use the remove_resourcedata for this task.
    """
    res = None
    if resource_scenario.value is not None:
        res = _update_resourcescenario(scenario_id, resource_scenario, user_id=user_id)
        if res is None:
            raise HydraError("Could not update resource data. No value "
                "sent with data. Check privilages.")

    return res

def delete_resourcedata(user_id,scenario_id, resource_scenario):
    """
        Remove the data associated with a resource in a scenario.
    """
    _delete_resourcescenario(scenario_id, resource_scenario)

def get_dataset(user_id, dataset_id):
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

    r_a = HydraIface.ResourceAttr(resource_attr_id=ra_id)

    res = r_a.get_resource()

    data_type = resource_scenario.value.type.lower()

    value = data.parse_value(resource_scenario.value)

    if value is None:
        logging.info("Cannot set data on resource attribute %s",ra_id)
        return None

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

    name      = resource_scenario.value.name

    rs_i = res.assign_value(scenario_id, ra_id, data_type, value,
                    data_unit, name, dimension, new=new, user_id=user_id)

    return rs_i

def process_incoming_data(data, user_id=None):

    unit = units.Units()

    scenario_data = {}

    for d in data:
        val = data.parse_value(d)

        if val is None:
            logging.info("Cannot parse data (dataset_id=%s). "
                         "Value not available.",d)
            continue

        scenario_datum = HydraIface.Dataset()
        scenario_datum.db.data_type  = d.type
        scenario_datum.db.data_name  = d.name
        scenario_datum.db.data_units = d.unit
        scenario_datum.db.dataset_id = d.id
        scenario_datum.db.created_by = user_id
        # Assign dimension if necessary
        if d.unit is not None and d.dimension is None:
            scenario_datum.db.data_dimen = unit.get_dimension(d.unit)
        else:
            scenario_datum.db.data_dimen = d.dimension

        data_hash = scenario_datum.set_hash(val)
        scenario_datum.db.data_hash = data_hash
        scenario_datum.val = val
        scenario_data[data_hash] = scenario_datum
        d.data_hash = data_hash

    return data, scenario_data

def get_existing_data(hashes):

    str_hashes = [str(h) for h in hashes]

    hash_dict = {}

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
            d.data_hash  in (%s)
    """ % (','.join(str_hashes)) 


    rs = HydraIface.execute(sql)

    for dr in rs:
        dataset = HydraIface.Dataset()
        dataset.db.dataset_id = dr.dataset_id
        dataset.db.data_id = dr.data_id
        dataset.db.data_type = dr.data_type
        dataset.db.data_units = dr.data_units
        dataset.db.data_dimen = dr.data_dimen
        dataset.db.data_name  = dr.data_name
        dataset.db.data_hash  = dr.data_hash
        dataset.db.locked     = dr.locked

        if dr.data_type in ('scalar', 'array', 'descriptor'):
            if dr.data_type == 'scalar':
                datum = HydraIface.Scalar()
                datum.db.data_id = dr.data_id
                datum.db.param_value = dr.value
                dataset.datum = datum
            elif dr.data_type == 'descriptor':
                datum = HydraIface.Descriptor()
                datum.db.data_id = dr.data_id
                datum.db.desc_val = dr.value
                dataset.datum = datum
            elif dr.data_type == 'array':
                datum = HydraIface.Array()
                datum.db.data_id = dr.data_id
                datum.db.arr_data = dr.value
                dataset.datum = datum
            else:
                dataset.get_val()
        hash_dict[dr.data_hash] = dataset
    logging.info("Retrieved %s datasets", len(hash_dict))

    return hash_dict


def add_data_to_attribute(user_id, scenario_id, resource_attr_id, dataset):
    """
            Add data to a resource scenario outside of a network update
    """
    r_a = HydraIface.ResourceAttr(resource_attr_id=resource_attr_id)

    res = r_a.get_resource()

    data_type = dataset.type.lower()

    value = data.parse_value(dataset)

    if value is None:
        raise HydraError("Cannot set value to attribute. "
            "No value was sent with dataset %s", dataset.id)

    user_id = user_id

    rs_i = res.assign_value(scenario_id, resource_attr_id, data_type, value,
                    dataset.unit, dataset.name, dataset.dimension, new=False, user_id=user_id)

    rs_i.load_all()

    return rs_i

def get_dataset_group(user_id, group_name):
    grp_i = HydraIface.DatasetGroup()
    grp_i.db.group_name = group_name

    grp_i.load()
    grp_i.commit()

    return grp_i

def add_dataset_group(user_id, group):

    grp_i = HydraIface.DatasetGroup()
    grp_i.db.group_name = group.group_name

    grp_i.save()
    grp_i.commit()
    grp_i.load()

    for item in group.datasetgroupitems:
        datasetitem = HydraIface.DatasetGroupItem()
        datasetitem.db.group_id = grp_i.db.group_id
        datasetitem.db.dataset_id = item.dataset_id
        datasetitem.save()
        datasetitem.commit()

    grp_i.load_all()

    return grp_i

def get_groups_like_name(user_id, group_name):
    """
        Get all the datasets from the group with the specified name
    """
    groups = []

    sql = """
        select
            group_id,
            group_name
        from
            tDatasetGroup
        where
            lower(group_name) like '%%%s%%'
    """ % group_name.lower()


    rs = HydraIface.execute(sql)

    for r in rs:
        g = HydraIface.DatasetGroup()
        g.db.group_id   = r.group_id
        g.db.group_name = r.group_name
        groups.append(g)

    return groups

def get_group_datasets(user_id, group_id):
    """
        Get all the datasets from the group with the specified name
    """
    scenario_data = []

    sql = """
        select
            item.dataset_id
        from
            tDatasetGroup as grp,
            tDatasetGroupItem as item
        where
            item.group_id = grp.group_id
        and grp.group_id = %s
    """ % group_id


    rs = HydraIface.execute(sql)

    for r in rs:
        sd = HydraIface.Dataset(dataset_id=r.dataset_id)
        scenario_data.append(sd)

    return scenario_data

def get_scenario_data(user_id, scenario_id):
    """
        Get all the datasets from the group with the specified name
        @returns a list of dictionaries
    """
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


    rs = HydraIface.execute(sql)

    for dr in rs:
        if dr.data_type in ('scalar', 'array', 'descriptor'):
            if dr.data_type == 'scalar':
                val = {
                    'object_type' : 'Scalar',
                    'param_value' : dr.value
                }
            elif dr.data_type == 'descriptor':
                val = {
                    'object_type': 'Descriptor',
                    'desc_val'   : dr.value
                }
            elif dr.data_type == 'array':
                val = {
                    'object_type': 'Array',
                    'arr_data'      : dr.value
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

        scenario_data.append(d)
    logging.info("Retrieved %s datasets", len(scenario_data))
    return scenario_data

def get_node_data(user_id, node_id, scenario_id, type_id):
    """
        Get all the resource scenarios for a given node 
        in a given scenario. If type_id is specified, only
        return the resource scenarios for the attributes
        within the type.
    """

    attr_string = ""
    
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
            ra.ref_key = 'NODE'
            and ra.ref_id  = %(node_id)s
            and rs.resource_attr_id = ra.resource_attr_id
            and rs.scenario_id = %(scenario_id)s
            %(attr_filter)s
        """ % {'scenario_id' : scenario_id,
               'node_id'     : node_id,
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
                    ra.ref_key = 'NODE'
                    and ra.ref_id = %(node_id)s 
                    and rs.scenario_id = %(scenario_id)s
                    and rs.resource_attr_id = ra.resource_attr_id
                    %(attr_filter)s
                )
    """ % {
            'scenario_id' : scenario_id,
            'node_id'     : node_id,
            'attr_filter' : attr_string,
        }


    rs = HydraIface.execute(sql)
    resource_scenarios = [] 
    for dr in rs:
        if dr.data_type in ('scalar', 'array', 'descriptor'):
            if dr.data_type == 'scalar':
                val = {
                    'object_type' : 'Scalar',
                    'param_value' : dr.value
                }
            elif dr.data_type == 'descriptor':
                val = {
                    'object_type': 'Descriptor',
                    'desc_val'   : dr.value
                }
            elif dr.data_type == 'array':
                val = {
                    'object_type': 'Array',
                    'arr_data'      : dr.value
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
        for rs in all_dataset_ids[dr.dataset_id]:
            rs['value'] = d 
            resource_scenarios.append(rs)

    logging.info("Retrieved %s resource scenarios for node %s", len(resource_scenarios), node_id)
    return resource_scenarios 

def get_val_at_time(user_id, dataset_id, timestamps):
    """
    Given a timestamp (or list of timestamps) and some timeseries data,
    return the values appropriate to the requested times.

    If the timestamp is before the start of the timeseries data, return
    None If the timestamp is after the end of the timeseries data, return
    the last value.  """
    t = []
    for time in timestamps:
        t.append(timestamp_to_ordinal(time))
    td = HydraIface.Dataset(dataset_id=dataset_id)
    #for time in t:
    #    data.append(td.get_val(timestamp=time))
    data = td.get_val(timestamp=t)
    dataset = {'data': data}

    return dataset

def get_vals_between_times(user_id, dataset_id, start_time, end_time, timestep):
    server_start_time = get_datetime(start_time)
    server_end_time   = get_datetime(end_time)

    times = [timestamp_to_ordinal(server_start_time)]

    next_time = server_start_time
    while next_time < server_end_time:
        next_time = next_time  + datetime.timedelta(**{timestep:1})
        times.append(timestamp_to_ordinal(next_time))

    td = HydraIface.Dataset(dataset_id=dataset_id)
    logging.debug("Number of times to fetch: %s", len(times))
    data = td.get_val(timestamp=times)

    dataset = {'data' : [data]}

    return dataset
