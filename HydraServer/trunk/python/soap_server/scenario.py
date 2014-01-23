import logging
from HydraLib.HydraException import HydraError
from spyne.model.primitive import Integer, Boolean, String, AnyDict
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from constraints import ConstraintService
from hydra_complexmodels import Scenario,\
        Descriptor,\
        TimeSeries,\
        EqTimeSeries,\
        Scalar,\
        Array as HydraArray,\
        ResourceScenario,\
        ResourceGroupItem,\
        Dataset,\
        DatasetGroup,\
        ScenarioDiff,\
        ResourceScenarioDiff,\
        ConstraintDiff,\
        ResourceGroupDiff,\
        parse_value,\
        get_as_complexmodel

from db import HydraIface
from HydraLib import units, IfaceLib
from HydraLib.util import timestamp_to_server_time

from hydra_base import HydraService

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

class ScenarioService(HydraService):
    """
        The scenario SOAP service
        as all resources already exist, there is no need to worry
        about negative IDS
    """

    @rpc(Integer, Scenario, _returns=Scenario)
    def add_scenario(ctx, network_id, scenario):
        """
            Add a scenario to a specified network.
        """
        logging.info("Adding scenarios to network")
        scen = HydraIface.Scenario()
        scen.db.scenario_name        = scenario.name
        scen.db.scenario_description = scenario.description
        scen.db.network_id           = network_id

        #Just in case someone puts in a negative ID for the scenario.
        if scenario.id < 0:
            scenario.id = None

        scen.save()

        #extract the data from each resourcescenario so it can all be
        #inserted in one go, rather than one at a time
        data = [r.value for r in scenario.resourcescenarios]

        dataset_ids = DataService.bulk_insert_data(ctx, data)

        #record all the resource attribute ids
        resource_attr_ids = [r.resource_attr_id for r in scenario.resourcescenarios]

        #get all the resource scenarios into a list and bulk insert them
        resource_scenarios = []
        for i, ra_id in enumerate(resource_attr_ids):
            rs_i = HydraIface.ResourceScenario()
            rs_i.db.resource_attr_id = ra_id
            rs_i.db.dataset_id       = dataset_ids[i]
            rs_i.db.scenario_id      = scen.db.scenario_id
            resource_scenarios.append(rs_i)

        IfaceLib.bulk_insert(resource_scenarios, 'tResourceScenario')

        #This is to get the resource scenarios into the scenario
        #object, so they are included into the scenario's complex model
        scen.load_all()

        for constraint in scenario.constraints:
            ConstraintService.add_constraint(ctx, scen.db.scenario_id, constraint)

        #Again doing bulk insert.
        group_items = []
        for group_item in scenario.resourcegroupitems:

            group_item_i = HydraIface.ResourceGroupItem()
            group_item_i.db.scenario_id = scen.db.scenario_id
            group_item_i.db.group_id    = group_item.group_id
            group_item_i.db.ref_key     = group_item.ref_key
            group_item_i.db.ref_id      = group_item.ref_id
            group_items.append(group_item_i)

        IfaceLib.bulk_insert(group_items, 'tResourceGroupItem')

        scen.save()
        scen.load_all()

        return get_as_complexmodel(ctx, scen)

    @rpc(Scenario, _returns=Scenario)
    def update_scenario(ctx, scenario):
        """
            Update a single scenario
            as all resources already exist, there is no need to worry
            about negative IDS
        """
        scen = HydraIface.Scenario(scenario_id=scenario.id)
        scen.db.scenario_name = scenario.name
        scen.db.scenario_description = scenario.description
        scen.save()

        for r_scen in scenario.resourcescenarios:
            _update_resourcescenario(scen.db.scenario_id, r_scen, user_id=ctx.in_header.user_id)

        for constraint in scenario.constraints:
            ConstraintService.add_constraint(ctx, scen.db.scenario_id, constraint)

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
        return get_as_complexmodel(ctx, scen)

    @rpc(Integer, _returns=Boolean)
    def delete_scenario(ctx, scenario_id):
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


    @rpc(Integer, _returns=Scenario)
    def clone_scenario(ctx, scenario_id):
        scen_i = HydraIface.Scenario(scenario_id = scenario_id)
        if scen_i.load_all() is False:
            raise HydraError("Scenario %s does not exist."%(scenario_id))

        cloned_scen = HydraIface.Scenario()
        cloned_scen.db.network_id           = scen_i.db.network_id
        cloned_scen.db.scenario_name        = "%s (clone)"%(scen_i.db.scenario_name)
        cloned_scen.db.scenario_description = scen_i.db.scenario_description
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

        return get_as_complexmodel(ctx, cloned_scen)

    @rpc(Integer, Integer, _returns=ScenarioDiff)
    def compare_scenarios(ctx, scenario_id_1, scenario_id_2):
        scenario_1 = HydraIface.Scenario(scenario_id=scenario_id_1)
        scenario_1.load_all()

        scenario_2 = HydraIface.Scenario(scenario_id=scenario_id_2)
        scenario_2.load_all()

        if scenario_1.db.network_id != scenario_2.db.network_id:
            raise HydraIface("Cannot compare scenarios that are not"
                             " in the same network!")

        scenariodiff = ScenarioDiff()
        resource_diffs = []

        #Make a list of all the resource scenarios (aka data) that are unique
        #to scenario 1 and that are in both scenarios, but are not the same.
        for s1_rs in scenario_1.resourcescenarios:
            for s2_rs in scenario_2.resourcescenarios:
                if s2_rs.db.resource_attr_id == s1_rs.db.resource_attr_id:
                    if s1_rs.db.dataset_id != s2_rs.db.dataset_id:
                        resource_diff = ResourceScenarioDiff()
                        resource_diff.resource_attr_id = s1_rs.db.resource_attr_id
                        resource_diff.scenario_1_dataset = get_as_complexmodel(ctx, s1_rs).value
                        resource_diff.scenario_2_dataset = get_as_complexmodel(ctx, s2_rs).value
                        resource_diffs.append(resource_diff)

                    break
            else:
                resource_diff = ResourceScenarioDiff()
                resource_diff.resource_attr_id = s1_rs.db.resource_attr_id
                resource_diff.scenario_1_dataset = get_as_complexmodel(ctx, s1_rs).value
                resource_diff.scenario_2_dataset = None
                resource_diffs.append(resource_diff)

        #make a list of all the resource scenarios (aka data) that are unique
        #in scenario 2.
        for s2_rs in scenario_2.resourcescenarios:
            for s1_rs in scenario_1.resourcescenarios:
                if s2_rs.db.resource_attr_id == s1_rs.db.resource_attr_id:
                    break
            else:
                resource_diff = ResourceScenarioDiff()
                resource_diff.resource_attr_id = s1_rs.db.resource_attr_id
                resource_diff.scenario_1_dataset = None
                resource_diff.scenario_2_dataset = get_as_complexmodel(ctx, s2_rs).value
                resource_diffs.append(resource_diff)

        scenariodiff.resourcescenarios = resource_diffs

        #Now compare groups.
        #Return list of group items in scenario 1 not in scenario 2 and vice versa
        s1_items = []
        for s1_item in scenario_1.resourcegroupitems:
            s1_items.append((s1_item.db.group_id, s1_item.db.ref_key, s1_item.db.ref_id))
        s2_items = []
        for s2_item in scenario_2.resourcegroupitems:
            s2_items.append((s2_item.db.group_id, s2_item.db.ref_key, s2_item.db.ref_id))

        groupdiff = ResourceGroupDiff()
        scenario_1_items = []
        scenario_2_items = []
        for s1_only_item in set(s1_items) - set(s2_items):
            item = ResourceGroupItem()
            item.group_id = s1_only_item[0]
            item.ref_key  = s1_only_item[1]
            item.ref_id   = s1_only_item[2]
            scenario_1_items.append(item)
        for s2_only_item in set(s2_items) - set(s1_items):
            item = ResourceGroupItem()
            item.group_id = s2_only_item[0]
            item.ref_key  = s2_only_item[1]
            item.ref_id   = s2_only_item[2]
            scenario_2_items.append(item)
        
        groupdiff.scenario_1_items = scenario_1_items
        groupdiff.scenario_2_items = scenario_2_items
        scenariodiff.groups = groupdiff

        #The next comparison is of constraints.
        constraint_diff = ConstraintDiff()
        common_constraints = []
        scenario_1_constraints = []
        scenario_2_constraints  = []
        #Make a list of all the constraints that are unique
        #to scenario 1 and that are in both scenarios, but are not the same.
        for s1_con in scenario_1.constraints:
            con1 = get_as_complexmodel(ctx, s1_con)
            for s2_con in scenario_2.constraints:
                con2 = get_as_complexmodel(ctx, s2_con)
                if con1.value == con2.value:
                    common_constraints.append(con1)
                    break
            else:
                scenario_1_constraints.append(con1)

        #make a list of all the constraints that are unique
        #in scenario 2.
        for s2_con in scenario_2.constraints:
            con2 = get_as_complexmodel(ctx, s2_con)
            for s1_con in scenario_1.constraints:
                con1 = get_as_complexmodel(ctx, s1_con)
                if con1.value == con2.value:
                    break
            else:
                scenario_2_constraints.append(con2)

        constraint_diff.common_constraints     = common_constraints
        constraint_diff.scenario_1_constraints = scenario_1_constraints
        constraint_diff.scenario_2_constraints = scenario_2_constraints

        scenariodiff.constraints = constraint_diff

        #for k, v in HydraIface.IfaceLib.MySqlIface.SQL_RESULT_DIFF_COUNT.items():
        #    run_count = HydraIface.IfaceLib.MySqlIface.SQL_CALL_COUNT[k]
        #    rs, count = v
        #    logging.debug("Run %s times, Changed %s times : \n %s"%(run_count, count, k))

        return scenariodiff


    @rpc(Integer, ResourceScenario, _returns=ResourceScenario)
    def update_resourcedata(ctx,scenario_id, resource_scenario):
        """
            Update the data associated with a scenario.
            Data missing from the resource scenario will not be removed
            from the scenario. Use the remove_resourcedata for this task.
        """
        if resource_scenario.value is not None:
            res = _update_resourcescenario(scenario_id, resource_scenario, user_id=ctx.in_header.user_id)
            if res is None:
                raise HydraError("Could not update resource data. No value "
                    "sent with data. Check privilages.")

            return get_as_complexmodel(ctx, res)

    @rpc(Integer, ResourceScenario, _returns=ResourceScenario)
    def delete_resourcedata(ctx,scenario_id, resource_scenario):
        """
            Remove the data associated with a resource in a scenario.
        """
        _delete_resourcescenario(scenario_id, resource_scenario)

    @rpc(Integer, _returns=Dataset)
    def get_dataset(ctx, dataset_id):
        """
            Get a single dataset, by ID
        """

        if dataset_id is None:
            return None

        sd_i = HydraIface.Dataset(dataset_id=dataset_id)
        sd_i.load()
        dataset = get_as_complexmodel(ctx, sd_i)

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
    
    value = parse_value(resource_scenario.value)

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

def hash_incoming_data(data):

    unit = units.Units()

    hashes = []

    for d in data:
        val = parse_value(d)

        if val is None:
            logging.info("Cannot parse data (dataset_id=%s). "
                         "Value not available.",d.id)
            continue

        scenario_datum = HydraIface.Dataset()
        scenario_datum.db.data_type  = d.type
        scenario_datum.db.data_name  = d.name
        scenario_datum.db.data_units = d.unit

        # Assign dimension if necessary
        if d.unit is not None and d.dimension is None:
            scenario_datum.db.data_dimen = unit.get_dimension(d.unit)
        else:
            scenario_datum.db.data_dimen = d.dimension

        data_hash = scenario_datum.set_hash(val)

        hashes.append(data_hash)

    return hashes

def get_existing_data(hashes):

    str_hashes = [str(h) for h in hashes]

    sql = """
        select
            dataset_id,
            data_id,
            data_hash
        from
            tDataset
        where
            data_hash in (%s)
    """ % (','.join(str_hashes))

    rs = HydraIface.execute(sql)

    hash_dict = {}
    for r in rs:
        hash_dict[r.data_hash] = (r.dataset_id, r.data_id)

    return hash_dict


class DataService(HydraService):

    """
        The data SOAP service
    """

    @rpc(SpyneArray(Dataset), _returns=SpyneArray(Integer))
    def bulk_insert_data(ctx, bulk_data):
        """
            Insert sereral pieces of data at once.
        """

        data_hashes     = hash_incoming_data(bulk_data)
        existing_hashes = get_existing_data(data_hashes)


        sql = """
            select
                max(dataset_id) as max_dataset_id
            from
                tDataset
        """

        user_id = ctx.in_header.user_id

        unit = units.Units()

        rs = HydraIface.execute(sql)
        dataset_id = rs[0].max_dataset_id
        if dataset_id is None:
            dataset_id = 0

        #A list of all the dataset objects
        datasets = []

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
        dataset_ids = []

        for i, d in enumerate(bulk_data):
            val = parse_value(d)

            if val is None:
                logging.info("Cannot parse data (dataset_id=%s). Value not available.",d.dataset_id)
                continue

            scenario_datum = HydraIface.Dataset()
            scenario_datum.db.data_type  = d.type
            scenario_datum.db.data_name  = d.name
            scenario_datum.db.data_units = d.unit
            scenario_datum.db.created_by = user_id

            # Assign dimension if necessary
            # It happens that d.dimension is and empty string. We set it to
            # None to achieve consistency in the DB.
            if d.unit is not None and d.dimension is None or \
                    d.unit is not None and len(d.dimension) == 0:
                scenario_datum.db.data_dimen = unit.get_dimension(d.unit)
            else:
                if d.dimension is None or len(d.dimension) == 0:
                    scenario_datum.db.data_dimen = None
                else:
                    scenario_datum.db.data_dimen = d.dimension

            current_hash = scenario_datum.set_hash(val)

            datasets.append(scenario_datum)

            #if this piece of data is already in the DB, then
            #there is no need to insert it!
            if current_hash in existing_hashes.keys():
                dataset_id = existing_hashes[current_hash][0]
                dataset_ids.append(dataset_id)
                scenario_datum.db.dataset_id = dataset_id
                continue
            else:
                #set a placeholder for a dataset_id we don't know yet.
                #The placeholder is the hash, which is unique to this object and
                #therefore easily identifiable.
                dataset_ids.append(current_hash)

            if d.type == 'descriptor':
                data = HydraIface.Descriptor()
                data.db.desc_val = val

                descriptors.append(data)
                descriptor_idx.append(i)

            elif d.type == 'scalar':
                data = HydraIface.Scalar()
                data.db.param_value = val

                scalars.append(data)
                scalar_idx.append(i)

            elif d.type == 'array':
                data = HydraIface.Array()
                data.db.arr_data = val

                arrays.append(data)
                array_idx.append(i)

            elif d.type == 'timeseries':
                data = HydraIface.TimeSeries()
                data.set_ts_values(val)
                timeseries.append(data)
                timeseries_idx.append(i)

            elif d.type == 'eqtimeseries':
                data = HydraIface.EqTimeSeries()
                data.db.start_time = val[0]
                data.db.frequency  = val[1]
                data.db.arr_data   = val[2]

                eqtimeseries.append(data)
                eqtimeseries_idx.append(i)

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

        last_scalar_id     = IfaceLib.bulk_insert(scalars, 'tScalar')

        if last_scalar_id:
            next_id = last_scalar_id - len(scalars) + 1
            idx = 0
            while idx < len(scalars):
                scalars[idx].db.data_id = next_id
                next_id                 = next_id + 1
                idx                     = idx     + 1

        last_array_id      = IfaceLib.bulk_insert(arrays, 'tArray')

        if last_array_id:
            next_id = last_array_id - len(arrays) + 1
            idx = 0
            while idx < len(arrays):
                arrays[idx].db.data_id = next_id
                next_id                = next_id + 1
                idx                    = idx     + 1

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

        last_eq_id         = IfaceLib.bulk_insert(eqtimeseries, 'tEqTimeSeries')

        if last_eq_id:
            next_id = last_eq_id - len(eqtimeseries) + 1
            idx = 0
            while idx < len(eqtimeseries):
                eqtimeseries[idx].db.data_id = next_id
                next_id        = next_id + 1
                idx            = idx  + 1

        #Now fill in the final piece of data before inserting the new
        #scenario data rows -- the data ids generated from the data inserts.
        for i, idx in enumerate(descriptor_idx):
            datasets[idx].db.data_id = descriptors[i].db.data_id
        for i, idx in enumerate(scalar_idx):
            datasets[idx].db.data_id = scalars[i].db.data_id
        for i, idx in enumerate(array_idx):
            datasets[idx].db.data_id = arrays[i].db.data_id
        for i, idx in enumerate(timeseries_idx):
            datasets[idx].db.data_id = timeseries[i].db.data_id
        for i, idx in enumerate(eqtimeseries_idx):
            datasets[idx].db.data_id = eqtimeseries[i].db.data_id

        #Isolate only the new datasets and insert them
        new_scenario_data = []
        for sd in datasets:
            if sd.db.dataset_id is None:
                new_scenario_data.append(sd)

        if len(new_scenario_data) > 0:
            last_dataset_id = IfaceLib.bulk_insert(new_scenario_data, 'tDataset')

            #set the dataset ids on the new scenario data objects
            next_id = last_dataset_id - len(new_scenario_data) + 1
            idx = 0

            while idx < len(new_scenario_data):
                dataset_id     = next_id
                new_scenario_data[idx].db.dataset_id = dataset_id
                next_id        = next_id + 1
                idx            = idx     + 1

            #using the has of the new scenario data, find the placeholder in dataset_ids
            #and replace it with the dataset_id.
            for sd in new_scenario_data:
                dataset_idx = dataset_ids.index(sd.db.data_hash)
                dataset_ids[dataset_idx] = sd.db.dataset_id

        return dataset_ids

    @rpc(Integer, Integer, Dataset, _returns=ResourceScenario)
    def add_data_to_attribute(ctx, scenario_id, resource_attr_id, dataset):
        """
                Add data to a resource scenario outside of a network update
        """
        r_a = HydraIface.ResourceAttr(resource_attr_id=resource_attr_id)

        res = r_a.get_resource()

        data_type = dataset.type.lower()

        value = parse_value(dataset)

        if value is None:
            raise HydraException("Cannot set value to attribute. "
                "No value was sent with dataset %s", dataset.id)

        user_id = ctx.in_header.user_id

        rs_i = res.assign_value(scenario_id, resource_attr_id, data_type, value,
                        dataset.unit, dataset.name, dataset.dimension, new=False, user_id=user_id)

        rs_i.load_all()

        x = get_as_complexmodel(ctx,rs_i)
        logging.info(x)
        return x

    @rpc(String, _returns=DatasetGroup)
    def get_dataset_group(ctx, group_name):
        grp_i = HydraIface.DatasetGroup()
        grp_i.db.group_name = group_name

        grp_i.load()
        grp_i.commit()

        return get_as_complexmodel(ctx,grp_i)

    @rpc(DatasetGroup, _returns=DatasetGroup)
    def add_dataset_group(ctx, group):

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

        return_grp = get_as_complexmodel(ctx,grp_i)
        return return_grp

    @rpc(String, _returns=SpyneArray(DatasetGroup))
    def get_groups_like_name(ctx, group_name):
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
            g = DatasetGroup()
            g.group_id   = r.group_id
            g.group_name = r.group_name
            groups.append(g)

        return groups

    @rpc(Integer, _returns=SpyneArray(Dataset))
    def get_group_datasets(ctx, group_id):
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
            scenario_data.append(get_as_complexmodel(ctx, sd))

        return scenario_data

    @rpc(Integer, _returns=SpyneArray(Dataset))
    def get_scenario_data(ctx, scenario_id):
        """
            Get all the datasets from the group with the specified name
        """
        scenario_data = []

        sql = """
            select
                dataset_id
            from
                tResourceScenario
            where
                scenario_id = %s
        """ % scenario_id


        rs = HydraIface.execute(sql)

        for r in rs:
            sd          = HydraIface.Dataset(dataset_id=r.dataset_id)
            scenario_data.append(get_as_complexmodel(ctx, sd))

        return scenario_data

    @rpc(Dataset, _returns=Dataset)
    def update_dataset(ctx, data):
        """
            Update a piece of data directly, rather than through a resource
            scenario.
        """


    @rpc(Integer, _returns=Boolean)
    def delete_dataset(dataset_id):
        """
            Removes a piece of data from the DB.
            CAUTION! Use with care, as this cannot be undone easily.
        """

    @rpc(Descriptor, _returns=Descriptor)
    def echo_descriptor(ctx, x):
        return x

    @rpc(TimeSeries, _returns=TimeSeries)
    def echo_timeseries(ctx, x):
        return x

    @rpc(EqTimeSeries, _returns=EqTimeSeries)
    def echo_eqtimeseries(ctx, x):
        return x

    @rpc(Scalar, _returns=Scalar)
    def echo_scalar(ctx, x):
        return x

    @rpc(HydraArray, _returns=HydraArray)
    def echo_array(ctx, x):
        return x

    @rpc(Integer, SpyneArray(String), _returns=AnyDict)
    #@rpc(Integer, SpyneArray(String), _returns=AnyDict)
    def get_val_at_time(ctx, dataset_id, timestamps):
        """
        Given a timestamp (or list of timestamps) and some timeseries data,
        return the values appropriate to the requested times.

        If the timestamp is before the start of the timeseries data, return
        None If the timestamp is after the end of the timeseries data, return
        the last value.  """
        t = []
        for time in timestamps:
            t.append(timestamp_to_server_time(time))
        td = HydraIface.Dataset(dataset_id=dataset_id)
        #for time in t:
        #    data.append(td.get_val(timestamp=time))
        data = td.get_val(timestamp=t)
        dataset = {'data': data}

        return dataset
