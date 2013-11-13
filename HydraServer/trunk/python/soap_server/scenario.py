import logging
from HydraLib.HydraException import HydraError
from spyne.model.primitive import Integer, Boolean, String, AnyDict
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import Scenario,\
        Descriptor,\
        TimeSeries,\
        EqTimeSeries,\
        Scalar,\
        Array as HydraArray,\
        ResourceScenario,\
        Dataset,\
        DatasetGroup,\
        parse_value

from db import HydraIface
from HydraLib import hdb
from HydraLib import units

from hydra_base import HydraService

class ScenarioService(HydraService):
    """
        The scenario SOAP service
    """

    @rpc(Scenario, _returns=Scenario)
    def add_scenario(ctx, scenario):
        """
            Add a scenario to a specified network.
        """
        x = HydraIface.Scenario()
        x.db.network_id           = scenario.network_id
        x.db.scenario_name        = scenario.name
        x.db.scenario_description = scenario.description
        x.save()
        scenario.scenario_id = x.db.scenario_id

        for r_scen in scenario.resourcescenarios:
            scenario._update_resourcescenario(x.db.scenario_id, r_scen, new=True)

        hdb.commit()

        return x.get_as_complexmodel()

    @rpc(Integer, _returns=Boolean)
    def delete_scenario(ctx, scenario_id):
        """
            Set the status of a scenario to 'X'.
        """
        success = True
        try:
            x = HydraIface.Scenario(scenario_id = scenario_id)
            x.db.status = 'X'
            x.save()
        except HydraError, e:
            logging.critical(e)
            hdb.rollback()
            success=False

        return success

    @rpc(Integer, ResourceScenario, _returns=ResourceScenario)
    def update_resourcedata(ctx,scenario_id, resource_scenario):
        """
            Update the data associated with a scenario.
            Data missing from the resource scenario will not be removed
            from the scenario. Use the remove_resourcedata for this task.
        """
        if resource_scenario.value is not None:
            res = _update_resourcescenario(scenario_id, resource_scenario)
            return res.get_as_complexmodel()

    @rpc(Integer, ResourceScenario, _returns=ResourceScenario)
    def delete_resourcedata(ctx,scenario_id, resource_scenario):
        """
            Remove the data associated with a resource in a scenario.
        """
        _delete_resourcescenario(scenario_id, resource_scenario)

def _delete_resourcescenario(scenario_id, resource_scenario):

    ra_id = resource_scenario.resource_attr_id
    sd = HydraIface.ResourceScenario(scenario_id=scenario_id, resource_attr_id=ra_id)
    sd.delete()



def _update_resourcescenario(scenario_id, resource_scenario, new=False):
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

    rs_i = res.assign_value(scenario_id, ra_id, data_type, value,
                    "", "", "", new=new)

    return rs_i

def hash_incoming_data(data):

    unit = units.Units()

    hashes = []

    for d in data:
        val = parse_value(d)

        scenario_datum = HydraIface.ScenarioData()
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
            tScenarioData
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

    @rpc(SpyneArray(Dataset), _returns=SpyneArray(Dataset))
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
                tScenarioData
        """

        unit = units.Units()

        rs = HydraIface.execute(sql)
        dataset_id = rs[0].max_dataset_id
        if dataset_id is None:
            dataset_id = 0

        #A list of all the scenariodata objects
        scenario_data = []

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

            scenario_datum = HydraIface.ScenarioData()
            scenario_datum.db.data_type  = d.type
            scenario_datum.db.data_name  = d.name
            scenario_datum.db.data_units = d.unit

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

            scenario_data.append(scenario_datum)

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

        last_descriptor_id = HydraIface.bulk_insert(descriptors, 'tDescriptor')

        #assign the data_ids to the correct data objects.
        #We will need this later to ensure the correct dataset_id / data_id mappings
        if last_descriptor_id:
            next_id = last_descriptor_id - len(descriptors) + 1
            idx = 0
            while idx < len(descriptors):
                descriptors[idx].db.data_id = next_id
                next_id          = next_id + 1
                idx              = idx     + 1

        last_scalar_id     = HydraIface.bulk_insert(scalars, 'tScalar')

        if last_scalar_id:
            next_id = last_scalar_id - len(scalars) + 1
            idx = 0
            while idx < len(scalars):
                scalars[idx].db.data_id = next_id
                next_id                 = next_id + 1
                idx                     = idx     + 1

        last_array_id      = HydraIface.bulk_insert(arrays, 'tArray')

        if last_array_id:
            next_id = last_array_id - len(arrays) + 1
            idx = 0
            while idx < len(arrays):
                arrays[idx].db.data_id = next_id
                next_id                = next_id + 1
                idx                    = idx     + 1

        last_ts_id         = HydraIface.bulk_insert(timeseries, 'tTimeSeries')

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

            HydraIface.bulk_insert(timeseriesdata, 'tTimeSeriesData')

        last_eq_id         = HydraIface.bulk_insert(eqtimeseries, 'tEqTimeSeries')

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
            scenario_data[idx].db.data_id = descriptors[i].db.data_id
        for i, idx in enumerate(scalar_idx):
            scenario_data[idx].db.data_id = scalars[i].db.data_id
        for i, idx in enumerate(array_idx):
            scenario_data[idx].db.data_id = arrays[i].db.data_id
        for i, idx in enumerate(timeseries_idx):
            scenario_data[idx].db.data_id = timeseries[i].db.data_id
        for i, idx in enumerate(eqtimeseries_idx):
            scenario_data[idx].db.data_id = eqtimeseries[i].db.data_id

        #Isolate only the new datasets and insert them
        new_scenario_data = []
        for sd in scenario_data:
            if sd.db.dataset_id is None:
                new_scenario_data.append(sd)

        if len(new_scenario_data) > 0:
            last_dataset_id = HydraIface.bulk_insert(new_scenario_data, 'tScenarioData')

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

        rs_i = res.assign_value(scenario_id, resource_attr_id, data_type, value,
                        dataset.unit, dataset.name, dataset.dimension, new=False)

        rs_i.load_all()

        x = rs_i.get_as_complexmodel()
        logging.info(x)
        return x

    @rpc(String, _returns=DatasetGroup)
    def get_dataset_group(ctx, group_name):
        grp_i = HydraIface.DatasetGroup()
        grp_i.db.group_name = group_name

        grp_i.load()
        grp_i.commit()

        return grp_i.get_as_complexmodel()

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

        return grp_i.get_as_complexmodel()

    @rpc(String, _returns=SpyneArray(DatasetGroup))
    def get_groups_like_name(ctx, group_name):
        """
            Get all the datsets from the group with the specified name
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
            Get all the datsets from the group with the specified name
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
            sd = HydraIface.ScenarioData(dataset_id=r.dataset_id)
            d           = Dataset()
            d.id        = sd.db.dataset_id
            d.type      = sd.db.data_type
            d.dimension = sd.db.data_dimen
            d.unit      = sd.db.data_units
            d.name      = sd.db.data_name
            d.value     = sd.get_as_complexmodel()
            scenario_data.append(d)

        return scenario_data

    @rpc(Integer, _returns=SpyneArray(Dataset))
    def get_scenario_data(ctx, scenario_id):
        """
            Get all the datsets from the group with the specified name
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
            sd = HydraIface.ScenarioData(dataset_id=r.dataset_id)
            d           = Dataset()
            d.id        = sd.db.dataset_id
            d.type      = sd.db.data_type
            d.dimension = sd.db.data_dimen
            d.unit      = sd.db.data_units
            d.name      = sd.db.data_name
            d.value     = sd.get_as_complexmodel()
            scenario_data.append(d)

        return scenario_data

    @rpc(AnyDict, _returns=AnyDict)
    def update_dataset(ctx, data):
        """
            Update a piece of data directly, rather than through a resource scenario
        """

    @rpc(Integer, _returns=AnyDict)
    def get_dataset(dataset_id):
        """
            Get a piece of data directly, rather than through a resource scenario
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
