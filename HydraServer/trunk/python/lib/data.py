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
import datetime
import sys
from HydraLib.util import timestamp_to_ordinal, get_datetime
from HydraLib import units
import logging
from db import HydraIface, IfaceLib
from numpy import array
global FORMAT
FORMAT = "%Y-%m-%d %H:%M:%S.%f"
#"2013-08-13T15:55:43.468886Z"

current_module = sys.modules[__name__]
NS = "soap_server.hydra_complexmodels"

log = logging.getLogger(__name__)

def parse_value(data):
    """
        Turn a complex model object into a hydraiface - friendly value.
    """

    data_type = data.type

    if data.value is None:
        log.warn("Cannot parse dataset. No value specified.")
        return None

    #attr_data.value is a dictionary,
    #but the keys have namespaces which must be stripped.
    val_names = data.value.keys()
    value = []
    for name in val_names:
        value.append(data.value[name])

    if data_type == 'descriptor':
        return value[0][0]
    elif data_type == 'timeseries':
        # The brand new way to parse time series data:
        ts = []
        for ts_val in value[0]:
            #The value is a list, so must get index 0
            timestamp = ts_val['{%s}ts_time'%NS][0]
            # Check if we have received a seasonal time series first
            ordinal_ts_time = timestamp_to_ordinal(timestamp)
            arr_data = ts_val['{%s}ts_value'%NS][0]
            try:
                arr_data = dict(arr_data)
                try:
                    ts_value = eval(arr_data)
                except:
                    ts_value = parse_array(arr_data)
            except:
                try:
                    ts_value = float(arr_data)
                except:
                    ts_value = arr_data

            ts.append((ordinal_ts_time, ts_value))

        return ts
    elif data_type == 'eqtimeseries':
        start_time = data.value['{%s}start_time'%NS][0]
        start_time = timestamp_to_ordinal(start_time) 
        frequency  = data.value['{%s}frequency'%NS][0]
        arr_data   = data.value['{%s}arr_data'%NS][0]
        try:
            val = eval(arr_data)
        except:
            val = parse_array(arr_data)

        arr_data   = val

        return (start_time, frequency, arr_data)
    elif data_type == 'scalar':
        return value[0][0]
    elif data_type == 'array':
        arr_data   = data.value['{%s}arr_data'%NS][0]

        try:
            val = eval(arr_data)
        except:
            val = parse_array(arr_data)
        return val


def parse_array(arr):
    """
        Take a list of nested dictionaries and return a python list containing
        a single value, a string or sub lists.
    """
    ret_arr = []
    sub_arr = arr.get('{%s}array'%NS, None)
    for val in sub_arr:
        sub_arr = val.get('{%s}array'%NS, None)
        if sub_arr is not None:
            ret_arr.append(parse_array(val))
            continue

        actual_vals = val.get('{%s}item'%NS)
        for v in actual_vals:
            try:
                ret_arr.append(float(v))
            except:
                ret_arr.append(v)
    return ret_arr

def create_dict(arr_data):
    arr_data = array(arr_data)

    arr = {'array': []}
    if arr_data.ndim == 0:
        return {'array': []}
    elif arr_data.ndim == 1:
        return {'array': [{'item': list(arr_data)}]}
    else:
        for a in arr_data:
            val = create_dict(a)
            arr['array'].append(val)

        return arr


def bulk_insert_data(bulk_data, user_id=None):
    get_timing = lambda x: datetime.datetime.now() - x

    start_time=datetime.datetime.now()
    log.info("Starting data insert (%s datasets) %s", len(bulk_data), get_timing(start_time))
    datasets, iface_data  = _process_incoming_data(bulk_data, user_id)
    existing_data = _get_existing_data(iface_data.keys())

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

    metadata         = {}

    #This is what gets returned.
    dataset_hashes = []
    idx = 0
    log.debug("Processing data %s", get_timing(start_time)) 
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
            metadata[current_hash] = dataset_i.metadatas

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

    log.debug("Finished Processessing data.")
    log.debug("Inserting descriptors %s", get_timing(start_time))
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

    log.debug("Inserting scalars %s", get_timing(start_time))
    last_scalar_id     = IfaceLib.bulk_insert(scalars, 'tScalar')

    if last_scalar_id:
        next_id = last_scalar_id - len(scalars) + 1
        idx = 0
        while idx < len(scalars):
            scalars[idx].db.data_id = next_id
            next_id                 = next_id + 1
            idx                     = idx     + 1

    log.debug("Inserting arrays %s", get_timing(start_time))
    last_array_id      = IfaceLib.bulk_insert(arrays, 'tArray')

    if last_array_id:
        next_id = last_array_id - len(arrays) + 1
        idx = 0
        while idx < len(arrays):
            arrays[idx].db.data_id = next_id
            next_id                = next_id + 1
            idx                    = idx     + 1

    log.debug("Inserting timeseries")
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

    log.debug("Inserting eqtimeseries")
    last_eq_id         = IfaceLib.bulk_insert(eqtimeseries, 'tEqTimeSeries')

    if last_eq_id:
        next_id = last_eq_id - len(eqtimeseries) + 1
        idx = 0
        while idx < len(eqtimeseries):
            eqtimeseries[idx].db.data_id = next_id
            next_id        = next_id + 1
            idx            = idx  + 1

    log.debug("Updating data IDs")
    #Now fill in the final piece of data before inserting the new
    #data rows -- the data ids generated from the data inserts.
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

    log.debug("Isolating new data")
    #Isolate only the new datasets and insert them
    new_scenario_data = []
    for sd in all_datset_objects:
        if sd.db.dataset_id is None:
            new_scenario_data.append(sd)

    if len(new_scenario_data) > 0:
        log.debug("Inserting new datasets")
        last_dataset_id = IfaceLib.bulk_insert(new_scenario_data, 'tDataset')

        #set the dataset ids on the new dataset objects
        next_id = last_dataset_id - len(new_scenario_data) + 1
        idx = 0

        log.debug("Updating dataset IDS")
        while idx < len(new_scenario_data):
            dataset_id     = next_id
            new_scenario_data[idx].db.dataset_id = dataset_id
            next_id        = next_id + 1
            idx            = idx     + 1

        #Update all the metadata with their dataset_ids and insert them.
        all_metadata = []
        for sd in new_scenario_data:
            sd_meta = metadata[sd.db.data_hash]
            for m in sd_meta:
                m.db.dataset_id = sd.db.dataset_id
                all_metadata.append(m)
                sd.metadata = m
        IfaceLib.bulk_insert(all_metadata, 'tMetadata')

        #using the hash of the new datasets, find the placeholders in dataset_ids
        #and replace it with the dataset_id.
        log.debug("Putting new data with existing data to complete function")
        sd_dict = dict([(sd.db.data_hash, sd) for sd in new_scenario_data])
        for idx, d in enumerate(dataset_hashes):
            if type(d) == int:
                dataset_hashes[idx] = sd_dict[d]
    log.info("Done bulk inserting data. %s datasets", len(dataset_hashes))
    return dataset_hashes 

def _process_incoming_data(data, user_id=None):

    unit = units.Units()

    datasets = {}

    for d in data:
        val = parse_value(d)

        if val is None:
            log.info("Cannot parse data (dataset_id=%s). "
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

        dataset_metadata = {}
        if d.metadata is not None:
            for m in d.metadata:
                dataset_metadata[m.name] = m.value

        scenario_datum.set_metadata(dataset_metadata)

        data_hash = scenario_datum.set_hash(val)
        scenario_datum.db.data_hash = data_hash
        scenario_datum.val = val
        datasets[data_hash] =scenario_datum 
        d.data_hash = data_hash

    return data, datasets


def _get_existing_data(hashes):

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

        dataset.get_metadata()
        
        hash_dict[dr.data_hash] = dataset
    log.info("Retrieved %s datasets", len(hash_dict))

    return hash_dict


def get_dataset_group(group_name,**kwargs):
    grp_i = HydraIface.DatasetGroup()
    grp_i.db.group_name = group_name

    grp_i.load()
    grp_i.commit()

    return grp_i

def add_dataset_group(group,**kwargs):

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

def get_groups_like_name(group_name,**kwargs):
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
        groups.append(g.get_as_dict())

    return groups

def get_group_datasets(group_id,**kwargs):
    """
        Get all the datasets from the group with the specified name
    """
    group_datasets = []

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
        d = HydraIface.Dataset(dataset_id=r.dataset_id)
        group_datasets.append(d.get_as_dict())

    return group_datasets 

def get_val_at_time(dataset_id, timestamps,**kwargs):
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

def get_vals_between_times(dataset_id, start_time, end_time, timestep,**kwargs):
    server_start_time = get_datetime(start_time)
    server_end_time   = get_datetime(end_time)

    times = [timestamp_to_ordinal(server_start_time)]

    next_time = server_start_time
    while next_time < server_end_time:
        next_time = next_time  + datetime.timedelta(**{timestep:1})
        times.append(timestamp_to_ordinal(next_time))

    td = HydraIface.Dataset(dataset_id=dataset_id)
    log.debug("Number of times to fetch: %s", len(times))
    data = td.get_val(timestamp=times)

    data = td.get_val(timestamp=times)
    data_to_return = []
    if type(data) is list:
        for d in data:
            data_to_return.append(create_dict(d))
    else:
        data_to_return.append(data)

    dataset = {'data' : data_to_return}

    return dataset

def delete_dataset(dataset_id,**kwargs):
    """
        Removes a piece of data from the DB.
        CAUTION! Use with care, as this cannot be undone easily.
    """
    d = HydraIface.Dataset(dataset_id=dataset_id)
    d.delete()
    d.save()
 
def update_dataset(dataset,**kwargs):
    d = HydraIface.Dataset(dataset_id=dataset.id)
    d.db.data_type  = dataset.type
    d.db.data_units = dataset.unit
    d.db.data_dimen = dataset.dimension
    d.db.data_name  = dataset.name
    d.db.locked     = dataset.locked
    val = parse_value(dataset.value)
    d.set_val(val)
    d.set_hash()
    d.save()
