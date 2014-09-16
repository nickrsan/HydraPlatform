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
from HydraLib.util import get_datetime
from HydraLib import units
import logging
from db.model import Dataset, Metadata, TimeSeriesData, DatasetOwner, DatasetGroup, DatasetGroupItem
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import case
from sqlalchemy import null
from db import DBSession

import pandas as pd
from HydraLib.HydraException import HydraError, ResourceNotFoundError
from sqlalchemy import and_
from HydraLib.util import create_dict
from decimal import Decimal
global FORMAT
FORMAT = "%Y-%m-%d %H:%M:%S.%f"
global qry_in_threshold
qry_in_threshold = 500
#"2013-08-13T15:55:43.468886Z"

current_module = sys.modules[__name__]
NS = "soap_server.hydra_complexmodels"

log = logging.getLogger(__name__)

def get_dataset(dataset_id,**kwargs):
    """
        Get a single dataset, by ID
    """

    user_id = int(kwargs.get('user_id'))

    if dataset_id is None:
        return None
    try:
        dataset = DBSession.query(Dataset.dataset_id,
                Dataset.data_type,
                Dataset.data_units,
                Dataset.data_dimen,
                Dataset.data_name,
                Dataset.locked,
                DatasetOwner.user_id,
                null().label('timeseriesdata'),
                null().label('metadata'),
                case([(and_(Dataset.locked=='Y', DatasetOwner.user_id is not None), None)], 
                        else_=Dataset.start_time).label('start_time'),
                case([(and_(Dataset.locked=='Y', DatasetOwner.user_id is not None), None)], 
                        else_=Dataset.frequency).label('frequency'),
                case([(and_(Dataset.locked=='Y', DatasetOwner.user_id is not None), None)], 
                        else_=Dataset.value).label('value')).filter(
                Dataset.dataset_id==dataset_id).outerjoin(DatasetOwner, 
                                    and_(DatasetOwner.dataset_id==Dataset.dataset_id, 
                                    DatasetOwner.user_id==user_id)).one()

        if dataset.data_type == 'timeseries' and (dataset.locked == 'N' or (Dataset.locked == 'Y' and dataset.user_id is not None)):
            tsdata = DBSession.query(TimeSeriesData).filter(TimeSeriesData.dataset_id==dataset_id).all()
            metadata = DBSession.query(Metadata).filter(Metadata.dataset_id==dataset_id).all()
            dataset.timeseriesdata = tsdata
            dataset.metadata = metadata
        else:
            dataset.timeseriesdata = []
            dataset.metadata = []
    except NoResultFound:
        raise HydraError("Dataset %s does not exist."%(dataset_id))

    return dataset 

def update_dataset(dataset_id, name, data_type, val, units, dimension, metadata={}, **kwargs):
    """
        Update an existing dataset
    """

    if dataset_id is None:
        raise HydraError("Dataset must have an ID to be updated.")

    dataset = DBSession.query(Dataset).filter(Dataset.dataset_id==dataset_id).one()
    #This dataset been seen before, so it may be attached
    #to other scenarios, which may be locked. If they are locked, we must
    #not change their data, so new data must be created for the unlocked scenarios
    locked_scenarios = []
    unlocked_scenarios = []
    for dataset_rs in dataset.resourcescenarios:
        if dataset_rs.scenario.locked == 'Y':
            locked_scenarios.append(dataset_rs)
        else:
            unlocked_scenarios.append(dataset_rs)

    #Are any of these scenarios locked?
    if len(locked_scenarios) > 0:
        #If so, create a new dataset and assign to all unlocked datasets.
        dataset = add_dataset(data_type,
                                val,
                                units,
                                dimension,
                                metadata=metadata,
                                name=name,
                                user_id=kwargs['user_id'])
        for unlocked_rs in unlocked_scenarios:
            unlocked_rs.dataset = dataset

    else:

        dataset.set_val(data_type, val)

        dataset.set_metadata(metadata)

        dataset.data_type  = data_type
        dataset.data_units = units
        dataset.data_name  = name
        dataset.data_dimen = dimension
        dataset.created_by = kwargs['user_id']
        dataset.data_hash  = dataset.set_hash()

    return dataset

    
def add_dataset(data_type, val, units, dimension, metadata={}, name="", user_id=None):
    """
        Data can exist without scenarios. This is the mechanism whereby
        single pieces of data can be added without doing it through a scenario.

        A typical use of this would be for setting default values on types.
    """

    d = Dataset()

    d.set_val(data_type, val)

    d.set_metadata(metadata)

    d.data_type  = data_type
    d.data_units = units
    d.data_name  = name
    d.data_dimen = dimension
    d.created_by = user_id
    d.data_hash  = d.set_hash()
 
    try:
        existing_dataset = DBSession.query(Dataset).filter(Dataset.data_hash==d.data_hash).one()
        return existing_dataset
    except NoResultFound:
        DBSession.add(d)
        return d

def bulk_insert_data(data, **kwargs):
    datasets = _bulk_insert_data(data, user_id=kwargs.get('user_id'), source=kwargs.get('app_name'))
    DBSession.flush()
    return datasets

def _bulk_insert_data(bulk_data, user_id=None, source=None):
    """
        Insert lots of datasets at once to reduce the number of DB interactions.
        user_id indicates the user adding the data
        source indicates the name of the app adding the data
        both user_id and source are added as metadata
    """
    get_timing = lambda x: datetime.datetime.now() - x

    start_time=datetime.datetime.now()
    log.info("Starting data insert (%s datasets) %s", len(bulk_data), get_timing(start_time))
    
    datasets, incoming_data  = _process_incoming_data(bulk_data, user_id, source)
    log.info("Incoming data processed.")
    existing_data = _get_existing_data(incoming_data.keys())
    log.info("Existing data retrieved.")

    #A list of all the dataset objects
    all_dataset_objects = []

    metadata         = {}

    #This is what gets returned.
    dataset_hashes = []
    log.debug("Processing data %s", get_timing(start_time)) 
    for d in bulk_data:
        dataset_i = incoming_data[d.data_hash]
        current_hash = d.data_hash

        #if this piece of data is already in the DB, then
        #there is no need to insert it!
        if  existing_data.get(current_hash):

            existing_datum = existing_data[current_hash]
            #Is this user allowed to use this dataset?
            if existing_datum.check_user(user_id) == False:
                #If the user is not allowed to use the existing dataset, a new
                #one must be created. This means a unique hash must be created
                #To create a unique hash, add a unique piece of metadata.
                datum_metadata = existing_datum.get_metadata_as_dict()
                datum_metadata['created_at'] = datetime.datetime.now()
                dataset_i.set_metadata(datum_metadata)
                new_hash = dataset_i.set_hash()

                dataset_hashes.append(new_hash)
                all_dataset_objects.append(dataset_i)
                metadata[new_hash] = dataset_i.metadata 
            else:
                dataset_hashes.append(existing_data[current_hash])
                all_dataset_objects.append(existing_data[current_hash])
            continue
        elif current_hash in dataset_hashes:
            dataset_hashes.append(current_hash)
            continue
        else:
            #set a placeholder for a dataset_id we don't know yet.
            #The placeholder is the hash, which is unique to this object and
            #therefore easily identifiable.
            dataset_hashes.append(current_hash)
            all_dataset_objects.append(dataset_i)
            metadata[current_hash] = dataset_i.metadata

    log.debug("Isolating new data")
    #Isolate only the new datasets and insert them
    new_scenario_data = []

    for sd in all_dataset_objects:
        if sd.dataset_id is None and sd not in new_scenario_data:
            DBSession.add(sd)
            new_scenario_data.append(sd)

    if len(new_scenario_data) > 0:
        #using the hash of the new datasets, find the placeholders in dataset_ids
        #and replace it with the dataset_id.
        log.debug("Putting new data with existing data to complete function")
        sd_dict = dict([(sd.data_hash, sd) for sd in new_scenario_data])
        for idx, d in enumerate(dataset_hashes):
            if type(d) == int:
                dataset_hashes[idx] = sd_dict[d]
    log.info("Done bulk inserting data. %s datasets", len(dataset_hashes))
    return dataset_hashes 

def _process_incoming_data(data, user_id=None, source=None):
    unit = units.Units()

    datasets = {}

    for d in data:
        val = d.parse_value()

        if val is None:
            log.info("Cannot parse data (dataset_id=%s). "
                         "Value not available.",d)
            continue

        scenario_datum = Dataset()
        scenario_datum.data_type  = d.type
        scenario_datum.data_name  = d.name
        scenario_datum.data_units = d.unit
        scenario_datum.dataset_id = d.id
        scenario_datum.created_by = user_id
        # Assign dimension if necessary
        if d.unit is not None and d.dimension is None:
            scenario_datum.data_dimen = unit.get_dimension(d.unit)
        else:
            scenario_datum.data_dimen = d.dimension

        scenario_datum.set_val(d.type, val)

        metadata_names = []
        if d.metadata is not None:
            for m in d.metadata:
                metadata_names.append(m.name)
                scenario_datum.metadata.append(Metadata(metadata_name=m.name,metadata_val=m.value))
        
        if user_id is not None and 'user_id' not in metadata_names:
            scenario_datum.metadata.append(Metadata(metadata_name='user_id',metadata_val=str(user_id)))
        if source is not None and 'source' not in metadata_names:
            scenario_datum.metadata.append(Metadata(metadata_name='source',metadata_val=str(source)))

        data_hash = scenario_datum.set_hash()

        datasets[data_hash] = scenario_datum 
        d.data_hash = data_hash

    return data, datasets

def _get_timeseriesdata(dataset_ids):
    """
        Get all the timeseries data entries for a given list of
        dataset ids.
    """
    if len(dataset_ids) == 0:
        return []

    tsdata = []
    if len(dataset_ids) > qry_in_threshold:
        idx = 0
        extent = qry_in_threshold - 1
        while idx < len(dataset_ids):
            log.info("Querying %s timeseries", len(dataset_ids[idx:extent]))
            rs = DBSession.query(TimeSeriesData).filter(TimeSeriesData.dataset_id.in_(dataset_ids[idx:extent])).all()
            tsdata.extend(rs)
            idx = idx + qry_in_threshold 
            
            if idx + qry_in_threshold > len(dataset_ids):
                extent = len(dataset_ids)
            else:
                extent = extent +qry_in_threshold 
    else:
        ts_qry = DBSession.query(TimeSeriesData).filter(TimeSeriesData.dataset_id.in_(dataset_ids)).all()
        for ts in ts_qry:
            tsdata.append(ts)

    return tsdata 

def get_metadata(dataset_ids, **kwargs):
    return _get_metadata(dataset_ids)

def _get_metadata(dataset_ids):
    """
        Get all the metadata for a given list of datasets
    """
    metadata = []
    if len(dataset_ids) == 0:
        return []
    if len(dataset_ids) > qry_in_threshold:
        idx = 0
        extent = qry_in_threshold 
        while idx < len(dataset_ids):
            log.info("Querying %s metadatas", len(dataset_ids[idx:extent]))
            rs = DBSession.query(Metadata).filter(Metadata.dataset_id.in_(dataset_ids[idx:extent])).all()
            metadata.extend(rs)
            idx = idx + qry_in_threshold 
            
            if idx + qry_in_threshold > len(dataset_ids):
                extent = len(dataset_ids)
            else:
                extent = extent +qry_in_threshold 
    else:
        metadata_qry = DBSession.query(Metadata).filter(Metadata.dataset_id.in_(dataset_ids))
        for m in metadata_qry:
            metadata.append(m)

    return metadata

def _get_existing_data(hashes):

    str_hashes = [str(h) for h in hashes]

    hash_dict = {}

    datasets = []
    if len(str_hashes) > qry_in_threshold:
        idx = 0
        extent =qry_in_threshold 
        while idx < len(str_hashes):
            log.info("Querying %s datasets", len(str_hashes[idx:extent]))
            rs = DBSession.query(Dataset).filter(Dataset.data_hash.in_(str_hashes[idx:extent])).all()
            datasets.extend(rs)
            idx = idx + qry_in_threshold 
            
            if idx + qry_in_threshold > len(str_hashes):
                extent = len(str_hashes)
            else:
                extent = extent + qry_in_threshold 
    else:
        datasets = DBSession.query(Dataset).filter(Dataset.data_hash.in_(str_hashes))


    for r in datasets:
        hash_dict[r.data_hash] = r

    log.info("Retrieved %s datasets", len(hash_dict))

    return hash_dict


def get_dataset_group(group_id,**kwargs):
    try:
        group = DBSession.query(DatasetGroup).filter(DatasetGroup.group_id==group_id).one()
    except NoResultFound:
        log.info("No dataset group found with id %s"%group_id)
        return None

    return group

def get_dataset_group_by_name(group_name,**kwargs):
    try:
        group = DBSession.query(DatasetGroup).filter(DatasetGroup.group_name==group_name).one()
    except NoResultFound:
        log.info("No dataset group found with name %s"%group_name)
        return None

    return group

def add_dataset_group(group,**kwargs):

    grp_i = DatasetGroup(group_name=group.group_name)

    for dataset_id in group.dataset_ids:
        datasetitem = DatasetGroupItem(dataset_id=dataset_id)
        grp_i.items.append(datasetitem)
    DBSession.add(grp_i)
    DBSession.flush()
    return grp_i

def get_groups_like_name(group_name,**kwargs):
    """
        Get all the datasets from the group with the specified name
    """
    try:
        groups = DBSession.query(DatasetGroup).filter(DatasetGroup.group_name.like("%%%s%%"%group_name.lower())).all()
    except NoResultFound:
        raise ResourceNotFoundError("No dataset group found with name %s"%group_name)

    return groups

def get_group_datasets(group_id,**kwargs):
    """
        Get all the datasets from the group with the specified name
    """
    group_datasets = DBSession.query(Dataset).filter(Dataset.dataset_id==DatasetGroupItem.dataset_id,
                                        DatasetGroupItem.group_id==DatasetGroup.group_id,
                                        DatasetGroup.group_id==group_id).all()
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
        t.append(get_datetime(time))
    td = DBSession.query(Dataset).filter(Dataset.dataset_id==dataset_id).one()
    #for time in t:
    #    data.append(td.get_val(timestamp=time))
    data = td.get_val(timestamp=t)
    dataset = {'data': data}

    return dataset

def get_vals_between_times(dataset_id, start_time, end_time, timestep,increment,**kwargs):
    """
        Retrive data between two specified times within a timeseries. The times
        need not be specified in the timeseries. This function will 'fill in the blanks'.

        Two types of data retrieval can be done. 

        If the timeseries is timestamp-based, then start_time and end_time
        must be datetimes and timestep must be specified (minutes, seconds etc).
        'increment' reflects the size of the timestep -- timestep = 'minutes' and increment = 2
        means 'every 2 minutes'.

        If the timeseries is float-based (relative), then start_time and end_time
        must be decimal values. timestep is ignored and 'increment' represents the increment
        to be used between the start and end. 
        Ex: start_time = 1, end_time = 5, increment = 1 will get times at 1, 2, 3, 4, 5
    """
    try:
        server_start_time = get_datetime(start_time)
        server_end_time   = get_datetime(end_time)
        times = [server_start_time]

        next_time = server_start_time
        while next_time < server_end_time:
            if int(increment) == 0:
                raise HydraError("%s is not a valid increment for this search."%increment)
            next_time = next_time  + datetime.timedelta(**{timestep:int(increment)})
            times.append(next_time)
    except ValueError:
        try:
            server_start_time = Decimal(start_time)
            server_end_time   = Decimal(end_time)
            times = [server_start_time]

            next_time = server_start_time
            while next_time < server_end_time:
                next_time = next_time + increment
                times.append(next_time)
        except:
            raise HydraError("Unable to get times. Please check to and from times.")

    td = DBSession.query(Dataset).filter(Dataset.dataset_id==dataset_id).one()
    log.debug("Number of times to fetch: %s", len(times))
    data = td.get_val(timestamp=times)

    data_to_return = []
    if type(data) is list:
        for d in data:
            data_to_return.append(create_dict(list(d)))
    else:
        data_to_return.append(data)

    dataset = {'data' : data_to_return}

    return dataset

def delete_dataset(dataset_id,**kwargs):
    """
        Removes a piece of data from the DB.
        CAUTION! Use with care, as this cannot be undone easily.
    """
    d = DBSession.query(Dataset).filter(Dataset.dataset_id==dataset_id).one()
    DBSession.delete(d)
    DBSession.flush()

def read_json(json_string):
    pd.read_json(json_string)

