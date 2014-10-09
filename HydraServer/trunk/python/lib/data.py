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
from HydraLib.dateutil import get_datetime
from HydraLib import units
import logging
from db.model import Dataset, Metadata, DatasetOwner, DatasetGroup, DatasetGroupItem
from util import generate_data_hash
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.expression import case
from sqlalchemy import null
from db import DBSession

import pandas as pd
from HydraLib.HydraException import HydraError, ResourceNotFoundError
from sqlalchemy import and_
from HydraLib.util import create_dict
from decimal import Decimal
import copy

unit = units.Units()

global FORMAT
FORMAT = "%Y-%m-%d %H:%M:%S.%f"
global qry_in_threshold
qry_in_threshold = 999 
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
                Dataset.hidden,
                DatasetOwner.user_id,
                null().label('metadata'),
                case([(and_(Dataset.hidden=='Y', DatasetOwner.user_id is not None), None)], 
                        else_=Dataset.start_time).label('start_time'),
                case([(and_(Dataset.hidden=='Y', DatasetOwner.user_id is not None), None)], 
                        else_=Dataset.frequency).label('frequency'),
                case([(and_(Dataset.hidden=='Y', DatasetOwner.user_id is not None), None)], 
                        else_=Dataset.value).label('value')).filter(
                Dataset.dataset_id==dataset_id).outerjoin(DatasetOwner, 
                                    and_(DatasetOwner.dataset_id==Dataset.dataset_id, 
                                    DatasetOwner.user_id==user_id)).one()

        #convert the value row into a string as it is returned as a binary
        if dataset.value is not None:
            dataset.value = str(dataset.value)

        if dataset.data_type == 'timeseries' and (dataset.hidden == 'N' or (Dataset.hidden == 'Y' and dataset.user_id is not None)):
            metadata = DBSession.query(Metadata).filter(Metadata.dataset_id==dataset_id).all()
            dataset.metadata = metadata
        else:
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

    
def add_dataset(data_type, val, units, dimension, metadata={}, name="", user_id=None, flush=False):
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
        if existing_dataset.check_user(user_id):
            d = existing_dataset
        else:
            d.set_metadata({'created_at': datetime.datetime.now()})
            d.set_hash()
            DBSession.add(d)
    except NoResultFound:
        DBSession.add(d)

    if flush == True:
        DBSession.flush()
    return d

def bulk_insert_data(data, **kwargs):
    datasets = _bulk_insert_data(data, user_id=kwargs.get('user_id'), source=kwargs.get('app_name'))
    DBSession.flush()
    return datasets

def _make_new_dataset(dataset_dict):
    #If the user is not allowed to use the existing dataset, a new
    #one must be created. This means a unique hash must be created
    #To create a unique hash, add a unique piece of metadata.
    new_dataset = copy.deepcopy(dataset_dict)
    new_dataset['metadata']['created_at'] = datetime.datetime.now()
    new_hash = generate_data_hash(new_dataset)
    new_dataset['data_hash'] = new_hash

    return new_dataset

def _bulk_insert_data(bulk_data, user_id=None, source=None):
    """
        Insert lots of datasets at once to reduce the number of DB interactions.
        user_id indicates the user adding the data
        source indicates the name of the app adding the data
        both user_id and source are added as metadata
    """
    get_timing = lambda x: datetime.datetime.now() - x
    start_time=datetime.datetime.now()
   
    new_data = _process_incoming_data(bulk_data, user_id, source)
    log.info("Incoming data processed in %s", (get_timing(start_time)))

    existing_data = _get_existing_data(new_data.keys())
    log.info("Existing data retrieved.")
    
    #The list of dataset IDS to be returned.
    hash_id_map = {}
    new_datasets = []
    metadata         = {}
    #This is what gets returned.
    for d in bulk_data:
        dataset_dict = new_data[d.data_hash]
        current_hash = d.data_hash

        #if this piece of data is already in the DB, then
        #there is no need to insert it!
        if  existing_data.get(current_hash) is not None:

            dataset = existing_data.get(current_hash)
            #Is this user allowed to use this dataset?
            if dataset.check_user(user_id) == False:
                new_dataset = _make_new_dataset(dataset_dict)
                new_datasets.append(new_dataset)
                metadata[new_dataset['data_hash']] = dataset_dict['metadata']
            else:
                hash_id_map[current_hash] = dataset#existing_data[current_hash]
        elif current_hash in hash_id_map:
            new_datasets.append(dataset_dict)
        else:
            #set a placeholder for a dataset_id we don't know yet.
            #The placeholder is the hash, which is unique to this object and
            #therefore easily identifiable.
            new_datasets.append(dataset_dict)
            hash_id_map[current_hash] = dataset_dict
            metadata[current_hash] = dataset_dict['metadata']

    log.debug("Isolating new data", get_timing(start_time))
    #Isolate only the new datasets and insert them
    new_data_for_insert = []
    #keep track of the datasets that are to be inserted to avoid duplicate
    #inserts
    new_data_hashes = []
    for d in new_datasets:
        if d['data_hash'] not in new_data_hashes:
            new_data_for_insert.append(d)
            new_data_hashes.append(d['data_hash'])
    
    if len(new_data_for_insert) > 0:
        log.debug("Inserting new data", get_timing(start_time))
        DBSession.execute(Dataset.__table__.insert(), new_data_for_insert)
        log.debug("New data Inserted", get_timing(start_time))

        new_data = _get_existing_data(new_data_hashes)
        log.debug("New data retrieved", get_timing(start_time))
    
        for k, v in new_data.items():
            hash_id_map[k] = v
    
        _insert_metadata(metadata, hash_id_map)
        log.debug("Metadata inserted", get_timing(start_time))

    returned_ids = []
    for d in bulk_data:
        returned_ids.append(hash_id_map[d.data_hash])

    log.info("Done bulk inserting data. %s datasets", len(returned_ids))

    return returned_ids

def _insert_metadata(metadata_hash_dict, dataset_id_hash_dict):
    if metadata_hash_dict is None or len(metadata_hash_dict) == 0:
        return

    metadata_list = []
    for _hash, _metadata_dict in metadata_hash_dict.items():
        for k, v in _metadata_dict.items():
            metadata = {}
            metadata['metadata_name']  = k
            metadata['metadata_val']  = v
            metadata['dataset_id']      = dataset_id_hash_dict[_hash].dataset_id
            metadata_list.append(metadata)

    DBSession.execute(Metadata.__table__.insert(), metadata_list) 

def _process_incoming_data(data, user_id=None, source=None):

    datasets = {}

    for d in data:
        val = d.parse_value()

        if val is None:
            log.info("Cannot parse data (dataset_id=%s). "
                         "Value not available.",d)
            continue

        data_dict = {
            'data_type':d.type,
             'data_name':d.name,
            'data_units': d.unit,
            'created_by' : user_id,
            'frequency' : None,
            'start_time': None,
        }

        # Assign dimension if necessary
        if d.unit is not None and d.dimension is None:
            data_dict['data_dimen'] = unit.get_dimension(d.unit)
        else:
            data_dict['data_dimen'] = d.dimension

        if d.type == 'eqtimeseries':
            st, f, v = _get_db_val(d.type, val)
            data_dict['start_time'] = st
            data_dict['frequency']  = f
            data_dict['value']      = v
        else:
            db_val = _get_db_val(d.type, val)
            data_dict['value'] = db_val

        metadata_dict = {}
        if d.metadata is not None:
            for m in d.metadata:
                metadata_dict[str(m.name)]  = str(m.value)
        
        if user_id is not None and 'user_id' not in metadata_dict.keys():
            metadata_dict['user_id'] = str(user_id)
        if source is not None and 'source' not in metadata_dict.keys():
            metadata_dict['source'] = str(source)

        data_dict['metadata'] = metadata_dict

        d.data_hash = generate_data_hash(data_dict)
        data_dict['data_hash'] = d.data_hash
        datasets[d.data_hash] = data_dict 

    return datasets

def _get_db_val(data_type, val):
    if data_type in ('descriptor','scalar','array'):
        return str(val)
    elif data_type == 'eqtimeseries':
        return (str(val[0]), str(val[1]), str(val[2]))
    elif data_type == 'timeseries':
        return val 
    else:
        raise HydraError("Invalid data type %s"%(data_type,))

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
        raise ResourceNotFoundError("No dataset group found with id %s"%group_id)

    return group

def get_dataset_group_by_name(group_name,**kwargs):
    try:
        group = DBSession.query(DatasetGroup).filter(DatasetGroup.group_name==group_name).one()
    except NoResultFound:
        raise ResourceNotFoundError("No dataset group found with id %s"%group_name)

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
    dataset_i = DBSession.query(Dataset).filter(Dataset.dataset_id==dataset_id).one()
    #for time in t:
    #    data.append(td.get_val(timestamp=time))
    data = dataset_i.get_val(timestamp=t)
    if type(data) is list:
        data = create_dict(data)
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

