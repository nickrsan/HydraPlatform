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
from spyne.model.primitive import Integer, Integer32, Boolean, Unicode, AnyDict, Decimal
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import Dataset,\
        DatasetCollection

from HydraServer.lib import data

import json

from hydra_base import HydraService

class DataService(HydraService):

    """
        The data SOAP service
    """

    @rpc(Dataset, _returns=Dataset)
    def add_dataset(ctx, dataset):
        """
           Add a single dataset. Return the new dataset with a dataset ID.
        """
        value = dataset.parse_value()
        metadata = dataset.get_metadata_as_dict(user_id=ctx.in_header.user_id)
        dataset_i = data.add_dataset(dataset.type,
                                     value,
                                     dataset.unit,
                                     dataset.dimension,
                                     metadata,
                                     dataset.name,
                                     ctx.in_header.user_id,
                                    flush=True)
        
        return Dataset(dataset_i)

    @rpc(SpyneArray(Integer32), _returns=SpyneArray(Dataset))
    def get_datasets(ctx, dataset_ids):
        """
            Get a list of datasets, by ID
        """
        datasets = data.get_datasets(dataset_ids, **ctx.in_header.__dict__)
        ret_datasets = [Dataset(d) for d in datasets]
        return ret_datasets

    @rpc(Integer, _returns=Dataset)
    def get_dataset(ctx, dataset_id):
        """
            Get a single dataset, by ID
        """
        dataset_i = data.get_dataset(dataset_id, **ctx.in_header.__dict__)
        return Dataset(dataset_i)

    @rpc(Integer, Unicode, Unicode, Unicode, Unicode, Unicode,
         Integer, Unicode, Unicode,
         Integer, Integer, Unicode,
         Unicode(pattern='[YN]', default='N'), #include metadata flag
         Unicode(pattern='[YN]', default='N'), # include value flag
         Integer(default=0),Integer(default=2000), #start, size page flags
         _returns=SpyneArray(Dataset))
    def search_datasets(ctx, dataset_id,
                name,
                collection_name,
                data_type,
                dimension,
                unit,
                scenario_id,
                metadata_name,
                metadata_val,
                attr_id,
                type_id,
                unconnected,
                inc_metadata,
                inc_val,
                page_start,
                page_size):
        """
            Search for datadets that satisfy the criteria specified.
        """
        datasets = data.search_datasets(dataset_id,
                                     name,
                                     collection_name,
                                     data_type,
                                     dimension,
                                     unit,
                                     scenario_id,
                                     metadata_name,
                                     metadata_val,
                                     attr_id,
                                     type_id,
                                     unconnected,
                                     inc_metadata,
                                     inc_val,
                                     page_start,
                                     page_size,
                                     **ctx.in_header.__dict__)

        cm_datasets = []
        for d in datasets:
            cm_datasets.append(Dataset(d))

        return cm_datasets

    @rpc(Integer(max_occurs="unbounded"), _returns=Unicode)
    def get_metadata(ctx, dataset_ids):
        """
            Get the metadata for a dataset or list of datasets
        """

        if type(dataset_ids) == int:
            dataset_ids = [dataset_ids]
        
        metadata = data.get_metadata(dataset_ids)
        metadata_dict = {}
        for m in metadata:
            metadata_dict[m.metadata_name] = m.metadata_val

        return json.dumps(metadata_dict)

    @rpc(SpyneArray(Dataset), _returns=SpyneArray(Integer))
    def bulk_insert_data(ctx, bulk_data):
        """
            Insert sereral pieces of data at once.
        """
        datasets = data.bulk_insert_data(bulk_data, **ctx.in_header.__dict__)

        return [d.dataset_id for d in datasets]

    @rpc(_returns=SpyneArray(DatasetCollection))
    def get_all_dataset_collections(ctx):
        """
            Get all the dataset collections available. 
        """
        dataset_colns = data.get_all_dataset_collections(**ctx.in_header.__dict__)
        all_colns = []
        for d_g in dataset_colns:
            all_colns.append(DatasetCollection(d_g))
        return all_colns


    @rpc(Integer, Integer, _returns=Unicode)
    def add_dataset_to_collection(ctx, dataset_id, collection_id):
        """
            Add a single dataset to a dataset collection.
        """

        data.add_dataset_to_collection(dataset_id,
                                       collection_id,
                                       **ctx.in_header.__dict__)
        return 'OK'

    @rpc(SpyneArray(Integer32), Integer, _returns=Unicode)
    def add_datasets_to_collection(ctx, dataset_ids, collection_id):
        """
            Add multiple datasets to a dataset collection.
        """
        data.add_datasets_to_collection(dataset_ids,
                                        collection_id,
                                        **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, Integer, _returns=Unicode)
    def remove_dataset_from_collection(ctx, dataset_id, collection_id):
        """
            Remove a single dataset to a dataset collection.
        """
        data.remove_dataset_from_collection(dataset_id,
                                             collection_id,
                                             **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, Integer, _returns=Unicode(pattern='[YN]'))
    def check_dataset_in_collection(ctx, dataset_id, collection_id):
        """
            Check whether a dataset is contained inside a collection
        """
        
        result = data.check_dataset_in_collection(dataset_id,
                                         collection_id,
                                         **ctx.in_header.__dict__)
        return result

    @rpc(Integer, _returns=DatasetCollection)
    def get_dataset_collection(ctx, collection_id):
        """
            Get a single dataset collection, by ID.
            :param Collection ID.
        """

        dataset_coln_i = data.get_dataset_collection(collection_id,
                                                     **ctx.in_header.__dict__)
        return DatasetCollection(dataset_coln_i)

    @rpc(Unicode, _returns=DatasetCollection)
    def get_dataset_collection_by_name(ctx, collection_name):
        """
            Get all the dataset collections with the provided name.
            :param A collection name.
        """
        dataset_coln_i = data.get_dataset_collection_by_name(collection_name,
                                                             **ctx.in_header.__dict__)
        return DatasetCollection(dataset_coln_i)

    @rpc(DatasetCollection, _returns=DatasetCollection)
    def add_dataset_collection(ctx, collection):
        """
            Add a dataset collection:
            The name of the collection does NOT need to be unique, so be careful
            with the naming to ensure the collection is searchable later.

            :param DatasetCollection object, containing a list of DatasetCollectionItem objects
        """

        dataset_coln_i = data.add_dataset_collection(collection, **ctx.in_header.__dict__)

        new_coln = DatasetCollection(dataset_coln_i)
        return new_coln

    @rpc(Unicode, _returns=SpyneArray(DatasetCollection))
    def get_collections_like_name(ctx, collection_name):
        """
            Get all the dataset collections with a name like the specified name
            :param The collection name to search for.
        """
        collections = data.get_collections_like_name(collection_name, **ctx.in_header.__dict__)
        ret_collections = [DatasetCollection(g) for g in collections]
        return ret_collections

    @rpc(Integer, _returns=SpyneArray(Dataset))
    def get_collection_datasets(ctx, collection_id):
        """
            Get all the datasets from the collection with the specified name
        """
        collection_datasets = data.get_collection_datasets(collection_id,
                                                 **ctx.in_header.__dict__)
        ret_data = [Dataset(d) for d in collection_datasets]

        return ret_data

    @rpc(Dataset, _returns=Dataset)
    def update_dataset(ctx, dataset):
        """
            Update a piece of data directly, rather than through a resource
            scenario.
        """
        val = dataset.parse_value()

        metadata = dataset.get_metadata_as_dict()

        updated_dataset = data.update_dataset(dataset.id,
                                        dataset.name,
                                        dataset.type,
                                        val,
                                        dataset.unit,
                                        dataset.dimension,
                                        metadata,
                                        **ctx.in_header.__dict__)

        return Dataset(updated_dataset)


    @rpc(Integer, _returns=Boolean)
    def delete_dataset(ctx, dataset_id):
        """
            Removes a piece of data from the DB.
            CAUTION! Use with care, as this cannot be undone easily.
        """
        success = True
        data.delete_dataset(dataset_id, **ctx.in_header.__dict__)
        return success

    @rpc(Integer, Unicode(min_occurs=0, max_occurs='unbounded'), _returns=AnyDict)
    def get_val_at_time(ctx, dataset_id, timestamps):
        return data.get_val_at_time(dataset_id, 
                                    timestamps,
                                    **ctx.in_header.__dict__)
    
    @rpc(Integer32(min_occurs=0, max_occurs='unbounded'), Unicode(min_occurs=0, max_occurs='unbounded'), _returns=AnyDict)
    def get_multiple_vals_at_time(ctx, dataset_ids, timestamps):
        result = data.get_multiple_vals_at_time(dataset_ids, 
                                    timestamps,
                                    **ctx.in_header.__dict__)
        return result

    @rpc(Integer,Unicode,Unicode,Unicode(values=['seconds', 'minutes', 'hours', 'days', 'months']), Decimal(default=1),_returns=AnyDict)
    def get_vals_between_times(ctx, dataset_id, start_time, end_time, timestep, increment):
        return data.get_vals_between_times(dataset_id, 
                                           start_time, 
                                           end_time, 
                                           timestep,
                                           increment,
                                           **ctx.in_header.__dict__)

    @rpc(Unicode, _returns=Unicode)
    def check_json(ctx, json_string):
        try:
            data.check_json(json_string)
        except Exception, e:
            return "Unable to process JSON string. error was: %s"%e

        return 'OK'


