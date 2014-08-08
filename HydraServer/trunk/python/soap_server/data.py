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
from spyne.model.primitive import Integer, Boolean, Unicode, AnyDict
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import Descriptor,\
        TimeSeries,\
        EqTimeSeries,\
        Scalar,\
        Array as HydraArray,\
        Dataset,\
        Scenario,\
        DatasetGroup

from lib import data

from hydra_base import HydraService

class DataService(HydraService):

    """
        The data SOAP service
    """

    @rpc(Integer, _returns=Dataset)
    def get_dataset(ctx, dataset_id):
        """
            Get a single dataset, by ID
        """
        dataset_i = data.get_dataset(dataset_id, **ctx.in_header.__dict__)
        return Dataset(dataset_i)


    @rpc(SpyneArray(Dataset), _returns=SpyneArray(Integer))
    def bulk_insert_data(ctx, bulk_data):
        """
            Insert sereral pieces of data at once.
        """
        datasets = data.bulk_insert_data(bulk_data, **ctx.in_header.__dict__)

        return [d.dataset_id for d in datasets]


    @rpc(Integer, _returns=SpyneArray(Scenario))
    def get_dataset_scenarios(ctx, dataset_id):

        dataset_scenarios = data.get_dataset_scenarios(dataset_id, **ctx.in_header.__dict__)
        return [Scenario(s) for s in dataset_scenarios]

    @rpc(Unicode, _returns=DatasetGroup)
    def get_dataset_group(ctx, group_name):

        dataset_grp_i = data.get_dataset_group(group_name, **ctx.in_header.__dict__)
        return DatasetGroup(dataset_grp_i)

    @rpc(DatasetGroup, _returns=DatasetGroup)
    def add_dataset_group(ctx, group):

        dataset_grp_i = data.add_dataset_group(group, **ctx.in_header.__dict__)

        new_grp = DatasetGroup(dataset_grp_i)
        return new_grp

    @rpc(Unicode, _returns=SpyneArray(DatasetGroup))
    def get_groups_like_name(ctx, group_name):
        """
            Get all the datasets from the group with the specified name
        """
        groups = data.get_groups_like_name(group_name, **ctx.in_header.__dict__)
        ret_groups = [DatasetGroup(g) for g in groups]
        return ret_groups

    @rpc(Integer, _returns=SpyneArray(Dataset))
    def get_group_datasets(ctx, group_id):
        """
            Get all the datasets from the group with the specified name
        """
        group_datasets = data.get_group_datasets(group_id,
                                                 **ctx.in_header.__dict__)
        ret_data = [Dataset(d) for d in group_datasets]

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

    @rpc(Integer, SpyneArray(Unicode), _returns=AnyDict)
    def get_val_at_time(ctx, dataset_id, timestamps):
        return data.get_val_at_time(dataset_id, 
                                    timestamps,
                                    **ctx.in_header.__dict__)

    @rpc(Integer,Unicode,Unicode,Unicode(values=['seconds', 'minutes', 'hours', 'days', 'months']),_returns=AnyDict)
    def get_vals_between_times(ctx, dataset_id, start_time, end_time, timestep):
        return data.get_vals_between_times(dataset_id, 
                                           start_time, 
                                           end_time, 
                                           timestep,
                                           **ctx.in_header.__dict__)
