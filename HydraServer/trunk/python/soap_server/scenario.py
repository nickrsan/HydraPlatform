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
from spyne.model.primitive import Integer, Unicode
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import Scenario,\
        ResourceScenario,\
        Dataset,\
        ScenarioDiff

from lib import scenario
from hydra_base import HydraService

class ScenarioService(HydraService):
    """
        The scenario SOAP service
        as all resources already exist, there is no need to worry
        about negative IDS
    """

    @rpc(Integer, _returns=Scenario)
    def get_scenario(ctx, scenario_id):
        """
            Get the specified scenario
        """
        scen = scenario.get_scenario(scenario_id, **ctx.in_header.__dict__)
        return Scenario(scen)

    @rpc(Integer, Scenario, _returns=Scenario)
    def add_scenario(ctx, network_id, scen):
        """
            Add a scenario to a specified network.
        """
        new_scen = scenario.add_scenario(network_id, scen, **ctx.in_header.__dict__)

        return Scenario(new_scen)

    @rpc(Scenario, _returns=Scenario)
    def update_scenario(ctx, scen):
        """
            Update a single scenario
            as all resources already exist, there is no need to worry
            about negative IDS
        """
        updated_scen = scenario.update_scenario(scen, **ctx.in_header.__dict__)
        return Scenario(updated_scen)

    @rpc(Integer, _returns=Unicode)
    def delete_scenario(ctx, scenario_id):
        """
            Set the status of a scenario to 'X'.
        """

        success = 'OK'
        scenario.update_scenario(scenario_id, **ctx.in_header.__dict__)
        return success


    @rpc(Integer, _returns=Scenario)
    def clone_scenario(ctx, scenario_id):

        cloned_scen = scenario.clone_scenario(scenario_id, **ctx.in_header.__dict__)

        return Scenario(cloned_scen)

    @rpc(Integer, Integer, _returns=ScenarioDiff)
    def compare_scenarios(ctx, scenario_id_1, scenario_id_2):
        scenariodiff = scenario.compare_scenarios(scenario_id_1,
                                                  scenario_id_2,
                                                  **ctx.in_header.__dict__)

        return ScenarioDiff(scenariodiff)


    @rpc(Integer, _returns=Unicode)
    def lock_scenario(ctx, scenario_id):
        result = scenario.lock_scenario(scenario_id, **ctx.in_header.__dict__)
        return result

    @rpc(Integer, _returns=Unicode)
    def unlock_scenario(ctx, scenario_id):
        result = scenario.unlock_scenario(scenario_id, **ctx.in_header.__dict__)
        return result

    @rpc(Integer, ResourceScenario, _returns=ResourceScenario)
    def update_resourcedata(ctx,scenario_id, resource_scenario):
        """
            Update the data associated with a scenario.
            Data missing from the resource scenario will not be removed
            from the scenario. Use the remove_resourcedata for this task.
        """
        res = scenario.update_resourcedata(scenario_id,
                                           resource_scenario,
                                           **ctx.in_header.__dict__)
        return ResourceScenario(res)

    @rpc(Integer, ResourceScenario, _returns=Unicode)
    def delete_resourcedata(ctx,scenario_id, resource_scenario):
        """
            Remove the data associated with a resource in a scenario.
        """
        success = 'OK'
        scenario.delete_resourcescenario(scenario_id,
                                         resource_scenario,
                                         **ctx.in_header.__dict__)
        return success

    @rpc(Integer, _returns=Dataset)
    def get_dataset(ctx, dataset_id):
        """
            Get a single dataset, by ID
        """
        dataset_i = scenario.get_dataset(dataset_id, **ctx.in_header.__dict__)
        return Dataset(dataset_i)

    @rpc(Integer, Integer, Dataset, _returns=ResourceScenario)
    def add_data_to_attribute(ctx, scenario_id, resource_attr_id, dataset):
        """
                Add data to a resource scenario outside of a network update
        """
        new_data = scenario.add_data_to_attribute(scenario_id,
                                                  resource_attr_id,
                                                  dataset,
                                                  **ctx.in_header.__dict__)
        x = Dataset(new_data)
        return x

    @rpc(Integer, _returns=SpyneArray(Dataset))
    def get_scenario_data(ctx, scenario_id):
        scenario_data = scenario.get_scenario_data(scenario_id,
                                                   **ctx.in_header.__dict__)
        data_cm = [Dataset(d) for d in scenario_data]
        return data_cm

    @rpc(Integer, Integer, Integer, _returns=SpyneArray(ResourceScenario))
    def get_node_data(ctx, node_id, scenario_id, type_id):
        """
            Get all the resource scenarios for a given node 
            in a given scenario. If type_id is specified, only
            return the resource scenarios for the attributes
            within the type.
        """
        node_data = scenario.get_resource_data('NODE',
                                               node_id,
                                               scenario_id,
                                               type_id,
                                               **ctx.in_header.__dict__
                                              )
        
        ret_data = [ResourceScenario(rs) for rs in node_data]
        return ret_data 

    @rpc(Integer, Integer, Integer, _returns=SpyneArray(ResourceScenario))
    def get_link_data(ctx, link_id, scenario_id, type_id):
        """
            Get all the resource scenarios for a given link 
            in a given scenario. If type_id is specified, only
            return the resource scenarios for the attributes
            within the type.
        """
        link_data = scenario.get_resource_data('LINK',
                                               link_id,
                                               scenario_id,
                                               type_id,
                                               **ctx.in_header.__dict__
        )
        
        ret_data = [ResourceScenario(rs) for rs in link_data]
        return ret_data

    @rpc(Integer, Integer, Integer, _returns=SpyneArray(ResourceScenario))
    def get_network_data(ctx, network_id, scenario_id, type_id):
        """
            Get all the resource scenarios for a given network 
            in a given scenario. If type_id is specified, only
            return the resource scenarios for the attributes
            within the type.
        """
        network_data = scenario.get_resource_data('NETWORK',
                                               network_id,
                                               scenario_id,
                                               type_id,
                                                **ctx.in_header.__dict__)
        
        ret_data = [ResourceScenario(rs) for rs in network_data]
        return ret_data

    @rpc(Integer, Integer, Integer, _returns=SpyneArray(ResourceScenario))
    def get_resourcegroup_data(ctx, resourcegroup_id, scenario_id, type_id):
        """
            Get all the resource scenarios for a given resourcegroup 
            in a given scenario. If type_id is specified, only
            return the resource scenarios for the attributes
            within the type.
        """
        group_data = scenario.get_resource_data('GROUP',
                                               resourcegroup_id,
                                               scenario_id,
                                               type_id,
                                               **ctx.in_header.__dict__)
        
        ret_data = [ResourceScenario(rs) for rs in group_data]
        return ret_data
