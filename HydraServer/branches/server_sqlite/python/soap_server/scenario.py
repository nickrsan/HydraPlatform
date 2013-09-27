import logging
from HydraLib.HydraException import HydraError
from spyne.model.primitive import Integer, Boolean, AnyDict
from spyne.decorator import rpc
from hydra_complexmodels import Scenario,\
        Descriptor,\
        TimeSeries,\
        EqTimeSeries,\
        Scalar,\
        Array as HydraArray,\
        ResourceScenario,\
        parse_value

from db import HydraIface
from HydraLib import hdb
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
    
    data_type = resource_scenario.type.lower()
   
    value = parse_value(data_type, resource_scenario)

    res.assign_value(scenario_id, ra_id, data_type, value,
                    "", "", "", new=new)

    return res

class DataService(HydraService):
  
    """
        The data SOAP service
    """

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
