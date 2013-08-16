from spyne.service import ServiceBase
import logging
from HydraLib.HydraException import HydraError
from spyne.model.primitive import Integer, Boolean
from spyne.decorator import rpc
from hydra_complexmodels import Scenario,\
        Descriptor,\
        TimeSeries,\
        EqTimeSeries,\
        Scalar,\
        Array as HydraArray,\
        parse_value

from db import HydraIface

class ScenarioService(ServiceBase):
   @rpc(Scenario, _returns=Scenario)
   def add_scenario(ctx, scenario):
        x = HydraIface.Scenario()
        x.db.network_id           = scenario.network_id
        x.db.scenario_name        = scenario.scenario_name
        x.db.scenario_description = scenario.scenario_description
        x.save()
        x.commit()
        scenario.scenario_id = x.db.scenario_id

        #scenario.attributes are ScenarioAttr types.
        for attr in scenario.attributes:
            #ra = resource attribute
            ra = x.add_attribute(attr.attr_id, attr.is_val)
            ra.save()
            ra.commit()
            attr.resource_attr_id = ra.db.resource_attr_id
            
            data_type, units, name, dimension, value = parse_value(attr.value)
            x.assign_value(scenario.scenario_id,
                           attr.resource_attr_id,
                           data_type,
                           value,
                           units,
                           name,
                           dimension
                          )
        return scenario 

   @rpc(Integer, _returns=Boolean)
   def delete_scenario(ctx, scenario_id):
        success = True
        try:
            x = HydraIface.Scenario(scenario_id = scenario_id)
            x.db.status = 'X'
            x.save()
        except HydraError, e:
            logging.critical(e)
            success=False

        return success


class DataService(ServiceBase):

    @rpc(Descriptor, _returns=Descriptor)
    def echo_descriptor(x):
        return x

    @rpc(TimeSeries, _returns=TimeSeries)
    def echo_timeseries(x):
        return x

    @rpc(EqTimeSeries, _returns=EqTimeSeries)
    def echo_eqtimeseries(x):
        return x

    @rpc(Scalar, _returns=Scalar)
    def echo_scalar(x):
        return x

    @rpc(HydraArray, _returns=HydraArray)
    def echo_array(x):
        return x