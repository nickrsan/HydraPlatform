from spyne.service import ServiceBase
import logging
from HydraLib.HydraException import HydraError
from spyne.model.primitive import Integer, Boolean
from spyne.decorator import rpc
from hydra_complexmodels import Attr
from db import HydraIface

class AttributeService(ServiceBase):
    @rpc(Attr, _returns=Attr)
    def add_attribute(ctx, attr):
        x = HydraIface.Attr()
        x.db.attr_name = attr.attr_name
        x.db.attr_dimen = attr.attr_dimen
        x.save()
        return x.get_as_complexmodel()

    @rpc(Integer, _returns=Boolean)
    def delete_attribute(ctx, attr_id):
        success = True
        try:
            x = HydraIface.Attr(attr_id = attr_id)
            x.db.status = 'X'
            x.save()
        except HydraError, e:
            logging.critical(e)
            success = False
        return success
        
