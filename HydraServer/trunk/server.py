import sys
if "./python" not in sys.path:
    sys.path.append("./python")
if "../../HydraLib/trunk/" not in sys.path:
    sys.path.append("../../HydraLib/trunk/")

import logging

from spyne.application import Application
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication

from spyne.decorator import srpc
from spyne.service import ServiceBase
from spyne.model.complex import Iterable
from spyne.model.primitive import Integer, AnyDict
from spyne.model.primitive import Unicode

from soap_server.network import NetworkService
from soap_server.project import ProjectService
from soap_server.attributes import AttributeService
from soap_server.scenario import ScenarioService, DataService

from HydraLib import hydra_logging, hdb, util
from db import HydraIface

class HelloWorldService(ServiceBase):
    @srpc(Unicode, Integer, _returns=Iterable(Unicode))
    def say_hello(name, times):
        for i in range(times):
            yield u'Hello, %s' % name


    @srpc(_returns=AnyDict)
    def testf():
        return {"a": "1", "b": "2"}


if __name__=='__main__':


    hydra_logging.init(level='INFO')
    #logging.getLogger('spyne.protocol.xml').setLevel(logging.DEBUG)
    connection = hdb.connect()
    HydraIface.init(connection)

    from wsgiref.simple_server import make_server

    #logging.basicConfig(level=logging.DEBUG)
    #logging.getLogger('spyne.protocol.xml').setLevel(logging.DEBUG)

    logging.info("listening to http://127.0.0.1:8000")
    logging.info("wsdl is at: http://localhost:8000/?wsdl")

    applications = [
        HelloWorldService,
        NetworkService,
        ProjectService,
        AttributeService,
        ScenarioService,
        DataService,
    ]

    application = Application(applications, 'hydra.soap',
                in_protocol=Soap11(validator='lxml'),
                out_protocol=Soap11()
            )
    wsgi_application = WsgiApplication(application)

    config = util.load_config()
    port = config.getint('soap_server', 'port') 

    server = make_server('127.0.0.1', port, wsgi_application)
    server.serve_forever()
