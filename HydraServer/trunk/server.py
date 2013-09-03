import sys
if "./python" not in sys.path:
    sys.path.append("./python")
if "../../HydraLib/trunk/" not in sys.path:
    sys.path.append("../../HydraLib/trunk/")

import logging

from spyne.application import Application
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication

from spyne.error import InternalError,\
                        RequestNotAllowed,\
                        Fault

from soap_server.network import NetworkService
from soap_server.project import ProjectService
from soap_server.attributes import AttributeService
from soap_server.scenario import ScenarioService, DataService
from soap_server.plugins import PluginService

from HydraLib import hydra_logging, hdb, util
from db import HydraIface

import datetime
import sys, traceback


def _on_method_call(ctx):
    #Open a cursor?
    pass


def _on_method_context_closed(ctx):
    pass
    #hdb.commit()


class MyApplication(Application):
    """
        Subclass of the base spyne Application class.

        Used to handle transactions in request handlers and to log
        how long each request takes.

        Used also to handle exceptions, allowing server side exceptions
        to be send to the client.
    """
    def __init__(self, services, tns, name=None,
                                         in_protocol=None, out_protocol=None):
        Application.__init__(self, services, tns, name, in_protocol,
                                                                 out_protocol)

        self.event_manager.add_listener('method_call', _on_method_call)
        self.event_manager.add_listener("method_context_closed",
                                                    _on_method_context_closed)

    def call_wrapper(self, ctx):
        try:
            start = datetime.datetime.now()
            res =  ctx.service_class.call_wrapper(ctx)
            logging.info("Call took: %s"%(datetime.datetime.now()-start))
            return res
        except Fault, e:
            logging.error(e)
            raise

        except Exception, e:
            logging.critical(e)
            traceback.print_exc(file=sys.stdout)
            hdb.rollback()
            raise Fault('Server', e.message)

#if __name__=='__main__':


class HydraServer():

    def crate_application(self):

        hydra_logging.init(level='INFO')
        #logging.getLogger('spyne.protocol.xml').setLevel(logging.DEBUG)
        connection = hdb.connect()
        HydraIface.init(connection)

        applications = [
            NetworkService,
            ProjectService,
            AttributeService,
            ScenarioService,
            DataService,
            PluginService,
        ]

        application = MyApplication(applications, 'hydra.soap',
                    in_protocol=Soap11(validator='lxml'),
                    out_protocol=Soap11()
                )
        wsgi_application = WsgiApplication(application)
        return wsgi_application

    def run_server(self):
        wsgi_application = self.crate_application()

        from wsgiref.simple_server import make_server

        config = util.load_config()
        port = config.getint('hydra_server', 'port')

        logging.info("listening to http://127.0.0.1:%s", port)
        logging.info("wsdl is at: http://localhost:%s/?wsdl", port)

        server = make_server('127.0.0.1', port, wsgi_application)
        server.serve_forever()

# These few lines are needed to turn the server into a WSGI script.
server = HydraServer()
application = server.crate_application()


if __name__ == '__main__':
    server = HydraServer()
    server.run_server()
