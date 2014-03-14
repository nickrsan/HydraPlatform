#!/usr/local/bin/python
#
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

import sys
import spyne.service #Needed for build script.
#if "./python" not in sys.path:
#    sys.path.append("./python")
#if "../../HydraLib/trunk/" not in sys.path:
#    sys.path.append("../../HydraLib/trunk/")

import logging
from decimal import getcontext
getcontext().prec = 26

from spyne.application import Application
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication

import spyne.decorator

from spyne.error import Fault, ArgumentError

from soap_server.network import NetworkService
from soap_server.project import ProjectService
from soap_server.attributes import AttributeService
from soap_server.scenario import ScenarioService, DataService
from soap_server.plugins import PluginService
from soap_server.users import UserService
from soap_server.template import TemplateService
from soap_server.constraints import ConstraintService
from soap_server.static import ImageService, FileService
from soap_server.groups import ResourceGroupService
from soap_server.units import UnitService
from soap_server.hydra_base import AuthenticationService,\
    LogoutService,\
    get_session_db,\
    make_root_user,\
    AuthenticationError,\
    ObjectNotFoundError,\
    HydraServiceError
from soap_server.sharing import SharingService, DataSharingService

from HydraLib.HydraException import HydraError

from HydraLib import config
from db import hdb
from db import HydraIface

import datetime
import traceback


def _on_method_call(ctx):

    if ctx.function == AuthenticationService.login:
        return

    if ctx.in_object is None:
        raise ArgumentError("RequestHeader is null")
    if ctx.in_header is None:
        raise AuthenticationError("No headers!")
    if not (ctx.in_header.username, ctx.in_header.session_id) in get_session_db():
        raise AuthenticationError(ctx.in_object.username)

def _on_method_context_closed(ctx):
    hdb.commit()

class HydraSoapApplication(Application):
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

            logging.info("Received request: %s", ctx.function)

            start = datetime.datetime.now()
            res =  ctx.service_class.call_wrapper(ctx)

            logging.info("Call took: %s"%(datetime.datetime.now()-start))
            return res
        except HydraError, e:
            hdb.rollback()
            logging.critical(e)
            traceback.print_exc(file=sys.stdout)
            raise HydraServiceError(e.message)
        except ObjectNotFoundError, e:
            hdb.rollback()
            logging.critical(e)
            raise
        except Fault, e:
            hdb.rollback()
            logging.critical(e)
            raise
        except Exception, e:
            logging.critical(e)
            traceback.print_exc(file=sys.stdout)
            hdb.rollback()
            raise Fault('Server', e.message)

class HydraServer():

    def __init__(self):

        logging.getLogger('spyne').setLevel(logging.INFO)
        connection = hdb.connect()
        HydraIface.init(connection)

        make_root_user()

    def create_application(self):

        applications = [
            AuthenticationService,
            UserService,
            LogoutService,
            NetworkService,
            ProjectService,
            ResourceGroupService,
            AttributeService,
            ScenarioService,
            DataService,
            PluginService,
            ConstraintService,
            TemplateService,
            ImageService,
            FileService,
            SharingService,
            UnitService,
            DataSharingService,
        ]

        application = HydraSoapApplication(applications, 'hydra.base',
                    in_protocol=Soap11(validator='lxml'),
                    out_protocol=Soap11()
                )
        wsgi_application = WsgiApplication(application)
        wsgi_application.max_content_length = 100 * 0x100000 # 10 MB

        return wsgi_application

    def run_server(self):

        wsgi_application = self.create_application()

        from wsgiref.simple_server import make_server

        port = config.getint('hydra_server', 'port')
        spyne.const.xml_ns.DEFAULT_NS = 'soap_server.hydra_complexmodels'

        logging.info("listening to http://127.0.0.1:%s", port)
        logging.info("wsdl is at: http://localhost:%s/?wsdl", port)

        server = make_server('127.0.0.1', port, wsgi_application)
        server.serve_forever()

# These few lines are needed to turn the server into a WSGI script.
server = HydraServer()
application = server.create_application()


if __name__ == '__main__':
    server = HydraServer()
    server.run_server()
