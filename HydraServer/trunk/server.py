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
from spyne.protocol.json import JsonDocument

import spyne.decorator

from spyne.error import Fault, ArgumentError

from db.util import make_root_user
import db.model

from soap_server.network import NetworkService
from soap_server.project import ProjectService
from soap_server.attributes import AttributeService
from soap_server.scenario import ScenarioService
from soap_server.data import DataService
from soap_server.plugins import PluginService
from soap_server.users import UserService
from soap_server.template import TemplateService
from soap_server.static import ImageService, FileService
from soap_server.groups import ResourceGroupService
from soap_server.units import UnitService
from soap_server.hydra_base import AuthenticationService,\
    LogoutService,\
    get_session_db,\
    AuthenticationError,\
    ObjectNotFoundError,\
    HydraServiceError,\
    HydraDocument
from soap_server.sharing import SharingService
from spyne.util.wsgi_wrapper import WsgiMounter

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
    TemplateService,
    ImageService,
    FileService,
    SharingService,
    UnitService,
]

from HydraLib.HydraException import HydraError

from HydraLib import config
from db import util

import datetime
import traceback

from cherrypy.wsgiserver import CherryPyWSGIServer
from db import commit_transaction 

log = logging.getLogger(__name__)

def _on_method_call(ctx):
    
    if ctx.function == AuthenticationService.login:
        return
    
    if ctx.in_object is None:
        raise ArgumentError("RequestHeader is null")
    if ctx.in_header is None:
        raise AuthenticationError("No headers!")
    session_db = get_session_db()
    sess_info  = session_db.get(ctx.in_header.session_id)
    if sess_info is None:
        raise Fault("No Session")

    ctx.in_header.user_id  = sess_info[0]
    ctx.in_header.username = sess_info[1]

def _on_method_context_closed(ctx):
    commit_transaction()

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

            log.info("Received request: %s", ctx.function)

            start = datetime.datetime.now()
            res =  ctx.service_class.call_wrapper(ctx)

            log.info("Call took: %s"%(datetime.datetime.now()-start))
            return res
        except HydraError, e:
            log.critical(e)
            traceback.print_exc(file=sys.stdout)
            code = "HydraError %s"%e.code
            raise HydraServiceError(e.message, code)
        except ObjectNotFoundError, e:
            log.critical(e)
            raise
        except Fault, e:
            log.critical(e)
            raise
        except Exception, e:
            log.critical(e)
            traceback.print_exc(file=sys.stdout)
            raise Fault('Server', e.message)

class HydraServer():

    def __init__(self):

        logging.getLogger('spyne').setLevel(logging.INFO)
#        logging.getLogger('sqlalchemy').setLevel(logging.INFO)
        util.create_default_users_and_perms()
        util.create_default_net()
        make_root_user()

    def create_soap_application(self):

        app = HydraSoapApplication(applications, tns='hydra.base',
                    in_protocol=Soap11(validator='lxml'),
                    out_protocol=Soap11()
                )
        return app

    def create_json_application(self):

        app = HydraSoapApplication(applications, tns='hydra.base',
                    in_protocol=HydraDocument(validator='soft'),
                    out_protocol=JsonDocument()
                )
        return app

    def run_server(self):
        
        logging.info("home_dir %s",config.get('DEFAULT', 'home_dir'))
        logging.info("hydra_base_dir %s",config.get('DEFAULT', 'hydra_base_dir'))
        logging.info("common_app_data_folder %s",config.get('DEFAULT', 'common_app_data_folder'))
        logging.info("win_common_documents %s",config.get('DEFAULT', 'win_common_documents'))
        logging.info("sqlite url %s",config.get('mysqld', 'url'))
        logging.info("layout_xsd_path %s",config.get('hydra_server', 'layout_xsd_path'))
        logging.info("default_directory %s",config.get('plugin', 'default_directory'))
        logging.info("result_file %s",config.get('plugin', 'result_file'))
        logging.info("plugin_xsd_path %s",config.get('plugin', 'plugin_xsd_path'))
        logging.info("log_config_path %s",config.get('logging_conf', 'log_config_path'))
        


        port = config.getint('hydra_server', 'port', 12345)
        domain = config.get('hydra_server', 'domain', '127.0.0.1')
        
        spyne.const.xml_ns.DEFAULT_NS = 'soap_server.hydra_complexmodels'
        cp_wsgi_application = CherryPyWSGIServer((domain,port), root, numthreads=1)

        log.info("listening to http://%s:%s", domain, port)
        log.info("wsdl is at: http://%s:%s/?wsdl", domain, port)
        try:
            cp_wsgi_application.start()
        except KeyboardInterrupt:
            cp_wsgi_application.stop()

# These few lines are needed by mod_wsgi to turn the server into a WSGI script.
s = HydraServer()
soap_application = s.create_soap_application()
json_application = s.create_json_application()

root = WsgiMounter({
    'soap': soap_application,
    'json': json_application,
})

for server in root.mounts.values():
    server.max_content_length = 100 * 0x100000 # 10 MB

#To kill this process, use this command:
#ps -ef | grep 'server.py' | grep 'python' | awk '{print $2}' | xargs kill
if __name__ == '__main__':
    s.run_server()
