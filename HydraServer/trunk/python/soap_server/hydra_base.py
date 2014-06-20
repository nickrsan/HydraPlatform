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
from spyne.model.primitive import Mandatory, String
from spyne.error import Fault
from spyne.model.complex import ComplexModel
from spyne.decorator import srpc, rpc
from hydra_complexmodels import LoginResponse
import logging
from db.util import login_user
from HydraLib.HydraException import HydraError


from spyne.service import ServiceBase

log = logging.getLogger(__name__)
_session_db = set()

def get_session_db():
    return _session_db

class RequestHeader(ComplexModel):
    __namespace__ = 'hydra.base'
    session_id    = Mandatory.String
    username      = Mandatory.String
    user_id       = Mandatory.String

class HydraService(ServiceBase):
    __tns__ = 'hydra.base'
    __in_header__ = RequestHeader

class AuthenticationError(Fault):
    __namespace__ = 'hydra.base'

    def __init__(self, user_name):
        Fault.__init__(self,
                faultcode='Client.AuthenticationError',
                faultstring='Invalid authentication request for %r' % user_name
            )

class AuthorizationError(Fault):
    __namespace__ = 'hydra.authentication'

    def __init__(self):

        Fault.__init__(self,
                faultcode='Client.AuthorizationError',
                faultstring='You are not authozied to access this resource.'
            )

class HydraServiceError(Fault):
    __namespace__ = 'hydra.error'

    def __init__(self, message, code="HydraError"):

        Fault.__init__(self,
                faultcode=code,
                faultstring=message
        )

class ObjectNotFoundError(HydraServiceError):
    __namespace__ = 'hydra.error'

    def __init__(self, message):

        Fault.__init__(self,
                faultcode='NoObjectFoundError',
                faultstring=message
        )

class LogoutService(HydraService):
    __tns__      = 'hydra.authentication'
    
    @rpc(Mandatory.String, Mandatory.String, _returns=String,
                                                    _throws=AuthenticationError)
    def logout(ctx, username):
        _session_db.remove((ctx.in_header.username, ctx.in_header.session_id))
        return "OK"

class AuthenticationService(ServiceBase):
    __tns__      = 'hydra.base'

    @srpc(Mandatory.String, String, _returns=LoginResponse,
                                                   _throws=AuthenticationError)
    def login(username, password):
        try:
            user_id, session_info = login_user(username, password)
        except HydraError, e:
            raise AuthenticationError(e)
       
        _session_db.add(session_info)
        loginresponse = LoginResponse()
        loginresponse.session_id = session_info[1]
        loginresponse.user_id    = user_id

        return loginresponse 


