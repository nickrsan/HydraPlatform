from spyne.model.primitive import Mandatory, String
from spyne.error import Fault
from spyne.model.complex import ComplexModel
from spyne.decorator import srpc, rpc
from db import HydraIface
from hydra_complexmodels import LoginResponse
import pytz

import datetime

import bcrypt
import random

from spyne.service import ServiceBase

_session_db = set()
_user_map   = {}

def get_session_db():
    return _session_db

def get_user_id(username):
    return _user_map.get(username)

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

    def __init__(self, message):

        Fault.__init__(self,
                faultcode='HydraError',
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
        user_i = HydraIface.User()
        user_i.db.username = username
        if _user_map.get(username) is None:
            user_id = user_i.get_user_id()
            if user_id is None:
                raise AuthenticationError(username)
            _user_map[username] = user_id
        else:
            user_id = _user_map[username]

        user_i.db.user_id = user_id
        user_i.load()

        if bcrypt.hashpw(password, user_i.db.password) == user_i.db.password:
            session_id = (username, '%x' % random.randint(1<<124, (1<<128)-1))
            _session_db.add(session_id)
        else:
           raise AuthenticationError(username)

        user_i.db.last_login = datetime.datetime.now()
        user_i.save()

        loginresponse = LoginResponse()
        loginresponse.session_id = session_id[1]
        loginresponse.user_id    = user_i.db.user_id

        return loginresponse 


