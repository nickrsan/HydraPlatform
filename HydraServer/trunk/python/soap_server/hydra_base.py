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
from db import HydraIface
from hydra_complexmodels import LoginResponse
import pytz
import logging
from HydraLib.HydraException import HydraError

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

def make_root_user():
    user = HydraIface.User()
    user.db.username = 'root'
    user.db.password =  bcrypt.hashpw('', bcrypt.gensalt())
    user.db.display_name = 'Root user'

    if user.get_user_id() is None:
        user.save()
        user.commit()
    else:
        logging.info("Root user exists.")

    sql = """
        select
            role_id
        from
            tRole
        where
            role_code = 'admin'
    """
    rs = HydraIface.execute(sql)
    if len(rs) == 0:
        raise HydraError("Admin role not found.")

    role_id = rs[0].role_id
    userrole = HydraIface.RoleUser(role_id=role_id,user_id=user.db.user_id) 
    if not userrole.load():
        userrole.save()
    userrole.commit()

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


