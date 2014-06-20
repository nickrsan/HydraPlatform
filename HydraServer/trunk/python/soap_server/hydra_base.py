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
from HydraLib.HydraException import HydraError
from db.HydraAlchemy import User, Role, RoleUser
from db import DBSession
from sqlalchemy.orm.exc import NoResultFound
import transaction
import datetime

import bcrypt
import random

from spyne.service import ServiceBase

log = logging.getLogger(__name__)
_session_db = set()

def get_session_db():
    return _session_db

def make_root_user():

    try:
        user = DBSession.query(User).filter(User.username=='root').one()
    except NoResultFound:
        user = User(username='root',
                    password=bcrypt.hashpw('', bcrypt.gensalt()),
                    display_name='Root User')
        DBSession.add(user)

    try:
        role = DBSession.query(Role).filter(Role.role_code=='admin').one()
    except NoResultFound:
        raise HydraError("Admin role not found.")

    try:
        userrole = DBSession.query(RoleUser).filter(RoleUser.role_id==role.role_id,
                                                   RoleUser.user_id==user.user_id).one()
    except NoResultFound:
        userrole = RoleUser(role_id=role.role_id,user_id=user.user_id) 
        user.roleusers.append(userrole)
        DBSession.add(userrole)
    DBSession.flush()
    transaction.commit()
    
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
            user_i = DBSession.query(User).filter(User.username==username).one()
        except NoResultFound:
           raise AuthenticationError(username)

        if bcrypt.hashpw(password, user_i.password) == user_i.password:
            session_id = (username, '%x' % random.randint(1<<124, (1<<128)-1))
            _session_db.add(session_id)
        else:
           raise AuthenticationError(username)

        user_i.last_login = datetime.datetime.now()

        loginresponse = LoginResponse()
        loginresponse.session_id = session_id[1]
        loginresponse.user_id    = user_i.user_id

        DBSession.flush()
        
        return loginresponse 


