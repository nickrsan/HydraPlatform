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
from db import HydraIface
import bcrypt

from HydraLib.HydraException import HydraError

def add_user(user_id, user):
    """
        Add a new user.
    """
    #check_perm(user_id, 'add_user')
    user_i = HydraIface.User()
    
    user_i.db.username     = user.username
    user_i.db.display_name = user.display_name
    
    user_id = user_i.get_user_id()

    #If the user is already there, cannot add another with 
    #the same username.
    if user_id is not None:
        raise HydraError("User %s already exists!"%user.username)

    user_i.db.password = bcrypt.hashpw(user.password, bcrypt.gensalt())
    
    user_i.save()

    return user_i

def update_user_display_name(user_id, user):
    """
        Add a new user.
    """
    #check_perm(user_id, 'edit_user')
    user_i = HydraIface.User(user_id=user.id)

    user_i.db.display_name = user.display_name

    user_i.save()

    return user_i


def update_user_password(user_id, new_pwd_user_id, new_password):
    """
        Add a new user.
    """
    #check_perm(user_id, 'edit_user')
    user_i = HydraIface.User(user_id=new_pwd_user_id)

    user_i.db.password = bcrypt.hashpw(new_password, bcrypt.gensalt())

    user_i.save()

    return user_i

def get_user_by_name(user_id, username):
    """
        Add a new user.
    """
    user_i = HydraIface.User()
    
    user_i.db.username = username
    
    user_id = user_i.get_user_id()

    #If the user is already there, cannot add another with 
    #the same username.
    if user_id is not None:
        user_i.load_all()
        return user_i
    
    return None

def delete_user(user_id, deleted_user_id):
    """
        Add a new user.
    """
    #check_perm(user_id, 'edit_user')
    user_i = HydraIface.User(user_id=deleted_user_id)
    
    #If the user is already there, cannot add another with 
    #the same username.
    if user_i.load() is False:
        raise HydraError("User %s is not in the system."%user_id)

    user_i.delete()

    return 'OK'


def add_role(user_id, role):
    """
        Add a new role.
    """
    #check_perm(user_id, 'add_role')
    role_i = HydraIface.Role()
    role_i.db.role_name = role.name
    role_i.db.role_code = role.code

    role_i.save()

    return role_i

def delete_role(user_id, role_id):
    """
        Add a new role.
    """
    #check_perm(user_id, 'edit_role')
    role_i = HydraIface.Role(role_id=role_id)

    role_i.delete()

    return 'OK'

def add_perm(user_id, perm):
    """
        Add a new permission
    """
    #check_perm(user_id, 'add_perm')
    perm_i = HydraIface.Perm()
    perm_i.db.perm_name = perm.name
    perm_i.db.perm_code = perm.code

    perm_i.save()

    return perm_i

def delete_perm(user_id, perm_id):
    """
        Add a new permission
    """

    #check_perm(user_id, 'edit_perm')
    perm_i = HydraIface.Perm(perm_id=perm_id)

    perm_i.delete()

    return 'OK' 

def set_user_role(user_id, new_user_id, role_id):
    #check_perm(user_id, 'edit_role')
    roleuser_i = HydraIface.RoleUser(user_id=new_user_id, role_id=role_id)
    
    roleuser_i.save()
    roleuser_i.role.load_all()

    return roleuser_i.role

def delete_user_role(user_id, deleted_user_id, role_id):

    #check_perm(user_id, 'edit_role')
    roleuser_i = HydraIface.RoleUser(user_id=deleted_user_id, role_id=role_id)
    
    roleuser_i.delete()

    return 'OK'

def set_role_perm(user_id, role_id, perm_id):
    #check_perm(user_id, 'edit_perm')
    roleperm_i = HydraIface.RolePerm(role_id=role_id, perm_id=perm_id)

    roleperm_i.save()

    roleperm_i.role.load_all()

    return roleperm_i.role

def delete_role_perm(user_id, role_id, perm_id):
    #check_perm(user_id, 'edit_perm')
    roleperm_i = HydraIface.RolePerm(role_id=role_id, perm_id=perm_id)
    
    roleperm_i.delete()

    return 'OK'


def update_role(user_id, role):
    """
        Update the role.
        Used to add permissions and users to a role.
    """
    #check_perm(user_id, 'edit_role')
    role_i = HydraIface.Role(role_id=role.id)
    role_i.db.role_name = role.name
    role_i.db.role_code = role.code

    for perm in role.permissions:
        if hasattr('id', perm) and perm.id is not None:
            perm_i = HydraIface.Perm(perm_id=perm.id)

        perm_i = HydraIface.Perm()
        perm_i.db.perm_name = perm.name

        perm_i.save()

        roleperm_i = HydraIface.RolePerm(
                                         role_id=role_i.db.role_id, 
                                         perm_id=perm_i.db.perm_id
                                        )

        roleperm_i.save()

    for user in role.users:
        if hasattr('id', user) and user.id is not None:
            user_i = HydraIface.User(user_id=user.id)

        user_i = HydraIface.User()
        user_i.db.username = user.username

        user_i.save()

        roleuser_i = HydraIface.RoleUser(user_id=user_i.db.user_id,
                                         perm_id=perm_i.db.perm_id
                                        )

        roleuser_i.save()

    return role_i

        
def get_all_users(ctx):
    """
        Get the username & ID of all users.
    """

    sql = """
        select
            username,
            display_name,
            user_id
        from
            tUser
    """

    rs = HydraIface.execute(sql)
    users = []
    for r in rs:
        user = dict(
            username = r.username,
            display_name = r.display_name,
            id  = r.user_id,
        )
        users.append(user)

    return users

def get_all_perms(ctx):
    """
        Get all permissions
    """
    sql = """
        select
            perm_name,
            perm_id,
            perm_code
        from
            tPerm
    """
    rs = HydraIface.execute(sql)
    perms = []
    for r in rs:
        perm = dict(
            name = r.perm_name,
            id   = r.perm_id,
            code = r.perm_code,
        )
        perms.append(perm)
    return perms

def get_all_roles(ctx):
    """
        Get all roleissions
    """
    sql = """
        select
            role_id
        from
            tRole
    """
    rs = HydraIface.execute(sql)
    roles = []
    for r in rs:
        role = HydraIface.Role(role_id=r.role_id)
        role.load_all()
        roles.append(role)
    
    return roles

def get_role(user_id, role_id):
    """
        Get a role by its ID.
    """
    sql = """
        select
            role_id
        from
            tRole
        where
            role_id=%s
    """ % (role_id)

    rs = HydraIface.execute(sql)
    if len(rs) == 0:
        raise HydraError("Role not found (role_id=%s)", role_id)
    
    role = HydraIface.Role(role_id=rs[0].role_id)
    role.load_all()
    
    return role


def get_role_by_code(user_id, role_code):
    """
        Get a role by its code
    """
    sql = """
        select
            role_id
        from
            tRole
        where
            role_code='%s'
    """ % (role_code)

    rs = HydraIface.execute(sql)
    if len(rs) == 0:
        raise HydraError("Role not found (role_code=%s)", role_code)
    
    role = HydraIface.Role(role_id=rs[0].role_id)
    role.load_all()
    
    return role


def get_perm(user_id, perm_id):
    """
        Get all permissions
    """
    sql = """
        select
            perm_id,
            perm_name,
            perm_code
        from
            tPerm
        where
            perm_id=%s
    """ % (perm_id)

    rs = HydraIface.execute(sql)
    if len(rs) == 0:
        raise HydraError("perm not found (perm_id=%s)", perm_id)

    p = dict(
        id = rs[0].perm_id,
        name= rs[0].perm_name,
        code = rs[0].perm_code,
    )

    return p

def get_perm_by_code(user_id, perm_code):
    """
        Get a permission by its code 
    """
    sql = """
        select
            perm_id,
            perm_name,
            perm_code
        from
            tPerm
        where
            perm_code='%s'
    """ % (perm_code)

    rs = HydraIface.execute(sql)
    if len(rs) == 0:
        raise HydraError("perm not found (perm_id=%s)", perm_code)

    p = dict(
        id = rs[0].perm_id,
        name= rs[0].perm_name,
        code = rs[0].perm_code,
    )

    return p
