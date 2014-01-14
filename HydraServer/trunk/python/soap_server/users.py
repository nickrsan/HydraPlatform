from spyne.model.primitive import Integer, String
from spyne.decorator import rpc
from db import HydraIface
from hydra_complexmodels import User,\
        Role,\
        Perm

import bcrypt

from hydra_base import HydraService
from HydraLib.HydraException import HydraError

class UserService(HydraService):
    """
        The user soap service
    """

    @rpc(User, _returns=User)
    def add_user(ctx, user):
        """
            Add a new user.
        """
        user_i = HydraIface.User()
        
        user_i.db.username = user.username
        
        user_id = user_i.get_user_id()

        #If the user is already there, cannot add another with 
        #the same username.
        if user_id is not None:
            raise HydraError("User %s already exists!"%user.username)

        user_i.db.password = bcrypt.hashpw(user.password, bcrypt.gensalt())
        
        user_i.save()

        return user_i.get_as_complexmodel()

    @rpc(String, _returns=User)
    def get_user_by_name(ctx, username):
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
            return user_i.get_as_complexmodel()
        
        return None

    @rpc(User, _returns=String)
    def delete_user(ctx, user):
        """
            Add a new user.
        """
        user_i = HydraIface.User(user_id=user.id)
        
        #If the user is already there, cannot add another with 
        #the same username.
        if user_i.load() is False:
            raise HydraError("User %s is not in the system."%user.username)

        user_i.delete()

        return 'OK'


    @rpc(Role, _returns=Role)
    def add_role(ctx, role):
        """
            Add a new role.
        """
        role_i = HydraIface.Role()
        role_i.db.role_name = role.name
        role_i.db.role_code = role.code

        role_i.save()

        return role_i.get_as_complexmodel()

    @rpc(Role, _returns=String)
    def delete_role(ctx, role):
        """
            Add a new role.
        """
        role_i = HydraIface.Role(role_id=role.id)

        role_i.delete()

        return 'OK'

    @rpc(Perm, _returns=Perm)
    def add_perm(ctx, perm):
        """
            Add a new permission
        """
        perm_i = HydraIface.Perm()
        perm_i.db.perm_name = perm.name
        perm_i.db.perm_code = perm.code

        perm_i.save()

        return perm_i.get_as_complexmodel()

    @rpc(Perm, _returns=String)
    def delete_perm(ctx, perm):
        """
            Add a new permission
        """
        perm_i = HydraIface.Perm(perm_id=perm.id)

        perm_i.delete()

        return 'OK' 

    @rpc(User, Role, _returns=Role)
    def set_user_role(ctx, user, role):
        roleuser_i = HydraIface.RoleUser(user_id=user.id, role_id=role.id)
        
        roleuser_i.save()
        roleuser_i.role.load_all()

        return roleuser_i.role.get_as_complexmodel()

    @rpc(User, Role, _returns=String)
    def delete_user_role(ctx, user, role):
        roleuser_i = HydraIface.RoleUser(user_id=user.id, role_id=role.id)
        
        roleuser_i.delete()

        return 'OK'

    @rpc(Role, Perm, _returns=Role)
    def set_role_perm(ctx, role, perm):
        roleperm_i = HydraIface.RolePerm(role_id=role.id, perm_id=perm.id)

        roleperm_i.save()

        roleperm_i.role.load_all()

        return roleperm_i.role.get_as_complexmodel()

    @rpc(Role, Perm, _returns=String)
    def delete_role_perm(ctx, role, perm):
        roleperm_i = HydraIface.RolePerm(role_id=role.id, perm_id=perm.id)
        
        roleperm_i.delete()

        return 'OK'


    @rpc(Role, _returns=Role)
    def update_role(ctx, role):
        """
            Update the role.
            Used to add permissions and users to a role.
        """
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

        return role_i.get_as_complexmodel()

            
