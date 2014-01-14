from HydraLib.HydraException import HydraError
from spyne.model.primitive import String, Integer
from spyne.decorator import rpc
from db import HydraIface
from hydra_base import HydraService


class SharingService(HydraService):
    """
        The network SOAP service.
    """

    @rpc(Integer, String(max_occurs='unbounded'), 
         String(pattern="[YN]"), String(pattern="[YN]"), _returns=String)
    def share_network(ctx, network_id, usernames, read_only, share='N'):
        """
            Share a network with a list of users, identified by their usernames.
            
            The read_only flag ('Y' or 'N') must be set
            to 'Y' to allow write access or sharing.

            The share flat ('Y' or 'N') must be set to 'Y' to allow the 
            project to be shared with other users 
        """

        net_i = HydraIface.Network(network_id=network_id)
       
        #Check if the user is allowed to share this network.
        net_i.check_share_permission(ctx.in_header.user_id)
       
        if read_only == 'Y':
            write = 'N'
            share = 'N'
        else:
            write = 'Y'

        if net_i.db.created_by != int(ctx.in_header.user_id) and share == 'Y':
            raise HydraError("Cannot share the 'sharing' ability as user %s is not"
                         " the owner of network %s"%
                         (ctx.in_header.user_id, network_id))

        for username in usernames:
            user_i = HydraIface.User()
            user_i.db.username = username
            user_i.get_user_id()
            net_i.set_ownership(user_i.db.user_id, write=write, share=share)
    
            net_i.project.set_ownership(user_i.db.user_id, write='N', share='N')

        return "OK"

    @rpc(Integer, String(max_occurs='unbounded'),
         String(pattern="[YN]"), String(pattern="[YN]"), _returns=String)
    def share_project(ctx, project_id, usernames, read_only, share):
        """
            Share an entire project with a list of users, identifed by 
            their usernames. 
            
            The read_only flag ('Y' or 'N') must be set
            to 'Y' to allow write access or sharing.

            The share flat ('Y' or 'N') must be set to 'Y' to allow the 
            project to be shared with other users
        """
        proj_i = HydraIface.Project(project_id=project_id)
        proj_i.load_all()

        #Is the sharing user allowed to share this project?
        proj_i.check_share_permission(ctx.in_header.user_id)
       
        user_id = int(ctx.in_header.user_id)

        if user_id not in proj_i.get_owners():
           raise HydraError("Permission Denied. Cannot share project.")
     
        if read_only == 'Y':
            write = 'N'
            share = 'N'
        else:
            write = 'Y'

        if proj_i.db.created_by != ctx.in_header.user_id and share == 'Y':
            raise HydraError("Cannot share the 'sharing' ability as user %s is not"
                         " the owner of project %s"%
                         (ctx.in_header.user_id, project_id))

        for username in usernames:
            user_i = HydraIface.User()
            user_i.db.username = username
            user_i.get_user_id()
            proj_i.set_ownership(user_i.db.user_id, write=write, share=share)
            
            for net_i in proj_i.networks:
                net_i.set_ownership(user_i.db.user_id, write=write, share=share)

        return "OK"

    @rpc(Integer, String(max_occurs="unbounded"), 
         String(pattern="[YN]"), String(pattern="[YN]"), String(pattern="[YN]"),
         _returns=String)
    def set_project_permission(ctx, project_id, usernames, read, write, share):
        """
            Set permissions on a project to a list of users, identifed by 
            their usernames. 
            
            The read flag ('Y' or 'N') sets read access, the write
            flag sets write access. If the read flag is 'N', then there is
            automatically no write access or share access.
        """
        proj_i = HydraIface.Project(project_id=project_id)
       
        #Is the sharing user allowed to share this project?
        proj_i.check_share_permission(ctx.in_header.user_id)
       
        #You cannot edit something you cannot see.
        if read == 'N':
            write = 'N'
            share = 'N'

        for username in usernames:
            user_i = HydraIface.User()
            user_i.db.username = username
            user_i.get_user_id()

            #The creator of a project must always have read and write access
            #to their project
            if proj_i.db.created_by == user_i.db.user_id:
                raise HydraError("Cannot set permissions on project %s"
                                 " for user %s as tis user is the creator." % 
                                 (project_id, username)) 
            
            proj_i.set_ownership(user_i.db.user_id, read=read, write=write)
            
            for net_i in proj_i.networks:
                net_i.set_ownership(user_i.db.user_id, read=read, write=write, share=share)

        return "OK"

    @rpc(Integer, String(max_occurs="unbounded"), 
         String(pattern="[YN]"), String(pattern="[YN]"), String(pattern="[YN]"),
         _returns=String)
    def set_network_permission(ctx, network_id, usernames, read, write, share):
        """
            Set permissions on a network to a list of users, identifed by 
            their usernames. The read flag ('Y' or 'N') sets read access, the write
            flag sets write access. If the read flag is 'N', then there is
            automatically no write access or share access.
        """

        net_i = HydraIface.Network(network_id=network_id)

        #Check if the user is allowed to share this network.
        net_i.check_share_permission(ctx.in_header.user_id)

        #You cannot edit something you cannot see.
        if read == 'N':
            write = 'N'
            share = 'N'
       
        for username in usernames:
            user_i = HydraIface.User()
            user_i.db.username = username
            user_i.get_user_id()
            #The creator of a project must always have read and write access
            #to their project
            if net_i.db.created_by == user_i.db.user_id:
                raise HydraError("Cannot set permissions on network %s"
                                 " for user %s as tis user is the creator." % 
                                 (network_id, username))
            
            net_i.set_ownership(user_i.db.user_id, read=read, write=write, share=share)

        return "OK"
