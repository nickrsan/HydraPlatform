from spyne.decorator import rpc
from spyne.model.primitive import Integer, Boolean
from spyne.model.complex import Array as SpyneArray
from db import HydraIface
from hydra_complexmodels import Project,\
ProjectSummary,\
Network,\
get_as_complexmodel
from hydra_base import HydraService, ObjectNotFoundError
from HydraLib.HydraException import HydraError
import scenario
import logging

def _add_project_attributes(resource_i, attributes):
    if attributes is None:
        return []
    #As projects do not have scenarios (or to be more precise, they can only use
    #scenario 1, we can put
    #resource scenarios directly into the 'attributes' attribute
    #meaning we can add the data directly here.

    for attr in attributes:

        if attr.resource_attr_id < 0:
            ra_i = resource_i.add_attribute(attr.attr_id)
            ra_i.save()
            ra_i.load()
            attr.resource_attr_id = ra_i.db.resource_attr_id
    
        scenario._update_resourcescenario(None, attr)

class ProjectService(HydraService):
    """
        The project SOAP service
    """

    @rpc(Project, _returns=Project)
    def add_project(ctx, project):
        """
            Add a new project
            returns a project complexmodel
        """

        proj_i = HydraIface.Project()
        proj_i.db.project_name = project.name
        proj_i.db.project_description = project.description
        proj_i.db.created_by = ctx.in_header.user_id

        proj_i.save()

        _add_project_attributes(proj_i, project.attributes)

        user_id = ctx.in_header.user_id
        proj_i.set_ownership(user_id)

        ret = get_as_complexmodel(ctx, proj_i)

        return ret

    @rpc(Project, _returns=Project)
    def update_project(ctx, project):
        """
            Update a project
            returns a project complexmodel
        """

        proj_i = HydraIface.Project(project_id = project.id)
        
        proj_i.check_write_permission(ctx.in_header.user_id)
        
        proj_i.db.project_name        = project.name
        proj_i.db.project_description = project.description

        _add_project_attributes(proj_i, project.attributes)

        proj_i.save()

        return get_as_complexmodel(ctx, proj_i)


    @rpc(Integer, _returns=Project)
    def get_project(ctx, project_id):
        """
            get a project complexmodel
        """
        proj_i = HydraIface.Project(project_id = project_id)

        proj_i.check_read_permission(ctx.in_header.user_id)

        if proj_i.load() is False:
            raise ObjectNotFoundError("Project (project_id=%s) not found."%project_id)

        return get_as_complexmodel(ctx, proj_i)
 
    @rpc(Integer, _returns=SpyneArray(ProjectSummary))
    def get_projects(ctx):
        """
            get a project complexmodel
        """
        user_id = ctx.in_header.user_id
        if user_id is None:
            user_i = HydraIface.User()
            user_i.db.username = ctx.in_header.username
            user_id = user_i.get_user_id()

        if user_id is None:
           raise HydraError("User %s does not exist!", ctx.in_header.username)

        #Potentially join this with an rs of projects
        #where no owner exists?
        sql = """
            select
                p.project_id,
                p.project_name,
                p.cr_date,
                p.created_by
            from
                tOwner o,
                tProject p
            where
                o.user_id = %s
                and o.ref_key = 'PROJECT'
                and p.project_id = o.ref_id 
                and o.view = 'Y'
            order by p.cr_date

        """ % user_id

        rs = HydraIface.execute(sql)

        projects = []
        for r in rs:
            p = ProjectSummary()
            p.id = r.project_id
            p.name = r.project_name
            p.cr_date = str(r.cr_date)
            p.created_by = r.created_by
            projects.append(p)

        return projects


    @rpc(Integer, _returns=Boolean)
    def delete_project(ctx, project_id):
        """
            Set the status of a project to 'X'
        """
        success = True
        x = HydraIface.Project(project_id = project_id)
        x.check_write_permission(ctx.in_header.user_id)
        x.db.status = 'X'
        x.save()
        x.commit()

        return success

    @rpc(Integer, _returns=SpyneArray(Network))
    def get_networks(ctx, project_id):
        """
            Get all networks in a project
            Returns an array of network objects.
        """
        user_id = ctx.in_header.user_id
        networks = []

        x = HydraIface.Project(project_id = project_id)
        x.check_read_permission(user_id)
        x.load_all()

        for n_i in x.networks:
            try:
                n_i.check_read_permission(user_id)
                n_i.load()
                networks.append(get_as_complexmodel(ctx, n_i))
            except:
                logging.info("Not returning network %s as user %s does not have "
                             "permission to read it."%(n_i.db.network_id, user_id))

        return networks

