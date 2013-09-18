from spyne.decorator import rpc
from spyne.model.primitive import Integer, Boolean
from spyne.model.complex import Array as SpyneArray
from db import HydraIface
from hydra_complexmodels import Project, ProjectSummary, Network
from hydra_base import HydraService, ObjectNotFoundError, get_user_id
from HydraLib.HydraException import HydraError

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
        proj_i.db.created_by = get_user_id(ctx.in_header.username)

        proj_i.save()

        proj_owner_i = HydraIface.ProjectOwner()
        proj_owner_i.db.user_id = get_user_id(ctx.in_header.username)
        proj_owner_i.db.project_id = proj_i.db.project_id

        proj_owner_i.save()

        ret = proj_i.get_as_complexmodel()

        return ret

    @rpc(Project, _returns=Project)
    def update_project(ctx, project):
        """
            Update a project
            returns a project complexmodel
        """

        x = HydraIface.Project(project_id = project.id)
        x.db.project_name        = project.name
        x.db.project_description = project.description

        x.save()

        return x.get_as_complexmodel()


    @rpc(Integer, _returns=Project)
    def get_project(ctx, project_id):
        """
            get a project complexmodel
        """
        x = HydraIface.Project(project_id = project_id)
        
        if x.load() is False:
            raise ObjectNotFoundError("Project (project_id=%s) not found."%project_id)

        return x.get_as_complexmodel()
 
    @rpc(Integer, _returns=SpyneArray(ProjectSummary))
    def get_projects(ctx):
        """
            get a project complexmodel
        """
        user_id = get_user_id(ctx.in_header.username)
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
                p.cr_date
            from
                tProjectOwner o,
                tProject p
            where
                o.user_id = %s
                and p.project_id = o.project_id 
            order by p.cr_date

        """ % user_id

        rs = HydraIface.execute(sql)

        projects = []
        for r in rs:
            p = ProjectSummary()
            p.id = r.project_id
            p.name = r.project_name
            p.cr_date = r.cr_date
            projects.append(p)

        return projects


    @rpc(Integer, _returns=Boolean)
    def delete_project(ctx, project_id):
        """
            Set the status of a project to 'X'
        """
        success = True
        x = HydraIface.Project(project_id = project_id)
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
        networks = []

        x = HydraIface.Project(project_id = project_id)
        x.load_all()

        for n_i in x.networks:
            n_i.load()
            networks.append(n_i.get_as_complexmodel())

        return networks

