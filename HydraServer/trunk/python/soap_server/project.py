from spyne.decorator import rpc
from spyne.model.primitive import Integer, Boolean
from db import HydraIface
from hydra_complexmodels import Project
from hydra_base import HydraService, ObjectNotFoundError

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

        x = HydraIface.Project()
        x.db.project_name = project.name
        x.db.project_description = project.description
        x.save()
        x.commit()

        ret = x.get_as_complexmodel()

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

#    @rpc(Integer, _returns=SpyneArray(Network))
#    def get_networks(ctx, project_id):
#        """
#            Get all networks in a project
#        """
#        networks = []
#
#        x = HydraIface.Project(project_id = project_id)
#
#        for n_i in x.get_networks():
#            networks.append(n_i.get_as_complexmodel())
#
#        return networks




