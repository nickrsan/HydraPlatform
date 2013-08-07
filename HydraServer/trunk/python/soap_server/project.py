from spyne.service import ServiceBase
from spyne.decorator import rpc
from spyne.model.primitive import Integer
from db import HydraIface
from hydra_complexmodels import Project

class ProjectService(ServiceBase):

    @rpc(Project, _returns=Project) 
    def add_project(ctx, project):
        x = HydraIface.Project()
        x.db.project_name = project.project_name
        x.db.project_description = project.project_description
        x.save()
        x.commit()
        return x.get_as_complexmodel()

    @rpc(Project, _returns=Project) 
    def update_project(ctx, project):
        x = HydraIface.Project(project_id = project.project_id)
        x.db.project_name = project.project_name
        x.db.project_description = project.project_description
        x.save()
        x.commit()
        return x.get_as_complexmodel()
 

    @rpc(Integer, _returns=Project)
    def get_project(ctx, project_id):
        x = HydraIface.Project(project_id = project_id)
        x.load()
        project = Project()
        project.project_id = x.db.project_id
        project.project_name = x.db.project_name
        project.project_description = x.db.project_description

        return x.get_as_complexmodel()
 


