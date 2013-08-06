from spyne.service import ServiceBase
from spyne.decorator import rpc
from spyne.model.primitive import Integer
from db import HydraIface
from hydra_struct import Project

class ProjectService(ServiceBase):

    @rpc(Project, _returns=Project) 
    def add_project(ctx, project):
        x = HydraIface.Project()
        x.db.project_name = project.project_name
        x.db.project_description = project.project_description
        x.save()
        x.commit()
        project.project_id = x.db.project_id
        return project

    @rpc(Project, _returns=Project) 
    def update_project(ctx, project):
        x = HydraIface.Project(project_id = project.project_id)
        x.db.project_name = project.project_name
        x.db.project_name = project.project_description
        x.save()
        x.commit()
        return project

    @rpc(Integer, _returns=Project)
    def get_project(ctx, project_id):
        x = HydraIface.Project(project_id = project_id)
        x.load()
        project = Project()
        project.project_id = x.db.project_id
        project.project_name = x.db.project_name
        project.project_description = x.db.project_description

        return project


