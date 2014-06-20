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
from spyne.decorator import rpc
from spyne.model.primitive import Integer, String, Unicode 
from spyne.model.complex import Array as SpyneArray
from hydra_complexmodels import Project,\
ProjectSummary,\
Network,\
get_as_complexmodel
from hydra_base import HydraService
from lib import project

class ProjectService(HydraService):
    """
        The project SOAP service
    """

    @rpc(Project, _returns=Project)
    def add_project(ctx, proj):
        """
            Add a new project
            returns a project complexmodel
        """

        proj = project.add_project(proj, **ctx.in_header.__dict__) 
        ret_proj = Project(proj)
        return ret_proj

    @rpc(Project, _returns=Project)
    def update_project(ctx, proj):
        """
            Update a project
            returns a project complexmodel
        """
        proj_i = project.update_project( proj,  **ctx.in_header.__dict__) 

        return Project(proj_i)


    @rpc(Integer, _returns=Project)
    def get_project(ctx, project_id):
        """
            get a project complexmodel
        """
        proj_dict = project.get_project(project_id,  **ctx.in_header.__dict__) 

        return Project(proj_dict)
 
    @rpc(Integer, _returns=SpyneArray(ProjectSummary))
    def get_projects(ctx, user_id):
        """
            get a project complexmodel
        """
        if user_id is None:
            user_id = ctx.in_header.user_id
        project_dicts = project.get_projects(user_id,  **ctx.in_header.__dict__)
        projects = [Project(p) for p in project_dicts]
        return projects


    @rpc(Integer, _returns=String)
    def delete_project(ctx, project_id):
        """
            Set the status of a project to 'X'
        """
        project.delete_project(project_id,  **ctx.in_header.__dict__)
        return 'OK' 

    @rpc(Integer, Unicode(pattern="[YN]", default='Y'), _returns=SpyneArray(Network))
    def get_networks(ctx, project_id, include_data):
        """
            Get all networks in a project
            Returns an array of network objects.
        """
        net_dicts = project.get_networks(project_id, include_data, **ctx.in_header.__dict__)
        networks = [Network(n) for n in net_dicts]
        return networks

