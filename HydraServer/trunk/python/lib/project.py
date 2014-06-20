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
from HydraLib.HydraException import ResourceNotFoundError
import scenario
import logging
from HydraLib.HydraException import PermissionError
from db.model import Project, ProjectOwner, Network
from db import DBSession
import network
from sqlalchemy.orm.exc import NoResultFound
from db.util import add_attributes


log = logging.getLogger(__name__)

def _get_project(project_id):
    try:
        project = DBSession.query(Project).filter(Project.project_id==project_id).one()
        return project
    except NoResultFound:
        raise ResourceNotFoundError("Project %s not found"%(project_id))

def _add_project_attribute_data(project_i, attr_map, attribute_data):
    if attribute_data is None:
        return []
    #As projects do not have scenarios (or to be more precise, they can only use
    #scenario 1, we can put
    #resource scenarios directly into the 'attributes' attribute
    #meaning we can add the data directly here.
    resource_scenarios = []
    for attr in attribute_data:

        rscen = scenario._update_resourcescenario(None, attr)
        if attr.resource_attr_id < 0:
            ra_i = attr_map[attr.resource_attr_id] 
            rscen.resourceattr = ra_i

        resource_scenarios.append(rscen)
    return resource_scenarios 

def add_project(project,**kwargs):
    """
        Add a new project
        returns a project complexmodel
    """
    user_id = kwargs.get('user_id') 

    #check_perm(user_id, 'add_project')
    proj_i = Project()
    proj_i.project_name = project.name
    proj_i.project_description = project.description
    proj_i.created_by = user_id

    attr_map = add_attributes(proj_i, project.attributes)
    proj_data = _add_project_attribute_data(proj_i, attr_map, project.attribute_data)
    proj_i.attribute_data = proj_data

    proj_i.set_owner(user_id)
    
    DBSession.add(proj_i)
    DBSession.flush()

    return proj_i

def update_project(project,**kwargs):
    """
        Update a project
        returns a project complexmodel
    """

    user_id = kwargs.get('user_id') 
    #check_perm(user_id, 'update_project')
    proj_i = _get_project(project.id) 
    
    proj_i.check_write_permission(user_id)
    
    proj_i.project_name        = project.name
    proj_i.project_description = project.description

    attr_map = add_attributes(proj_i, project.attributes)
    proj_data = _add_project_attribute_data(proj_i, attr_map, project.attribute_data)
    proj_i.attribute_data = proj_data
    DBSession.flush()

    return proj_i


def get_project(project_id,**kwargs):
    """
        get a project complexmodel
    """
    user_id = kwargs.get('user_id') 
    proj_i = _get_project(project_id)

    proj_i.check_read_permission(user_id)

    return proj_i

def get_projects(uid,**kwargs):
    """
        get a project complexmodel
    """
    req_user_id = kwargs.get('user_id') 

    #Potentially join this with an rs of projects
    #where no owner exists?

    projects = DBSession.query(Project).join(ProjectOwner).filter(ProjectOwner.user_id==uid).all()
    for project in projects:
        project.check_read_permission(req_user_id)

    return projects


def delete_project(project_id,**kwargs):
    """
        Set the status of a project to 'X'
    """
    user_id = kwargs.get('user_id') 
    #check_perm(user_id, 'delete_project')
    project = _get_project(project_id)
    project.check_write_permission(user_id)
    project.status = 'X'
    DBSession.flush()

def get_networks(project_id, include_data='N', **kwargs):
    """
        Get all networks in a project
        Returns an array of network objects.
    """
    user_id = kwargs.get('user_id') 
    project = _get_project(project_id)
    project.check_read_permission(user_id)

    rs = DBSession.query(Network.network_id).filter(Network.project_id==project_id).all()
    networks=[]
    for r in rs:
        try:
            net = network.get_network(r.network_id, include_data, **kwargs)
            networks.append(net)
        except PermissionError:
            log.info("Not returning network %s as user %s does not have "
                         "permission to read it."%(r.network_id, user_id))

    return networks

