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
from HydraLib.HydraException import ResourceNotFoundError
import scenario
import logging
from HydraLib.HydraException import PermissionError
from db.HydraAlchemy import Project
from db import DBSession
import network

log = logging.getLogger(__name__)

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

  #  for attr in project.attributes:
  #      project.add_attribute(attr.attr_id)

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
    proj_i = HydraIface.Project(project_id = project.id)
    
    proj_i.check_write_permission(user_id)
    
    proj_i.db.project_name        = project.name
    proj_i.db.project_description = project.description

    _add_project_attributes(proj_i, project.attributes)

    proj_i.save()

    return proj_i.get_as_dict(**{'user_id':user_id})


def get_project(project_id,**kwargs):
    """
        get a project complexmodel
    """
    user_id = kwargs.get('user_id') 
    proj_i = HydraIface.Project(project_id = project_id)

    proj_i.check_read_permission(user_id)

    if proj_i.load() is False:
        raise ResourceNotFoundError("Project (project_id=%s) not found."%project_id)

    return proj_i.get_as_dict(**{'user_id':user_id})

def get_projects(user_to_check,**kwargs):
    """
        get a project complexmodel
    """
    user_id = kwargs.get('user_id') 

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

    """ % user_to_check

    rs = HydraIface.execute(sql)

    projects = []
    for r in rs:
        p = HydraIface.Project()
        p.db.project_id = r.project_id
        p.db.project_name = r.project_name
        p.db.cr_date = str(r.cr_date)
        p.db.created_by = r.created_by
        p.ref_id = r.project_id
        projects.append(p.get_as_dict(**{'user_id':user_id}))

    return projects


def delete_project(project_id,**kwargs):
    """
        Set the status of a project to 'X'
    """
    user_id = kwargs.get('user_id') 
    #check_perm(user_id, 'delete_project')
    x = HydraIface.Project(project_id = project_id)
    x.check_write_permission(user_id)
    x.db.status = 'X'
    x.save()
    x.commit()

def get_networks(project_id, include_data='N', **kwargs):
    """
        Get all networks in a project
        Returns an array of network objects.
    """
    user_id = kwargs.get('user_id') 
    x = HydraIface.Project(project_id = project_id)
    x.check_read_permission(user_id)

    networks = []

    sql = """
        select
            network_id
        from
            tNetwork
        where
            project_id=%s
    """%project_id

    rs = HydraIface.execute(sql)

    for r in rs:
        try:
            net = network.get_network(r.network_id, include_data, **kwargs)
            networks.append(net)
        except PermissionError:
            log.info("Not returning network %s as user %s does not have "
                         "permission to read it."%(r.network_id, user_id))

    return networks

