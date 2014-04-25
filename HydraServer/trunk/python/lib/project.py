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
from hydra_base import ObjectNotFoundError
from HydraLib.HydraException import HydraError
import scenario
import logging
from network import NetworkService
import network
import users

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

    def add_project(user_id, project):
        """
            Add a new project
            returns a project complexmodel
        """

        #check_perm(user_id, 'add_project')
        proj_i = HydraIface.Project()
        proj_i.db.project_name = project.name
        proj_i.db.project_description = project.description
        proj_i.db.created_by = user_id

        proj_i.save()

        _add_project_attributes(proj_i, project.attributes)

        user_id = user_id
        proj_i.set_ownership(user_id)

        return proj_i

    def update_project(user_id, project):
        """
            Update a project
            returns a project complexmodel
        """

        #check_perm(user_id, 'update_project')
        proj_i = HydraIface.Project(project_id = project.id)
        
        proj_i.check_write_permission(user_id)
        
        proj_i.db.project_name        = project.name
        proj_i.db.project_description = project.description

        _add_project_attributes(proj_i, project.attributes)

        proj_i.save()

        return proj_i


    def get_project(user_id, project_id):
        """
            get a project complexmodel
        """
        proj_i = HydraIface.Project(project_id = project_id)

        proj_i.check_read_permission(user_id)

        if proj_i.load() is False:
            raise ObjectNotFoundError("Project (project_id=%s) not found."%project_id)

        return proj_i
 
    def get_projects(user_id):
        """
            get a project complexmodel
        """

        username = users.get_username(user_id)

        user_id = user_id
        if user_id is None:
            user_i = HydraIface.User()
            user_i.db.username = username
            user_id = user_i.get_user_id()

        if user_id is None:
           raise HydraError("User %s does not exist!", username)

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
            p = dict(
                id = r.project_id,
                name = r.project_name,
                cr_date = str(r.cr_date),
                created_by = r.created_by,
            )
            projects.append(p)

        return projects


    def delete_project(user_id, project_id):
        """
            Set the status of a project to 'X'
        """
        #check_perm(user_id, 'update_project')
        x = HydraIface.Project(project_id = project_id)
        x.check_write_permission(user_id)
        x.db.status = 'X'
        x.save()
        x.commit()

        return 'OK' 

    def get_networks(user_id, project_id):
        """
            Get all networks in a project
            Returns an array of network objects.
        """
        user_id = user_id
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
                net = network.get_network(user_id, r.network_id, 'Y', None)
                networks.append(net)
            except:
                logging.info("Not returning network %s as user %s does not have "
                             "permission to read it."%(r.network_id, user_id))

        return networks

