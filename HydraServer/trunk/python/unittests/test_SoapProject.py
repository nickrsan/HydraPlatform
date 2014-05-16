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
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
import datetime
import copy
import logging
log = logging.getLogger(__name__)

class ProjectTest(test_SoapServer.SoapServerTest):

    #def __init__(self):
    #    #super(ProjectTest).__init__(self)
    #    pass

    def add_data(self, proj):
        #Create some attributes, which we can then use to put data on our nodes
        attr1 = self.create_attr("proj_attr_1")
        attr2 = self.create_attr("proj_attr_2")
        attr3 = self.create_attr("proj_attr_3")

        proj_attr_1  = self.client.factory.create('hyd:ResourceAttr')
        proj_attr_1.id = -1
        proj_attr_1.attr_id = attr1.id
        proj_attr_2  = self.client.factory.create('hyd:ResourceAttr')
        proj_attr_2.attr_id = attr2.id
        proj_attr_2.id = -2
        proj_attr_3  = self.client.factory.create('hyd:ResourceAttr')
        proj_attr_3.attr_id = attr3.id
        proj_attr_3.id = -3

        attributes = self.client.factory.create('hyd:ResourceScenarioArray')
        
        attributes.ResourceScenario.append(self.create_descriptor(proj_attr_1, val="just project desscriptor"))
        attributes.ResourceScenario.append(self.create_array(proj_attr_2))
        attributes.ResourceScenario.append(self.create_timeseries(proj_attr_3))

        proj.attributes = attributes

        return proj

    def test_update(self):
        project = self.client.factory.create('hyd:Project')
        project.name = 'SOAP test %s'%(datetime.datetime.now())
        project.description = \
            'A project created through the SOAP interface.'
      
        project = self.add_data(project)
        
        project = self.client.service.add_project(project)
        new_project = copy.deepcopy(project)

        new_project.description = \
            'An updated project created through the SOAP interface.'
 
        updated_project = self.client.service.update_project(new_project)
 
        log.debug(updated_project)

        assert project.id == updated_project.id, \
            "project_id changed on update."
        assert project.created_by is not None, \
            "created by is null."
        assert project.name == updated_project.name, \
            "project_name changed on update."
        assert project.description != updated_project.description,\
            "project_description did not update"
        assert updated_project.description == \
            'An updated project created through the SOAP interface.', \
            "Update did not work correctly."

        rs_to_check = updated_project.attributes.ResourceScenario[0]
        assert rs_to_check.value.type == 'descriptor' and \
               rs_to_check.value.value.desc_val == 'just project desscriptor', \
               "There is an inconsistency with the attributes."

    def test_load(self):
        project = self.client.factory.create('hyd:Project')
        project.name = 'SOAP test %s'%(datetime.datetime.now())
        project.description = \
            'A project created through the SOAP interface.'
        project = self.client.service.add_project(project)

        new_project = self.client.service.get_project(project.id)

        assert new_project.name == project.name, \
            "project_name is not loaded correctly."
        assert project.description == new_project.description,\
            "project_description did not load correctly."

    def test_delete(self):
        project = self.client.factory.create('hyd:Project')
        project.name = 'SOAP test %s'%(datetime.datetime.now())
        project.description = \
            'A project created through the SOAP interface.'
        project = self.client.service.add_project(project)

        self.client.service.delete_project(project.id)

        proj = self.client.service.get_project(project.id)

        assert proj.status == 'X', \
            'Deleting project did not work correctly.'

    def test_get_projects(self):
        
        project = self.client.factory.create('hyd:Project')

        project.name = 'SOAP test %s'%(datetime.datetime.now())
        project.description = \
            'A project created through the SOAP interface.'
        project = self.client.service.add_project(project)

        projects = self.client.service.get_projects()

        assert len(projects.ProjectSummary) > 0, "Projects for user were not retrieved."

    def test_get_networks(self):
        
        proj = self.create_project('Project with multiple networks')

        self.create_network_with_data(proj.id)
        self.create_network_with_data(proj.id)

        nets = self.client.service.get_networks(proj.id)
        test_net = nets[0][0]
        assert test_net.scenarios is not None
        test_scenario = test_net.scenarios.Scenario[0]
        assert len(test_scenario.resourcescenarios.ResourceScenario) > 0
        assert len(test_net.nodes.Node) > 0
        assert len(test_net.links.Link) > 0

        assert len(nets.Network) == 2, "Networks were not retrieved correctly"
        
        nets = self.client.service.get_networks(proj.id, 'N')

        test_scenario = nets[0][0].scenarios.Scenario[0]
        assert test_scenario.resourcescenarios is None

if __name__ == '__main__':
    test_SoapServer.run()
