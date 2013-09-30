#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
import copy

def setup():
    test_SoapServer.connect()

class ProjectTest(test_SoapServer.SoapServerTest):

    #def __init__(self):
    #    #super(ProjectTest).__init__(self)
    #    pass

    def test_update(self):
        project = self.client.factory.create('hyd:Project')
        project.name = 'SOAP test'
        project.description = \
            'A project created through the SOAP interface.'
        project = self.client.service.add_project(project)

        new_project = copy.deepcopy(project)
        new_project.description = \
            'An updated project created through the SOAP interface.'

        new_project = self.client.service.update_project(new_project)

        assert project.id == new_project.id, \
            "project_id changed on update."
        assert project.created_by is not None, \
            "created by is null."
        assert project.name == new_project.name, \
            "project_name changed on update."
        assert project.description != new_project.description,\
            "project_description did not update"
        assert new_project.description == \
            'An updated project created through the SOAP interface.', \
            "Update did not work correctly."

    def test_load(self):
        project = self.client.factory.create('hyd:Project')
        project.name = 'SOAP test'
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
        project.name = 'SOAP test'
        project.description = \
            'A project created through the SOAP interface.'
        project = self.client.service.add_project(project)

        self.client.service.delete_project(project.id)

        proj = self.client.service.get_project(project.id)

        assert proj.status == 'X', \
            'Deleting project did not work correctly.'

    def test_get_projects(self):
        
        project = self.client.factory.create('hyd:Project')

        project.name = 'SOAP test'
        project.description = \
            'A project created through the SOAP interface.'
        project = self.client.service.add_project(project)

        projects = self.client.service.get_projects()

        assert len(projects.ProjectSummary) > 0, "Projects for user were not retrieved."

    def test_get_networks(self):
        
        proj = self.create_project('Project with multiple networks')

        self.create_network(proj)
        self.create_network(proj)

        nets = self.client.service.get_networks(proj.id)

        assert len(nets.Network) == 2, "Networks were not retrieved correctly"

if __name__ == '__main__':
    test_SoapServer.run()
