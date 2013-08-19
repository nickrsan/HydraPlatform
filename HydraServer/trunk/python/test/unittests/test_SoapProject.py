#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
import copy


class ProjectTest(test_SoapServer.SoapServerTest):

    #def __init__(self):
    #    #super(ProjectTest).__init__(self)
    #    pass

    def test_update(self):
        cli = test_SoapServer.SoapServerTest.connect(self)
        project = cli.factory.create('hyd:Project')
        project.project_name = 'SOAP test'
        project.project_description = \
            'A project created through the SOAP interface.'
        project = cli.service.add_project(project)

        new_project = copy.deepcopy(project)
        new_project.project_description = \
            'An updated project created through the SOAP interface.'

        new_project = cli.service.update_project(new_project)

        assert project.project_id == new_project.project_id, \
            "project_id changed on update."
        assert project.project_name == new_project.project_name, \
            "project_name changed on update."
        assert project.project_description != new_project.project_description,\
            "project_description did not update"
        assert new_project.project_description == \
            'An updated project created through the SOAP interface.', \
            "Update did not work correctly."

    def test_load(self):
        cli = test_SoapServer.SoapServerTest.connect(self)
        project = cli.factory.create('hyd:Project')
        project.project_name = 'SOAP test'
        project.project_description = \
            'A project created through the SOAP interface.'
        project = cli.service.add_project(project)

        new_project = cli.service.get_project(project.project_id)

        assert new_project.project_name == project.project_name, \
            "project_name is not loaded correctly."
        assert project.project_description == new_project.project_description,\
            "project_description did not load correctly."

    def test_delete(self):
        cli = test_SoapServer.SoapServerTest.connect(self)
        project = cli.factory.create('hyd:Project')
        project.project_name = 'SOAP test'
        project.project_description = \
            'A project created through the SOAP interface.'
        project = cli.service.add_project(project)

        cli.service.delete_project(project.project_id)

        proj = cli.service.get_project(project.project_id)

        assert proj.status == 'X', \
            'Deleting project did not work correctly.'


if __name__ == '__main__':
    test_SoapServer.run()
