#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer


class NetworkTest(test_SoapServer.SoapServerTest):

    def test_update(self):
        cli = test_SoapServer.SoapServerTest.connect(self)
        project = test_SoapServer.SoapServerTest.create_project(self, 'test')
        network = cli.factory.create('hyd:Network')
        nodes = cli.factory.create('hyd:NodeArray')
        links = cli.factory.create('hyd:LinkArray')

        nnodes = 3
        nlinks = 2

        for i in range(nnodes):
            nodes.Node.append(cli.factory.create('hyd:Node'))

        for i in range(nlinks):
            links.Link.append(cli.factory.create('hyd:Link'))


    def test_load(self):
        pass

    def test_delete(self):
        pass
