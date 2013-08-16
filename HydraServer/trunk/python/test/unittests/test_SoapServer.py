#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest
import logging
import shutil
import os

from HydraLib import hydra_logging

from suds.client import Client
from tempfile import gettempdir as tmp


class SoapServerTest(unittest.TestCase):

    def setUp(self):
        hydra_logging.init(level='INFO')
        # Clear SUDS cache:
        shutil.rmtree(os.path.join(tmp(), 'suds'), True)

    def tearDown(self):
        logging.debug("Tearing down")
        hydra_logging.shutdown()

    def connect(self):
        #logging.debug("Connecting to server.")
        url = 'http://localhost:8000?wsdl'
        client = Client(url)
        client.set_options(cache=None)
        client.add_prefix('hyd', 'soap_server.hydra_complexmodels')
        return client

    def create_project(self, name):
        cli = self.connect()
        project = cli.factory.create('hyd:Project')
        project.project_name = 'SOAP test'
        project = cli.service.add_project(project)
        return project

    def create_network(self, name, project_id):
        pass


def run():
    unittest.main()

if __name__ == '__main__':
    run()  # all tests
