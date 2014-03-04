#!/usr/local/bin/python
import sys
if "./python" not in sys.path:
    sys.path.append("./python")
if "../../HydraLib/trunk/" not in sys.path:
    sys.path.append("../../HydraLib/trunk/")

import os
import unittest
import shutil
from tempfile import gettempdir as tmp
shutil.rmtree(os.path.join(tmp(), 'suds'), True)
from HydraLib import config
from SOAPpy import WSDL
import SOAPpy
from pysimplesoap.client import SoapClient
import osa
from HydraLib import PluginLib

class SoapTest(unittest.TestCase):
    def test_SOAPpy(self):
        url = config.get('hydra_client', 'url')
        print "Connecting to %s"%url
        SOAPpy.Config.debug = 1
        client = WSDL.Proxy(url, namespace="hydra.base")
        WSDL.Config.namespaceStyle = '2001'
        WSDL.Config.strictNamespaces = 0
        for m in client.methods.values():
            m.namespace = client.wsdl.targetNamespace
        #session_id = client.login('root','')
        #session_id = client.get_network(1)

        session_id = client.get_projects()

    def test_PySimpleSOAP(self):
        url = config.get('hydra_client', 'url')
        print "Connecting to %s"%url
        client = SoapClient(wsdl=url)
        client.namespace = 'hydra.base'
        response = client.login("root","")
        client.http_headers['session_id'] = response['loginResult']['session_id']
        client.http_headers['user_id'] = str(int(response['loginResult']['user_id']))
        client.http_headers['username'] = 'root'
        client['RequestHeader'] = client.http_headers
        import pudb; pudb.set_trace()
        net = client.get_network_quickly(107,'Y')

    def test_OSA(self):
        url = config.get('hydra_client', 'url')
        print "Connecting to %s"%url
        cl = osa.Client(url)
        response = cl.service_1.login(username="root",password="")
        req_header = cl.types.RequestHeader()
        req_header.session_id = response.session_id
        req_header.username = 'root'
        req_header.user_id = 1
        import pudb; pudb.set_trace()

    def test_SUDS(self):
        client = PluginLib.connect()
        #net = client.service.get_network_quickly(network_id=2)
        sd = client.service.get_scenario_data(2)
        node_data = client.service.get_node_data(597, 2)
        print(node_data)
def run():

    unittest.main()


if __name__ == "__main__":
    run() # run all tests
