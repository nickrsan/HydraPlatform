#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer

class PermissionTest(test_SoapServer.SoapServerTest):

    #This relies on there being a user named 'root' with an empty password.
    def test_good_login(self):
        cli = test_SoapServer.connect(login=False)
        token = cli.service.login('root', '')
        assert token is not None, "Login did not work correctly!"

    def test_bad_login(self):
        cli = test_SoapServer.connect(login=False)
        token = None
        try:
            token = cli.service.login('root', 'invalid_password')
        except Exception, e:
            assert e.fault.faultcode == "senv:Client.AuthenticationError", "An unexpected excepton was thrown!"

        assert token is None, "Unexpected successful login."

    def test_logout(self):
        client = test_SoapServer.connect(login=True)
        msg = client.service.logout('root')
        assert msg == 'OK', "Logout failed."

if __name__ == '__main__':
    test_SoapServer.run()
