
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
import bcrypt

class UsersTest(test_SoapServer.SoapServerTest):

    def test_add_user(self):
        cli = test_SoapServer.SoapServerTest.connect(self, login=True)
        user = cli.factory.create('hyd:User')
        user.username = "test_user"
        user.password = "test_user_password"

        new_user = cli.service.add_user(user)

        assert new_user.username == user.username, "Usernames are not the same!"
        assert bcrypt.hashpw(user.password, new_user.password) == new_user.password

        delete_result = cli.service.delete_user(new_user)

        assert delete_result == 'OK', "User was not removed!"


    def test_add_role(self):
        cli = test_SoapServer.SoapServerTest.connect(self, login=True)
        role = cli.factory.create('hyd:Role')
        role.name = "test_role"

        new_role = cli.service.add_role(role)

        assert new_role.id is not None, "Role does not have an ID!"
        assert new_role.name == role.name, "Role are not the same!"

        delete_result = cli.service.delete_role(new_role)

        #print "Delete result: %s"%delete_result

        assert delete_result == 'OK', "Role was not removed!"

    def test_add_perm(self):
        cli = test_SoapServer.SoapServerTest.connect(self, login=True)
        perm = cli.factory.create('hyd:Perm')
        perm.name = "test_perm"

        new_perm = cli.service.add_perm(perm)

        assert new_perm.id is not None, "Perm does not have an ID!"
        assert new_perm.name == perm.name, "Perm are not the same!"

        delete_result = cli.service.delete_perm(new_perm)

        #print "Delete result: %s"%delete_result

        assert delete_result == 'OK', "perm was not removed!"

    def test_set_user_role(self):
        cli = test_SoapServer.SoapServerTest.connect(self, login=True)

        user = cli.factory.create('hyd:User')
        user.username = "test_user"
        user.password = "test_user_password"

        new_user = cli.service.add_user(user)

        role = cli.factory.create('hyd:Role')
        role.name = "test_role"

        new_role = cli.service.add_role(role)

        role_with_users = cli.service.set_user_role(new_user, new_role)

        assert role_with_users is not None, "Role user was not set correctly"
        assert role_with_users.roleusers.RoleUser[0].user_id == new_user.id, "User was not added to role correctly."

        delete_result = cli.service.delete_user_role(new_user, new_role)

        assert delete_result == 'OK', "Role User was not removed!"

        delete_result = cli.service.delete_user(new_user)

        #print "Delete result: %s"%delete_result

        assert delete_result == 'OK', "Role User was not removed!"


    def test_set_role_perm(self):
        cli = test_SoapServer.SoapServerTest.connect(self, login=True)

        role = cli.factory.create('hyd:Role')
        role.name = "test_role"

        new_role = cli.service.add_role(role)

        perm = cli.factory.create('hyd:Perm')
        perm.name = "test_perm"

        new_perm = cli.service.add_perm(perm)

        role_with_perms = cli.service.set_role_perm(new_role, new_perm)

        assert role_with_perms is not None, "Role perm was not set correctly"
        assert role_with_perms.roleperms.RolePerm[0].perm_id == new_perm.id, "Perm was not added to role correctly."

        delete_result = cli.service.delete_role_perm(new_role, new_perm)

        assert delete_result == 'OK', "Role Perm was not removed!"

        delete_result = cli.service.delete_perm(new_perm)

        #print "Delete result: %s"%delete_result

        assert delete_result == 'OK', "Role Perm was not removed!"

if __name__ == '__main__':
    test_SoapServer.run()
