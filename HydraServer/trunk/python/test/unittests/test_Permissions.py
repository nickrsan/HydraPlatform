import test_HydraIface
import mysql.connector
from db import HydraIface
import datetime
from HydraLib.HydraException import HydraError

class UserTest(test_HydraIface.HydraIfaceTest):
 
    def test_update(self):
        x = HydraIface.User()
        x.db.username = "test"
        x.db.password = "12345"
        x.save()
        x.commit()

        x.db.password = "23456"
        x.save()
        x.commit()
        x.load()
        assert x.db.password == "23456", "User did not update correctly"

    def test_delete(self):
        x = HydraIface.User()
        x.db.username = "test"
        x.db.password = "12345"
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.User()
        x.db.username = "test"
        x.db.password = "12345"
        x.save()
        x.commit()
        x.load()

        y = HydraIface.User(user_id=x.db.user_id)
        assert y.load() == True, "Load did not work correctly"

class RoleTest(test_HydraIface.HydraIfaceTest):
 
    def test_update(self):
        x = HydraIface.Role()
        x.db.role_name = "Unimportant users"
        x.db.role_code = "Unimportant users @ %s"%datetime.datetime.now()
        x.save()
        x.commit()

        x.db.role_name = "Important users 1"
        x.save()
        x.commit()
        x.load()
        assert x.db.role_name == "Important users 1", "Role did not update correctly"
        x.delete()

    def test_delete(self):
        x = HydraIface.Role()
        x.db.role_name = "Unimportant users"
        x.db.role_code = "Unimportant users @ %s"%datetime.datetime.now()
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.Role()
        x.db.role_name = "Unimportant users"
        x.db.role_code = "Unimportant users @ %s"%datetime.datetime.now()
        x.save()
        x.commit()
        x.load()

        y = HydraIface.Role(role_id=x.db.role_id)
        assert y.load() == True, "Load did not work correctly"
        x.delete()


class PermTest(test_HydraIface.HydraIfaceTest):
 
    def test_update(self):
        x = HydraIface.Perm()
        x.db.perm_name = "Unimportant permission"
        x.db.perm_code = "Unimportant users"
        x.save()
        x.commit()

        x.db.perm_name = "Important permission"
        x.save()
        x.commit()
        x.load()
        assert x.db.perm_name == "Important permission", "Perm did not update correctly"
        x.delete()

    def test_delete(self):
        x = HydraIface.Perm()
        x.db.perm_name = "Unimportant permission"
        x.db.perm_code = "Unimportant users"
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.Perm()
        x.db.perm_name = "Unimportant users"
        x.db.perm_code = "Unimportant users"
        x.save()
        x.commit()
        x.load()

        y = HydraIface.Perm(perm_id=x.db.perm_id)
        assert y.load() == True, "Load did not work correctly"
        x.delete()


class RoleUserTest(test_HydraIface.HydraIfaceTest):

    def create_user(self, username):
        x = HydraIface.User()
        x.db.username = username
        x.db.password = "12345"
        x.save()
        x.commit()
        x.load()
        return x

    def create_role(self):
        x = HydraIface.Role()
        x.db.role_name = "Role 1"
        x.db.role_code = "Unimportant users @ %s"%datetime.datetime.now()
        x.save()
        x.commit()
        x.load()
        return x


    def test_update(self):
        user1 = self.create_user("User 1")
        role = self.create_role()

        x = HydraIface.RoleUser()
        x.db.user_id = user1.db.user_id
        x.db.role_id = role.db.role_id
        x.save()
        x.commit()

        user2 = self.create_user("User 2")
        x.db.user_id = user2.db.user_id
        x.save()
        x.commit()
        x.load()
        assert x.db.user_id == user2.db.user_id, "RoleUser did not update correctly"

    def test_fk(self):
        user1 = self.create_user("User 1")
        role = self.create_role()

        x = HydraIface.RoleUser(user_id=user1.db.user_id, role_id=0)
        self.assertRaises(HydraError, x.save)

        y = HydraIface.RoleUser(user_id=0, role_id=role.db.role_id)
        self.assertRaises(HydraError, y.save)

    def test_delete(self):
        user = self.create_user("User 1")
        role = self.create_role()

        x = HydraIface.RoleUser()
        x.db.user_id = user.db.user_id
        x.db.role_id = role.db.role_id
        x.save()
        x.commit()
        x.load()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        user = self.create_user("User 1")
        role = self.create_role()
        x = HydraIface.RoleUser(user_id=user.db.user_id, role_id=role.db.role_id)
        x.commit()
        x.save()
        y = HydraIface.RoleUser(user_id=user.db.user_id, role_id=role.db.role_id)
        assert y.load() == True, "Load did not work correctly"


class RolePermTest(test_HydraIface.HydraIfaceTest):

    def create_perm(self, perm_name):
        x = HydraIface.Perm()
        x.db.perm_name = perm_name
        x.db.perm_code = "Unimportant perm @ %s"%datetime.datetime.now()
        x.save()
        x.commit()
        x.load()
        return x

    def create_role(self):
        x = HydraIface.Role()
        x.db.role_name = "Role 1"
        x.db.role_code = "Unimportant users @ %s"%datetime.datetime.now()
        x.save()
        x.commit()
        x.load()
        return x


    def test_update(self):
        perm1 = self.create_perm("Perm 1")
        role = self.create_role()

        x = HydraIface.RolePerm()
        x.db.perm_id = perm1.db.perm_id
        x.db.role_id = role.db.role_id
        x.save()
        x.commit()

        perm2 = self.create_perm("Perm 2")
        x.db.perm_id = perm2.db.perm_id
        x.save()
        x.commit()
        x.load()
        assert x.db.perm_id == perm2.db.perm_id, "RolePerm did not update correctly"

    def test_fk(self):
        perm1 = self.create_perm("Perm 1")
        role = self.create_role()

        x = HydraIface.RolePerm(perm_id=perm1.db.perm_id, role_id=0)
        self.assertRaises(HydraError, x.save)

        y = HydraIface.RolePerm(perm_id=0, role_id=role.db.role_id)
        self.assertRaises(HydraError, y.save)

    def test_delete(self):
        perm = self.create_perm("Perm 1")
        role = self.create_role()

        x = HydraIface.RolePerm()
        x.db.perm_id = perm.db.perm_id
        x.db.role_id = role.db.role_id
        x.save()
        x.commit()
        x.load()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        perm = self.create_perm("Perm 1")
        role = self.create_role()
        x = HydraIface.RolePerm(perm_id=perm.db.perm_id, role_id=role.db.role_id)
        x.commit()
        x.save()
        y = HydraIface.RolePerm(perm_id=perm.db.perm_id, role_id=role.db.role_id)
        assert y.load() == True, "Load did not work correctly"





if __name__ == "__main__":
    test_HydraIface.run() # run all tests
