import test_HydraIface
from db import HydraIface

class UserTest(test_HydraIface.HydraIfaceTest):
 
    def test_update(self):
        x = HydraIface.User()
        x.db.username = "test"
        x.save()
        x.commit()

        x.db.username = "test_new"
        x.save()
        x.commit()
        x.load()
        assert x.db.username == "test_new", "User did not update correctly"

    def test_delete(self):
        x = HydraIface.User()
        x.db.username = "test"
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.User()
        x.db.username = "test"
        x.save()
        x.commit()
        x.load()

        y = HydraIface.User(user_id=x.db.user_id)
        assert y.load() == True, "Load did not work correctly"

class RoleTest(test_HydraIface.HydraIfaceTest):
 
    def test_update(self):
        x = HydraIface.Role()
        x.db.role_name = "Unimportant users"
        x.save()
        x.commit()

        x.db.role_name = "Important users"
        x.save()
        x.commit()
        x.load()
        assert x.db.role_name == "Important users", "Role did not update correctly"

    def test_delete(self):
        x = HydraIface.Role()
        x.db.role_name = "Unimportant users"
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.Role()
        x.db.role_name = "Unimportant users"
        x.save()
        x.commit()
        x.load()

        y = HydraIface.Role(role_id=x.db.role_id)
        assert y.load() == True, "Load did not work correctly"


class PermTest(test_HydraIface.HydraIfaceTest):
 
    def test_update(self):
        x = HydraIface.Perm()
        x.db.perm_name = "Unimportant permission"
        x.save()
        x.commit()

        x.db.perm_name = "Important permission"
        x.save()
        x.commit()
        x.load()
        assert x.db.perm_name == "Important permission", "Perm did not update correctly"

    def test_delete(self):
        x = HydraIface.Perm()
        x.db.perm_name = "Unimportant permission"
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.Perm()
        x.db.perm_name = "Unimportant users"
        x.save()
        x.commit()
        x.load()

        y = HydraIface.Perm(perm_id=x.db.perm_id)
        assert y.load() == True, "Load did not work correctly"


if __name__ == "__main__":
    test_HydraIface.run() # run all tests
