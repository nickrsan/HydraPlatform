import test_HydraIface
from db import HydraIface
from HydraLib import util, hydra_logging

class HydraIfaceTest(test_HydraIface.HydraIfaceTest):

    def test_update(self):
        x = HydraIface.Constraint()
        x.db.constraint_name = "test"
        x.db.constraint_description = "test description"
        x.save()
        x.commit()

        x.db.constraint_name = "test_new"
        x.save()
        x.commit()
        x.load()
        assert x.db.constraint_name == "test_new", "Constraint did not update correctly"

    def test_delete(self):
        x = HydraIface.Constraint()
        x.db.constraint_name = "test"
        x.db.constraint_description = "test description"
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.Constraint()
        x.db.constraint_name = "test"
        x.db.constraint_description = "test description"
        x.save()
        x.commit()
        x.load()
        y = HydraIface.Constraint(constraint_id=x.db.constraint_id)
        assert y.load() == True, "Load did not work correctly"

if __name__ == "__main__":
    test_HydraIface.run() # run all tests
