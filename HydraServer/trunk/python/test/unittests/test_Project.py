import test_HydraIface
from db import HydraIface

class ProjectTest(test_HydraIface.HydraIfaceTest):
    def test_update(self):
        x = HydraIface.Project()
        x.db.project_name = "test"
        x.db.project_description = "test description"
        x.save()
        x.commit()

        x.db.project_name = "test_new"
        x.save()
        x.commit()
        x.load()
        assert x.db.project_name == "test_new", "Project did not update correctly"

    def test_delete(self):
        x = HydraIface.Project()
        x.db.project_name = "test"
        x.db.project_description = "test description"
        x.save()
        x.commit()
        assert x.load() == True, "Pre-delete save did not work correctly."

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.Project()
        x.db.project_name = "test"
        x.db.project_description = "test description"
        x.save()
        x.commit()
        x.load()
        y = HydraIface.Project(project_id=x.db.project_id)
        assert y.load() == True, "Load did not work correctly"


if __name__ == "__main__":
    test_HydraIface.run() # run all tests
