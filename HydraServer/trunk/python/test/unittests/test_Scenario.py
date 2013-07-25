import test_HydraIface
from db import HydraIface

class ScenarioTest(test_HydraIface.HydraIfaceTest):

    def test_update(self):
        x = HydraIface.Scenario()
        x.db.scenario_name = "test"
        x.db.scenario_description = "test description"
        x.save()
        x.commit()

        x.db.scenario_name = "test_new"
        x.save()
        x.commit()
        x.load()
        assert x.db.scenario_name == "test_new", "Scenario did not update correctly"

    def test_delete(self):
        x = HydraIface.Scenario()
        x.db.scenario_name = "test"
        x.db.scenario_description = "test description"
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.Scenario()
        x.db.scenario_name = "test"
        x.db.scenario_description = "test description"
        x.save()
        x.commit()
        x.save()
        y = HydraIface.Scenario(scenario_id=x.db.scenario_id)
        assert y.load() == True, "Load did not work correctly"

if __name__ == "__main__":
    test_HydraIface.run() # run all tests
