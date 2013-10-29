import test_HydraIface
from db import HydraIface
from decimal import Decimal
import datetime

class DatasetGroupTest(test_HydraIface.HydraIfaceTest):

    def create_sd(self, value):
        data = HydraIface.Scalar()
        data.db.param_value = Decimal(value)
        data.save()
        data.commit()
        data.load()

        sd = HydraIface.ScenarioData()
        sd.db.data_id = data.db.data_id
        sd.db.data_type  = 'scalar'
        sd.db.data_units = 'metres-cubes'
        sd.db.data_name  = 'volume'
        sd.db.data_dimen = 'metres-cubed'
        sd.save()
        sd.commit()

        assert sd.load() == True, "ScenarioData 1 did not create correctly"

        return sd

    def test_create(self):
        
        sd1 = self.create_sd("1.01")
        sd2 = self.create_sd("1.02")

        group = HydraIface.DatasetGroup()
        group.db.group_name = "Test Group @ %s"%(datetime.datetime.now())

        group.save()
        group.commit()
        group.load()

        assert group.db.group_id is not None, "Group was not created correctly."

        group_entry_1 = HydraIface.DatasetGroupItem()
        group_entry_1.db.group_id = group.db.group_id
        group_entry_1.db.dataset_id = sd1.db.dataset_id
        group_entry_1.save()
        group_entry_1.commit()
    
        assert group_entry_1.load(), "Group entry was not created correctly"

        group_entry_2 = HydraIface.DatasetGroupItem()
        group_entry_2.db.group_id = group.db.group_id
        group_entry_2.db.dataset_id = sd2.db.dataset_id
        group_entry_2.save()
        group_entry_2.commit()
    
        assert group_entry_1.load(), "Group entry was not created correctly"

    

if __name__ == "__main__":
    test_HydraIface.run() # run all tests
