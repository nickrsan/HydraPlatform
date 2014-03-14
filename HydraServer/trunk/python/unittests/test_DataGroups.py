# (c) Copyright 2013, 2014, University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
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

        sd = self.create_scenario_data(data.db.data_id)

        assert sd.load() == True, "Dataset 1 did not create correctly"

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
