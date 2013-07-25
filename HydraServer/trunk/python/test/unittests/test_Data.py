import unittest
import logging
from db import HydraIface
import test_HydraIface
from decimal import Decimal
import datetime

class ScenarioDataTest(test_HydraIface.HydraIfaceTest):
 
    def test_create(self):

        data = HydraIface.Scalar()
        data.db.param_value = Decimal("1.01")
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

        assert sd.load() == True, "ScenarioData did not create correctly"
 
    def test_update(self):

        data = HydraIface.Scalar()
        data.db.param_value = Decimal("1.01")
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

        sd.db.data_type  = 'scalar_updated'
        sd.db.data_units = 'metres-cubes_updated'
        sd.db.data_name  = 'volume_updated'
        sd.db.data_dimen = 'metres-cubed_updated'
        sd.save()
        sd.commit()
        sd.load()

        assert sd.db.data_type == "scalar_updated", "ScenarioData did not update correctly"

    def test_delete(self):

        data = HydraIface.Scalar()
        data.db.param_value = Decimal("1.01")
        data.save()
        data.commit()
        data.load()

        sd = HydraIface.ScenarioData()
        sd.db.data_id = data.db.data_id
        sd.db.data_type = 'scalar'
        sd.db.data_units = 'metres-cubes'
        sd.db.data_name  = 'volume'
        sd.db.data_dimen = 'metres-cubed'

        sd.save()
        sd.commit()

        sd.delete()
        sd.save()
        sd.commit()

        assert sd.load() == False, "ScenarioData did not delete correctly"

    def test_load(self):
        cursor = self.connection.cursor()
        cursor.execute("select min(data_id) as data_id, data_type from tScenarioData group by data_type")
        row = cursor.fetchall() 
        data_id = row[0][0]
        data_type = row[0][1]
        x = HydraIface.ScenarioData(data_id=data_id, data_type=data_type)
        assert x.load() == True, "Load did not work correctly"


class ScalarTest(test_HydraIface.HydraIfaceTest):

    def test_update(self):
        x = HydraIface.Scalar()
        x.db.param_value = Decimal("1.01")
        x.save()
        x.commit()

        x.db.param_value = Decimal("2.02")
        x.save()
        x.commit()
        x.load()

        assert x.db.param_value == Decimal("2.02"), "Scalar did not update correctly"

    def test_delete(self):
        x = HydraIface.Scalar()
        x.db.param_value = Decimal("1.01")
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        cursor = self.connection.cursor()
        cursor.execute("select min(data_id) as data_id from tScalar")
        data_id = cursor.fetchall()[0][0]
        x = HydraIface.Scalar(data_id=data_id)
        assert x.load() == True, "Load did not work correctly"


class TimeSeriesTest(test_HydraIface.HydraIfaceTest):
 
    def test_update(self):
        x = HydraIface.TimeSeries()
        x.db.ts_time = datetime.datetime.now()
        x.db.ts_value = 1.01
        x.save()
        x.commit()

        x.db.ts_value = 2.02
        x.save()
        x.commit()
        x.load()

        assert x.db.ts_value == Decimal("2.02"), "TimeSeries did not update correctly"

    def test_delete(self):
        x = HydraIface.TimeSeries()
        x.db.ts_time = datetime.datetime.now()
        x.db.ts_value = 1.01
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        cursor = self.connection.cursor()
        cursor.execute("select min(data_id) as data_id from tTimeSeries")
        row = cursor.fetchall()[0]
        data_id = row[0]
        x = HydraIface.TimeSeries(data_id=data_id)
        assert x.load() == True, "Load did not work correctly"


class EquallySpacedTimeSeriesTest(test_HydraIface.HydraIfaceTest):

    def test_update(self):
        x = HydraIface.EquallySpacedTimeSeries()

        x.add_ts_array([1, 2, 3, 4, 5])

        x.db.start_time = datetime.datetime.now()
        x.db.frequency = 1
        x.save()
        x.commit()

        x.add_ts_array([1, 2, 3, 4])
        x.save()
        x.commit()
        x.load()

        assert x.get_ts_array() == [1, 2, 3, 4], "tEquallySpacedTimeSeries did not update correctly"

    def test_delete(self):
        x = HydraIface.EquallySpacedTimeSeries()

        x.add_ts_array([1, 2, 3, 4, 5])

        x.db.start_time = datetime.datetime.now()
        x.db.frequency = 1
        x.save()
        x.commit()
        
        x.delete()
        x.commit()

        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        cursor = self.connection.cursor()
        cursor.execute("select min(data_id) as data_id from tEquallySpacedTimeSeries")
        row = cursor.fetchall()[0]
        data_id = row[0]
        x = HydraIface.EquallySpacedTimeSeries(data_id=data_id)
        assert x.load() == True and x.ts_array.load() == True, "Load did not work correctly"




if __name__ == "__main__":
    unittest.main() # run all tests
