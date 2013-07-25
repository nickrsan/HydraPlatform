import test_HydraIface
from db import HydraIface
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
        sd.load()

        y = HydraIface.ScenarioData(data_id=sd.db.data_id, data_type=sd.db.data_type)
        assert y.load() == True, "Load did not work correctly"


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
        x = HydraIface.Scalar()
        x.db.param_value = Decimal("1.01")
        x.save()
        x.commit()
        x.load()

        y = HydraIface.Scalar(data_id=x.db.data_id)
        assert y.load() == True, "Load did not work correctly"


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
        x = HydraIface.TimeSeries()
        x.db.ts_time = datetime.datetime.now()
        x.db.ts_value = 1.01
        x.save()
        x.commit()
        x.load()

        y = HydraIface.TimeSeries(data_id=x.db.data_id)
        assert y.load() == True, "Load did not work correctly"


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
        x = HydraIface.EquallySpacedTimeSeries()

        x.add_ts_array([1, 2, 3, 4, 5])

        x.db.start_time = datetime.datetime.now()
        x.db.frequency = 1
        x.save()
        x.commit()

        y = HydraIface.EquallySpacedTimeSeries(data_id=x.db.data_id)
        assert y.load() == True and x.ts_array.load() == True, "Load did not work correctly"

class ArrayTest(test_HydraIface.HydraIfaceTest):

    def test_update(self):
        x = HydraIface.Array()

        x.db.arr_data = [1, 2, 3, 4, 5]
        x.db.arr_x_dim = 1
        x.db.arr_y_dim = 2
        x.db.arr_z_dim = 3
        x.db.arr_precision = "really precise"
        x.db.arr_endianness = "totally endian."

        x.save()
        x.commit()

        x.db.arr_data = [1, 2, 3, 4]
        x.save()
        x.commit()
        x.load()

        assert x.db.arr_data == [1, 2, 3, 4], "tArray did not update correctly"

    def test_delete(self):
        x = HydraIface.Array()

        x.db.arr_data = [1, 2, 3, 4, 5]
        x.db.arr_x_dim = 1
        x.db.arr_y_dim = 2
        x.db.arr_z_dim = 3
        x.db.arr_precision = "really precise"
        x.db.arr_endianness = "totally endian."

        x.save()
        x.commit()
        
        x.delete()
        x.commit()

        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.Array()

        x.db.arr_data = [1, 2, 3, 4, 5]
        x.db.arr_x_dim = 1
        x.db.arr_y_dim = 2
        x.db.arr_z_dim = 3
        x.db.arr_precision = "really precise"
        x.db.arr_endianness = "totally endian."

        x.save()
        x.commit()
        x.db.load()

        x = HydraIface.Array(data_id=x.db.data_id)
        assert x.load() == True, "Load did not work correctly"

if __name__ == "__main__":
    test_HydraIface.run() # run all tests
