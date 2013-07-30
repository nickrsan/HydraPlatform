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

        y = HydraIface.ScenarioData(dataset_id=sd.db.dataset_id)
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


class DescriptorTest(test_HydraIface.HydraIfaceTest):

    def test_update(self):
        x = HydraIface.Descriptor()
        x.db.desc_val = "I am a descriptor"
        x.save()
        x.commit()

        x.db.desc_val = "I am a new descriptor"
        x.save()
        x.commit()
        x.load()

        assert x.db.desc_val == "I am a new descriptor", "Scalar did not update correctly"

    def test_delete(self):
        x = HydraIface.Descriptor()
        x.db.desc_val = "I am a descriptor"
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.Descriptor()
        x.db.desc_val = "I am a descriptor"
        x.save()
        x.commit()
        x.load()

        y = HydraIface.Descriptor(data_id=x.db.data_id)
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

        x.db.arr_data = [1, 2, 3, 4, 5]

        x.db.start_time = datetime.datetime.now()
        x.db.frequency = 1
        x.save()
        x.commit()

        x.db.arr_data = [1, 2, 3, 4]
        x.save()
        x.commit()
        x.load()

        assert x.db.arr_data == [1, 2, 3, 4], "tEquallySpacedTimeSeries did not update correctly"

    def test_delete(self):
        x = HydraIface.EquallySpacedTimeSeries()

        x.db.arr_data = [1, 2, 3, 4, 5]

        x.db.start_time = datetime.datetime.now()
        x.db.frequency = 1
        x.save()
        x.commit()
        
        x.delete()
        x.commit()

        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.EquallySpacedTimeSeries()

        x.db.arr_data = [1, 2, 3, 4, 5]

        x.db.start_time = datetime.datetime.now()
        x.db.frequency = 1
        x.save()
        x.commit()

        y = HydraIface.EquallySpacedTimeSeries(data_id=x.db.data_id)
        assert y.load() == True, "Load did not work correctly"

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

class DataAttrTest(test_HydraIface.HydraIfaceTest):

    def test_update(self):
        data = HydraIface.Scalar()
        data.db.param_value = Decimal("1.01")
        data.save()
        data.commit()
        data.load()
        
        dattr = HydraIface.DataAttr()
        dattr.db.data_id = data.db.data_id
        dattr.db.data_type = "Scalar"
        dattr.db.d_attr_name = "test attribute"
        dattr.db.d_attr_val  = 100.1
        dattr.save()
        dattr.commit()
        dattr.load()
        
        dattr.db.d_attr_val = 100.2
        dattr.save()
        dattr.commit()
        dattr.load()

        assert dattr.db.d_attr_val == Decimal("100.2"), "Data attr did not update correctly"

    def test_delete(self):
        data = HydraIface.Scalar()
        data.db.param_value = Decimal("1.01")
        data.save()
        data.commit()
        data.load()
        
        dattr = HydraIface.DataAttr()
        dattr.db.data_id = data.db.data_id
        dattr.db.data_type = "Scalar"
        dattr.db.d_attr_name = "test attribute"
        dattr.db.d_attr_val  = 100.1
        dattr.save()
        dattr.commit()
        dattr.load()
        
        dattr.delete()
        assert dattr.load() == False, "Delete did not work correctly."

    def test_load(self):
        data = HydraIface.Scalar()
        data.db.param_value = Decimal("1.01")
        data.save()
        data.commit()
        data.load()
        
        dattr = HydraIface.DataAttr()
        dattr.db.data_id = data.db.data_id
        dattr.db.data_type = "Scalar"
        dattr.db.d_attr_name = "test attribute"
        dattr.db.d_attr_val  = 100.1
        dattr.save()
        dattr.commit()
        dattr.load()
        
        dattr1 = HydraIface.DataAttr(d_attr_id=dattr.db.d_attr_id)
        assert dattr1.load() == True, "Load did not work correctly"

if __name__ == "__main__":
    test_HydraIface.run() # run all tests
