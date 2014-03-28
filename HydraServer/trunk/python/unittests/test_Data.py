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
from HydraLib.util import timestamp_to_ordinal, ordinal_to_timestamp

class DatasetTest(test_HydraIface.HydraIfaceTest):

    def test_create(self):

        data = HydraIface.Scalar()
        data.db.param_value = Decimal("1.01")
        data.save()
        data.commit()
        data.load()


        d = self.create_scenario_data(data.db.data_id) 

        assert d.load() == True, "Dataset did not create correctly"

    def test_update(self):

        data = HydraIface.Scalar()
        data.db.param_value = Decimal("1.01")
        data.save()
        data.commit()
        data.load()

        d = self.create_scenario_data(data.db.data_id) 

        d.db.data_type  = 'scalar_updated'
        d.db.data_units = 'metres-cubes_updated'
        d.db.data_name  = 'volume_updated'
        d.db.data_dimen = 'metres-cubed_updated'
        d.save()
        d.commit()
        d.load()

        assert d.db.data_type == "scalar_updated", "Dataset did not update correctly"

    def test_delete(self):

        data = HydraIface.Scalar()
        data.db.param_value = Decimal("1.01")
        data.save()
        data.commit()
        data.load()

        d = self.create_scenario_data(data.db.data_id) 

        d.delete()
        d.save()
        d.commit()

        assert d.load() == False, "Dataset did not delete correctly"

    def test_load(self):

        data = HydraIface.Scalar()
        data.db.param_value = Decimal("1.01")
        data.save()
        data.commit()
        data.load()

        d = self.create_scenario_data(data.db.data_id) 
        
        y = HydraIface.Dataset(dataset_id=d.db.dataset_id)
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

        t1 = timestamp_to_ordinal(datetime.datetime(2013, 07, 31, 9, 00, 00))
        t2 = timestamp_to_ordinal(datetime.datetime(2013, 07, 31, 9, 01, 00))
        t3 = timestamp_to_ordinal(datetime.datetime(2013, 07, 31, 9, 02, 00))

        ts_values = [
            (t1, 1),
            (t2, 2),
            (t3, 3),
        ]

        x.set_ts_values(ts_values)
        x.save()
        x.commit()

        x.set_ts_value(t1, 4)
        x.save()
        x.commit()
        x.load()

        assert x.get_ts_value(t1) == Decimal("4"), "TimeSeries did not update correctly"

    def test_delete(self):
        x = HydraIface.TimeSeries()

        t1 = timestamp_to_ordinal(datetime.datetime(2013, 07, 31, 9, 00, 00))
        t2 = timestamp_to_ordinal(datetime.datetime(2013, 07, 31, 9, 01, 00))
        t3 = timestamp_to_ordinal(datetime.datetime(2013, 07, 31, 9, 02, 00))

        ts_values = [
            (t1, 1),
            (t2, 2),
            (t3, 3),
        ]        
        
        x.set_ts_values(ts_values)
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.TimeSeries()

        t1 = timestamp_to_ordinal(datetime.datetime(2013, 07, 31, 9, 00, 00))
        t2 = timestamp_to_ordinal(datetime.datetime(2013, 07, 31, 9, 01, 00))
        t3 = timestamp_to_ordinal(datetime.datetime(2013, 07, 31, 9, 02, 00))

        ts_values = [
            (t1, 1),
            (t2, 2),
            (t3, 3),
        ]   

        x.set_ts_values(ts_values)

        x.save()
        x.commit()
        x.load()

        y = HydraIface.TimeSeries(data_id=x.db.data_id)
        assert y.load() == True, "Load did not work correctly"

    def test_conversion(self):
        x = datetime.datetime.now()
        ordinal_x = timestamp_to_ordinal(x)
        ts = HydraIface.TimeSeries()
        ts.set_ts_value(ordinal_x, 1)
        ts.save()
    
    
        ts_time = ts.get_val()[0][0]
        #print "%s == %s"%(ordinal_x, ts_time)
        #print "%s == %s"%(len(str(ordinal_x)[str(ordinal_x).find('.'):]),len(str(ts_time)[str(ts_time).find('.'):]))
        assert ts_time == ordinal_x

        y = ordinal_to_timestamp(ts_time)
        ordinal_y = timestamp_to_ordinal(y)
        assert x == y
        assert ordinal_x == ordinal_y
        assert ts_time == ordinal_y

class EqTimeSeriesTest(test_HydraIface.HydraIfaceTest):

    def test_update(self):
        x = HydraIface.EqTimeSeries()

        x.db.arr_data = [1, 2, 3, 4, 5]

        t1 = timestamp_to_ordinal(datetime.datetime(2013, 07, 31, 9, 00, 00))
        x.db.start_time = t1
        x.db.frequency = 1
        x.save()
        x.commit()

        x.db.arr_data = [1, 2, 3, 4]
        x.save()
        x.commit()
        x.load()

        assert x.db.arr_data == [1, 2, 3, 4], "tEqTimeSeries did not update correctly"

    def test_delete(self):
        x = HydraIface.EqTimeSeries()

        x.db.arr_data = [1, 2, 3, 4, 5]

        t1 = timestamp_to_ordinal(datetime.datetime(2013, 07, 31, 9, 00, 00))
        x.db.start_time = t1
        x.db.frequency = 1
        x.save()
        x.commit()

        x.delete()
        x.commit()

        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.EqTimeSeries()

        x.db.arr_data = [1, 2, 3, 4, 5]

        t1 = timestamp_to_ordinal(datetime.datetime(2013, 07, 31, 9, 00, 00))
        x.db.start_time = t1
        x.db.frequency = 1
        x.save()
        x.commit()

        y = HydraIface.EqTimeSeries(data_id=x.db.data_id)
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

    def create_dataset(self, data_id):
        d = HydraIface.Dataset()
        d.db.data_id    = data_id
        d.db.data_type  = 'scalar'
        d.db.data_units = 'metres-cubes'
        d.db.data_name  = 'volume'
        d.db.data_dimen = 'metres-cubed'
        d.load_all()
        d.set_hash(d.get_val())
        d.save()
        d.commit()
        return d

    def test_update(self):
        data = HydraIface.Scalar()
        data.db.param_value = Decimal("1.01")
        data.save()
        data.commit()
        data.load()

        d = self.create_dataset(data.db.data_id)

        dattr = HydraIface.DataAttr()
        dattr.db.dataset_id = d.db.dataset_id
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

        d = self.create_dataset(data.db.data_id)

        dattr = HydraIface.DataAttr()
        dattr.db.dataset_id = d.db.dataset_id
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

        d = self.create_dataset(data.db.data_id)

        dattr = HydraIface.DataAttr()
        dattr.db.dataset_id = d.db.dataset_id
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
