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
import datetime
from HydraLib.util import timestamp_to_ordinal

class ScenarioTest(test_HydraIface.HydraIfaceTest):

    def test_update(self):
        proj = self.create_project("Test Proj)")
        net = self.create_network("Test Net", proj.db.project_id)
        x = HydraIface.Scenario()
        x.db.scenario_name = "test"
        x.db.scenario_description = "test description"
        x.db.start_time = timestamp_to_ordinal(datetime.datetime.now())
        x.db.end_time   = timestamp_to_ordinal(datetime.datetime.now() + datetime.timedelta(hours=1))
        x.db.time_step = "1 day"
        x.db.network_id = net.db.network_id
        x.save()
        x.commit()

        x.db.scenario_name = "test_new"
        x.save()
        x.commit()
        x.load()
        assert x.db.scenario_name == "test_new", "Scenario did not update correctly"

    def test_delete(self):
        proj = self.create_project("Test Proj)")
        net = self.create_network("Test Net", proj.db.project_id)
        x = HydraIface.Scenario()
        x.db.scenario_name = "test"
        x.db.scenario_description = "test description"
        x.db.network_id = net.db.network_id
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        proj = self.create_project("Test Proj)")
        net = self.create_network("Test Net", proj.db.project_id)
        x = HydraIface.Scenario()
        x.db.scenario_name = "test"
        x.db.scenario_description = "test description"
        x.db.network_id = net.db.network_id
        x.save()
        x.commit()
        x.save()
        y = HydraIface.Scenario(scenario_id=x.db.scenario_id)
        assert y.load() == True, "Load did not work correctly"

    def test_update_shared_data(self):
        """
            Test for the situation where you want to update
            a piece of data which is being shared by multiple
            resources -- a new piece of data should be created.
        """
        project = self.create_project("Project A")
        network = self.create_network("Network A", project.db.project_id)
        scenario = self.create_scenario(network.db.network_id, "Scenario A")

        node_a = self.create_node("Node A", network.db.network_id)
        node_b = self.create_node("Node B", network.db.network_id)
        node_c = self.create_node("Node C", network.db.network_id)

        attr_a = self.create_attribute("Flow")

        ra = node_a.add_attribute(attr_a.db.attr_id)
        ra.save()
        rb = node_b.add_attribute(attr_a.db.attr_id)
        rb.save()
        rc = node_c.add_attribute(attr_a.db.attr_id)
        rc.save()

        base_time = datetime.datetime.now()
        t1 = timestamp_to_ordinal(base_time)
        t2 = timestamp_to_ordinal(base_time + datetime.timedelta(days=10)) 
        t3 = timestamp_to_ordinal(base_time + datetime.timedelta(days=20)) 

        ts_values = [(t1, [1, 2, 3]), (t2, [[1,2],[3,4]]), (t3, [1])]

        rsa = node_a.assign_value(scenario.db.scenario_id, ra.db.resource_attr_id, 'timeseries' , ts_values, 'm3', 'flow', 'int')
        rsb = node_a.assign_value(scenario.db.scenario_id, rb.db.resource_attr_id, 'scalar' ,3, 'm3', 'flow', 'int')
        rsc = node_a.assign_value(scenario.db.scenario_id, rc.db.resource_attr_id, 'scalar' ,3, 'm3', 'flow', 'int')
        rsc.commit()

        rsc_dataset_id = rsc.db.dataset_id

        rsd = node_a.assign_value(scenario.db.scenario_id, rc.db.resource_attr_id, 'scalar' ,10, 'm3', 'flow', 'int')
        rsd.commit()
        rsd_dataset_id = rsd.db.dataset_id

        assert rsd_dataset_id != rsc_dataset_id, "A new dataset should have been created but wasn't!"


    def test_get_val_at_timestamp(self):
        """
            Test for the retrieval of a single piece of data from a timeseries.
            Provide a time and get the correct value from the timeseries.
        """
        project = self.create_project("Project A")
        network = self.create_network("Network A", project.db.project_id)
        scenario = self.create_scenario(network.db.network_id, "Scenario A")

        node_a = self.create_node("Node A", network.db.network_id)
        node_b = self.create_node("Node B", network.db.network_id)
        node_c = self.create_node("Node C", network.db.network_id)

        attr_a = self.create_attribute("Flow")

        ra = node_a.add_attribute(attr_a.db.attr_id)
        ra.save()
        rb = node_b.add_attribute(attr_a.db.attr_id)
        rb.save()
        rc = node_c.add_attribute(attr_a.db.attr_id)
        rc.save()

        base_time = datetime.datetime.now()
        t1 = timestamp_to_ordinal(base_time)
        t2 = timestamp_to_ordinal(base_time + datetime.timedelta(days=10)) 
        t3 = timestamp_to_ordinal(base_time + datetime.timedelta(days=20)) 

        ts_values = [(t1, [1, 2, 3]), (t2, [[1,2],[3,4]]), (t3, [1])]

        rsa = node_a.assign_value(scenario.db.scenario_id, ra.db.resource_attr_id, 'timeseries' , ts_values, 'm3', 'flow', 'int')
        rsa.load_all()


        base_time = datetime.datetime.now()

        val123 = rsa.dataset.get_val(timestamp_to_ordinal(base_time + datetime.timedelta(seconds=1)))
        assert val123 == [1, 2, 3], "First Value is incorrect!"

        val1234 = rsa.dataset.get_val(
            timestamp_to_ordinal(base_time + datetime.timedelta(days=11)))
        assert val1234 == [[1,2],[3,4]], "Second value is incorrect!"
        
        val1 = rsa.dataset.get_val(
            timestamp_to_ordinal(base_time + datetime.timedelta(days=21)))
        assert val1 == [1], "Third value is incorrect!"

        after_ts_val = rsa.dataset.get_val(
            timestamp_to_ordinal(base_time + datetime.timedelta(days=31)))
        assert after_ts_val == [1], "Value for after range is incorrect!"

        before_ts_val = rsa.dataset.get_val(
            timestamp_to_ordinal(base_time - datetime.timedelta(days=30)))
        assert before_ts_val is None, "Value for after range is incorrect!"

        multi_ts_val = rsa.dataset.get_val(
            [timestamp_to_ordinal(base_time + datetime.timedelta(seconds=1)), 
             timestamp_to_ordinal(base_time - datetime.timedelta(days=30))])
        assert multi_ts_val == [[1, 2, 3], None], "Value for multiple timeseries is incorrect!"



if __name__ == "__main__":
    test_HydraIface.run() # run all tests
