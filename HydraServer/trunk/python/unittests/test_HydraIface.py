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
import sys
if "../../" not in sys.path:
    sys.path.append("../../")
if "../../../../../HydraLib/trunk/" not in sys.path:
    sys.path.append("../../../../../HydraLib/trunk/")
import unittest
from db import HydraIface
from db import hdb
from HydraLib import hydra_logging
import logging
import datetime

class HydraIfaceTest(unittest.TestCase):
    def setUp(self):
        self.am = None
        self.x = None
        hydra_logging.init(level='INFO')
        HydraIface.init(hdb.connect())

    def tearDown(self):

        if self.am is not None:
            self.am.delete()
            self.am.save()
            self.am.commit()

        if self.x is not None:
            self.x.delete()
            self.x.save()
            self.x.commit()


        hdb.commit()
        logging.debug("Tearing down")
        hdb.disconnect()
        hydra_logging.shutdown()

    def create_project(self, name):
        x = HydraIface.Project()
        x.db.project_name =  '%s_%s'%(name, datetime.datetime.now())
        x.save()
        x.commit()
        return x

    def create_network(self, name, project_id):
        x = HydraIface.Network()
        x.db.network_name = name
        x.db.project_id = project_id
        x.save()
        x.commit()
        return x

    def create_scenario(self, network_id, name):
        x = HydraIface.Scenario()
        x.db.scenario_name = name
        x.db.network_id    = network_id
        x.save()
        x.commit()
        return x

    def create_node(self, name, network_id):
        x = HydraIface.Node()
        x.db.node_name = name
        x.db.network_id = network_id
        x.save()
        x.commit()
        return x

    def create_link(self, name, network_id, node_x_id, node_y_id):
        x = HydraIface.Link()
        x.db.link_name = name
        x.db.network_id = network_id
        x.db.node_1_id = node_x_id
        x.db.node_2_id = node_y_id
        x.save()
        x.commit()
        return x

    def create_attribute(self, name):
        sql = """
            select
                attr_id
            from
                tAttr
            where
                attr_name = '%s'
        """ % name

        rs = HydraIface.execute(sql)

        if len(rs) == 0:
            x = HydraIface.Attr()
            x.db.attr_name = name
            x.db.attr_description = "test description"
            x.save()
            x.commit()
        else:
            x = HydraIface.Attr(attr_id=rs[0].attr_id)
            x.load()

        return x
    def create_scenario_data(self, data_id):
        x = HydraIface.Dataset()
        x.db.data_id = data_id
        x.db.data_type = "scalar"
        x.db.data_units = "metres cubed"
        x.db.data_name  = "output"
        x.db.data_dimen = "metres cubed"
        x.load_all()
        x.set_hash(x.get_val())
        x.save()
        x.commit()
        return x


    def get_ordinal_timestamp(self, ts_time):
        # Convert time to Gregorian ordinal (1 = January 1st, year 1)
        ordinal_ts_time = ts_time.toordinal()
        fraction = (ts_time -
                    datetime.datetime(ts_time.year,
                                      ts_time.month,
                                      ts_time.day,
                                      0, 0, 0)).total_seconds()
        fraction = fraction / (86400)
        ordinal_ts_time += fraction

        return ordinal_ts_time

def run():
   # hydra_logging.init(level='DEBUG')
   # HydraIface.init(hdb.connect())
    unittest.main()
   # hdb.disconnect()
   # hydra_logging.shutdown()

if __name__ == "__main__":
    run() # run all tests
