import sys
if "../../" not in sys.path:
    sys.path.append("../../")
if "../../../../../HydraLib/trunk/" not in sys.path:
    sys.path.append("../../../../../HydraLib/trunk/")
import unittest
from db import HydraIface
from HydraLib import hydra_logging, hdb
import logging

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
        x.db.project_name = name
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
        x = HydraIface.ScenarioData()
        x.db.data_id = data_id
        x.db.data_type = "double"
        x.db.data_units = "metres cubed"
        x.db.data_name  = "output"
        x.db.data_dim   = "metres cubed"
        x.save()
        x.commit()
        return x



def run():
   # hydra_logging.init(level='DEBUG')
   # HydraIface.init(hdb.connect())
    unittest.main()
   # hdb.disconnect()
   # hydra_logging.shutdown()

if __name__ == "__main__":
    run() # run all tests
