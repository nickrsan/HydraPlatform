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

class NetworkTest(test_HydraIface.HydraIfaceTest):

    def test_update(self):
        proj = self.create_project("test proj")
        x = HydraIface.Network()
        x.db.network_name = "test"
        x.db.network_description = "test description"
        x.db.project_id = proj.db.project_id
        x.save()
        x.commit()

        x.db.network_name = "test_new"
        x.save()
        x.commit()
        assert x.db.network_name == "test_new", "Network did not update correctly"

    def test_delete(self):
        proj = self.create_project("test proj")
        x = HydraIface.Network()
        x.db.network_name = "test"
        x.db.network_description = "test description"
        x.db.project_id = proj.db.project_id
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        proj = self.create_project("test proj")
        x = HydraIface.Network()
        x.db.network_name = "test"
        x.db.network_description = "test description"
        x.db.project_id = proj.db.project_id
        x.save()
        x.commit()
        y = HydraIface.Network(network_id=x.db.network_id)
        assert y.load() == True, "Load did not work correctly"

class LinkTest(test_HydraIface.HydraIfaceTest):

    def test_update(self):
        proj = self.create_project("Test Proj)")
        net = self.create_network("Test Net", proj.db.project_id)
        node_a = self.create_node("Node A", net.db.network_id)
        node_b = self.create_node("Node B", net.db.network_id)
        x = HydraIface.Link()
        x.db.link_name = "test"
        x.db.link_description = "test description"
        x.db.network_id = net.db.network_id
        x.db.node_1_id = node_a.db.node_id
        x.db.node_2_id = node_b.db.node_id
        x.save()
        x.commit()

        x.db.link_name = "test_new"
        x.save()
        x.commit()
        assert x.db.link_name == "test_new", "Link did not update correctly"

    def test_delete(self):
        proj = self.create_project("Test Proj)")
        net = self.create_network("Test Net", proj.db.project_id)
        node_a = self.create_node("Node A", net.db.network_id)
        node_b = self.create_node("Node B", net.db.network_id)
        x = HydraIface.Link()
        x.db.link_name = "test"
        x.db.link_description = "test description"
        x.db.network_id = net.db.network_id
        x.db.node_1_id = node_a.db.node_id
        x.db.node_2_id = node_b.db.node_id
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        proj = self.create_project("Test Proj)")
        net = self.create_network("Test Net", proj.db.project_id)
        node_a = self.create_node("Node A", net.db.network_id)
        node_b = self.create_node("Node B", net.db.network_id)
        node_c = self.create_node("Node C", net.db.network_id)
        x = HydraIface.Link()
        x.db.link_name = "test"
        x.db.link_description = "test description"
        x.db.network_id = net.db.network_id
        x.db.node_1_id = node_a.db.node_id
        x.db.node_2_id = node_b.db.node_id
        x.save()
        x.commit()

        z = HydraIface.Link()
        z.db.link_name = "test1"
        z.db.link_description = "test description 1"
        z.db.network_id = net.db.network_id
        z.db.node_1_id = node_a.db.node_id
        z.db.node_2_id = node_c.db.node_id
        z.save()
        z.commit()

        net.load()

        y = HydraIface.Link(link_id=x.db.link_id)
        assert y.load() == True, "Load did not work correctly"

class NodeTest(test_HydraIface.HydraIfaceTest):

    def test_update(self):
        proj = self.create_project("Test Proj)")
        net = self.create_network("Test Net", proj.db.project_id)
        x = HydraIface.Node()
        x.db.node_name = "test"
        x.db.node_description = "test description"
        x.db.network_id = net.db.network_id
        x.save()
        x.commit()

        x.db.node_name = "test_new"
        x.save()
        x.commit()
        x.load()
        assert x.db.node_name == "test_new", "Node did not update correctly"


    def test_delete(self):
        proj = self.create_project("Test Proj)")
        net = self.create_network("Test Net", proj.db.project_id)
        x = HydraIface.Node()
        x.db.node_name = "test"
        x.db.node_description = "test description"
        x.db.network_id = net.db.network_id
        x.save()
        x.commit()

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        proj = self.create_project("Test Proj)")
        net = self.create_network("Test Net", proj.db.project_id)
        x = HydraIface.Node()
        x.db.node_name = "test"
        x.db.node_description = "test description"
        x.db.network_id = net.db.network_id
        x.save()
        x.commit()
        y = HydraIface.Node(node_id=x.db.node_id)
        assert y.load() == True, "Load did not work correctly"

#class ChildrenTest(test_HydraIface.HydraIfaceTest):
#
#    def test_load(self):
#        x = HydraIface.Network(network_id=4)
#        x.load()
#        
#        l = x.links[0]
#        l.load()



if __name__ == "__main__":
    test_HydraIface.run() # run all tests
