
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
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
import logging
from suds import WebFault
log = logging.getLogger(__name__)

class NoteTest(test_SoapServer.SoapServerTest):
    def test_add_note(self):
        net = self.create_network_with_data()

        s = net['scenarios'].Scenario[0]
        node = net.nodes.Node[0]
        link = net.links.Link[0]
        grp = net.resourcegroups.ResourceGroup[0]

        note = dict(
            text    = "I am a note"
        )

        n_note = self.client.service.add_node_note(node.id, note)
        l_note = self.client.service.add_link_note(s.id, note)
        s_note = self.client.service.add_scenario_note(link.id, note)
        g_note = self.client.service.add_resourcegroup_note(grp.id, note)
        net_note = self.client.service.add_network_note(net.id, note)

        assert n_note.id is not None
        assert n_note.ref_key     == 'NODE'
        assert n_note.ref_id      == node.id
        assert n_note.text        == note['text']

        node_notes = self.client.service.get_node_notes(node.id)

        assert len(node_notes) == 1

    def test_update_note(self):
        net = self.create_network_with_data()

        node = net.nodes.Node[0]

        note = dict(
            text    = "I am a note"
        )

        new_note = self.client.service.add_node_note(node.id, note)

        new_note.text = "I am an updated note"
        updated_note = self.client.service.update_note(new_note)

        retrieved_note = self.client.service.get_note(updated_note.id)

        assert retrieved_note.text == updated_note.text == new_note.text

    def test_get_note(self):
        net = self.create_network_with_data()

        node = net.nodes.Node[0]

        note = dict(
            text    = "I am a note"
        )

        new_note = self.client.service.add_node_note(node.id, note)
        retrieved_note = self.client.service.get_note(new_note.id)
        assert str(new_note) == str(retrieved_note)

    def test_delete_note(self):
        net = self.create_network_with_data()

        node = net.nodes.Node[0]

        note = dict(
            text    = "I am a note"
        )

        new_note = self.client.service.add_node_note(node.id, note)

        retrieved_note = self.client.service.get_note(new_note.id)
        assert str(new_note) == str(retrieved_note)

        self.client.service.purge_note(new_note.id)
        node_notes = self.client.service.get_node_notes(node.id)
        assert len(node_notes) == 0
        self.assertRaises(WebFault, self.client.service.get_note, new_note.id)

if __name__ == '__main__':
    test_SoapServer.run()
