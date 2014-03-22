#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A set of classes to facilitate import and export from and to GAMS.

Basics
~~~~~~

The GAMS import and export plug-in provides pre- and post-processing facilities
for GAMS models. The basic idea is that this plug-in exports data and
constraints from Hydra to a text file which can be imported into an existing
GAMS model using the ``$ import`` statement.

API docs
~~~~~~~~
"""

import os

from HydraLib.PluginLib import HydraResource
from HydraLib.PluginLib import HydraNetwork


class GAMSnetwork(HydraNetwork):

    def load(self, soap_net, soap_attrs):

        # load network
        resource_scenarios = dict()
        for res_scen in \
                soap_net.scenarios.Scenario[0].resourcescenarios.ResourceScenario:
            resource_scenarios.update({res_scen.resource_attr_id: res_scen})
        attributes = dict()
        for attr in soap_attrs.Attr:
            attributes.update({attr.id: attr})

        self.name = soap_net.name
        self.ID = soap_net.id
        self.description = soap_net.description
        self.scenario_id = soap_net.scenarios.Scenario[0].id

        if soap_net.attributes is not None:
            for res_attr in soap_net.attributes.ResourceAttr:
                if res_attr.id in resource_scenarios.keys():
                    self.add_attribute(attributes[res_attr.attr_id],
                                       res_attr,
                                       resource_scenarios[res_attr.id])
                else:
                    self.add_attribute(attributes[res_attr.attr_id],
                                       res_attr,
                                       None)

        self.set_type(soap_net.types)

        # load nodes
        for node in soap_net.nodes.Node:
            new_node = HydraResource()
            new_node.ID = node.id
            new_node.name = node.name
            new_node.gams_name = node.name
            if node.attributes is not None:
                for res_attr in node.attributes.ResourceAttr:
                    if res_attr.id in resource_scenarios.keys():
                        new_node.add_attribute(attributes[res_attr.attr_id],
                                               res_attr,
                                               resource_scenarios[res_attr.id])
                    else:
                        new_node.add_attribute(attributes[res_attr.attr_id],
                                               res_attr,
                                               None)

            new_node.set_type(node.types)
            self.add_node(new_node)
            del new_node

        # load links
        for link in soap_net.links.Link:
            new_link = GAMSlink()
            new_link.ID = link.id
            new_link.name = link.name
            new_link.from_node = self.get_node(node_id=link.node_1_id).name
            new_link.to_node = self.get_node(node_id=link.node_2_id).name
            new_link.gams_name = new_link.from_node + ' . ' + new_link.to_node
            if link.attributes is not None:
                for res_attr in link.attributes.ResourceAttr:
                    if res_attr.id in resource_scenarios.keys():
                        new_link.add_attribute(attributes[res_attr.attr_id],
                                               res_attr,
                                               resource_scenarios[res_attr.id])
                    else:
                        new_link.add_attribute(attributes[res_attr.attr_id],
                                               res_attr,
                                               None)

            new_link.set_type(link.types)
            self.add_link(new_link)


class GAMSlink(HydraResource):

    gams_name = None
    from_node = None
    to_node = None


def convert_date_to_timeindex(date):
    totalseconds = date.hour * 3600 + date.minute * 60 + date.second
    return date.toordinal() + float(totalseconds) / 86400


def arr_to_matrix(arr, dim):
    """Reshape a multidimensional array to a 2 dimensional matrix.
    """
    tmp_arr = []
    for n in range(len(dim) - 2):
        for inner in arr:
            for i in inner:
                tmp_arr.append(i)
        arr = tmp_arr
        tmp_arr = []
    return arr


def create_arr_index(dim):
    arr_idx = []
    L = 1
    for d in dim:
        L *= d

    for l in range(L):
        arr_idx.append(())

    K = 1
    for d in dim:
        L = L / d
        n = 0
        for k in range(K):
            for i in range(d):
                for l in range(L):
                    arr_idx[n] += (i,)
                    n += 1
        K = K * d

    return arr_idx


def import_gms_data(filename):
    """Read whole .gms file and expand all $ include statements found.
    """
    basepath = os.path.dirname(filename)
    gms_data = ''
    with open(filename) as f:
        while True:
            line = f.readline()
            if line == '':
                break
            sline = line.strip()
            if len(sline) > 0 and sline[0] == '$':
                lineparts = sline.split()
                if len(lineparts) > 2 and \
                        lineparts[1] == 'include':
                    line = import_gms_data(os.path.join(basepath, lineparts[2]))
                elif len(lineparts) == 2 and lineparts[0] == '$include':
                    line = import_gms_data(os.path.join(basepath, lineparts[1]))
            gms_data += line
    return gms_data
