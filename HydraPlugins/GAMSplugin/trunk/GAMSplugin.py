#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A set of classes to facilitate import and export from and to GAMS.

Basics
~~~~~~

The GAMS import and export plug-in provides pre- and post-processing facilities
for GAMS models. The basic idea is that this plug-in exports data and
constraints from Hydra to a text file which can be imported into an existing
GAMS model using the ``$ import`` statement. It should also provide a GAMS
script handling the output of data from GAMS to a text file. That way we can
guarantee that results from GAMS can be imported back into Hydra in a
onsistent way.

API docs
~~~~~~~~
"""


class GAMSresource(object):
    """
    """
    def __init__(self):
        self.name = None
        self.ID = None
        self.attributes = []
        self.groups = []
        self.object_type = ''

    def add_attribute(self, attr, res_attr, res_scen):
        attribute = GAMSattribute(attr, res_attr, res_scen)

        self.attributes.append(attribute)

    def delete_attribute(self, attribute):
        idx = self.attributes.index(attribute)
        del self.attributes[idx]

    def get_attribute(self, attr_name=None, attr_id=None):

        if attr_name is not None:
            return self._get_attr_by_name(attr_name)
        elif attr_id is not None:
            return self._get_attr_by_id(attr_id)

    def group(self, group_attr):
        attr = self._get_attr_by_name(group_attr)
        if attr is not None:
            group = attr.value.__getitem__(0)
            self.groups.append(group)
            # The attribute is used for grouping and will not be exported
            self.delete_attribute(attr)
            return group

    def set_object_type(self, type_attr):
        attr = self._get_attr_by_name(type_attr)
        self.object_type = attr.value.__getitem__(0)
        return self.object_type

    def _get_attr_by_name(self, attr_name):
        for attr in self.attributes:
            if attr.name == attr_name:
                return attr

    def _get_attr_by_id(self, attr_id):
        for attr in self.attributes:
            if attr.attr_id == attr_id:
                return attr


class GAMSnetwork(GAMSresource):
    """
    """

    description = None
    scenario_id = None
    nodes = []
    links = []
    node_types = []
    node_groups = []
    link_types = []
    link_groups = []

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
            for res_attr in soap_net.attributes.Attr:
                self.add_attribute(attributes[res_attr.attr_id],
                                   res_attr,
                                   resource_scenarios[res_attr.id])

        # load nodes
        for node in soap_net.nodes.Node:
            new_node = GAMSnode()
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
                    new_link.add_attribute(attributes[res_attr.attr_id],
                                           res_attr,
                                           resource_scenarios[res_attr.id])
            self.add_link(new_link)

    def add_node(self, node):
        self.nodes.append(node)

    def delete_node(self, node):
        pass

    def get_node(self, node_name=None, node_id=None, node_type=None, group=None):
        if node_name is not None:
            return self._get_node_by_name(node_name)
        elif node_id is not None:
            return self._get_node_by_id(node_id)
        elif node_type is not None:
            return self._get_node_by_type(node_type)
        elif group is not None:
            return self._get_node_by_group(group)

    def add_link(self, link):
        self.links.append(link)

    def delete_link(self, link):
        pass

    def get_link(self, link_name=None, link_id=None, link_type=None, group=None):
        if link_name is not None:
            return self._get_link_by_name(link_name)
        elif link_id is not None:
            return self._get_link_by_id(link_id)
        elif link_type is not None:
            return self._get_link_by_type(link_type)
        elif group is not None:
            return self._get_link_by_group(group)

    def set_node_type(self, attr_name):
        for i, node in enumerate(self.nodes):
            object_type = self.nodes[i].set_object_type(attr_name)
            attr = self.nodes[i].get_attribute(attr_name=attr_name)
            self.nodes[i].delete_attribute(attr)
            if object_type not in self.node_types:
                self.node_types.append(object_type)

    def set_link_type(self, attr_name):
        for i, link in enumerate(self.links):
            object_type = self.links[i].set_object_type(attr_name)
            attr = self.links[i].get_attribute(attr_name=attr_name)
            self.links[i].delete_attribute(attr)
            if object_type not in self.link_types:
                self.link_types.append(object_type)

    def create_node_groups(self, group_attr):
        for i, node in enumerate(self.nodes):
            group = self.nodes[i].group(group_attr)
            if group is not None and group not in self.node_groups:
                self.node_groups.append(group)

    def create_link_groups(self, group_attr):
        for i, link in enumerate(self.links):
            group = self.links[i].group(group_attr)
            if group is not None and group not in self.link_groups:
                self.link_groups.append(group)

    def _get_node_by_name(self, name):
        for node in self.nodes:
            if node.name == name:
                return node

    def _get_node_by_id(self, ID):
        for node in self.nodes:
            if node.ID == ID:
                return node

    def _get_node_by_type(self, node_type):
        nodes = []
        for node in self.nodes:
            if node.object_type == node_type:
                nodes.append(node)
        return nodes

    def _get_node_by_group(self, node_group):
        nodes = []
        for node in self.nodes:
            if node_group not in node.groups:
                nodes.append(node)
        return nodes

    def _get_link_by_name(self, name):
        for link in self.links:
            if link.name == name:
                return link

    def _get_link_by_id(self, ID):
        for link in self.links:
            if link.ID == ID:
                return link

    def _get_link_by_type(self, link_type):
        links = []
        for link in self.links:
            if link.object_type == link_type:
                links.append(link)
        return links

    def _get_link_by_group(self, link_group):
        links = []
        for link in self.links:
            if link_group not in link.groups:
                links.append(link)
        return links


class GAMSnode(GAMSresource):
    pass


class GAMSlink(GAMSresource):

    gams_name = None
    from_node = None
    to_node = None


class GAMSattribute(object):

    name = None

    attr_id = None
    resource_attr_id = None
    is_var = False

    dataset_id = None
    dataset_type = ''

    value = None

    def __init__(self, attr, res_attr, res_scen):
        self.name = attr.name
        self.attr_id = attr.id
        self.resource_attr_id = res_attr.id
        if res_scen is None:
            self.is_var = True
        else:
            self.dataset_id = res_scen.value.id
            self.dataset_type = res_scen.value.type
            self.value = res_scen.value.value


def convert_date_to_timeindex(date):
    totalseconds = date.hour * 3600 + date.minute * 60 + date.second
    return date.toordinal() + float(totalseconds) / 86400


def array_dim(arr):
    """Return the size of a multidimansional array.
    """
    dim = []
    while True:
        try:
            dim.append(len(arr))
            arr = arr[0]
        except TypeError:
            return dim


def arr_to_vector(arr, dim):
    """Reshape a multidimensional array to a vector.
    """
    tmp_arr = []
    for n in range(len(dim) - 1):
        for inner in arr:
            for i in inner:
                tmp_arr.append(i)
        arr = tmp_arr
        tmp_arr = []
    return arr


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
