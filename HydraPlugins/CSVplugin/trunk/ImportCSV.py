#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A Hydra plug-in for importing CSV files.

Basics
~~~~~~

The plug-in for importing CSV files assumes that you have a collection of files
ready. You need one or several files for nodes (usually one file per node type)
and files for links (usually on file per link type). One node file and one link
file are mandatory, additional files are optional. The plug-in also allows you
to import network attributes.

Basic usage::

       ImportCSV.py [-h] [-p PROJECT] [-s SCENARIO] [-t NETWORK]
                    -n NODES [NODES ...] -l LINKS [LINKS ...]
                    [-r RULES [RULES ...]] [-z TIMEZONE] [-x]

Options
~~~~~~~

====================== ====== ========= =======================================
Option                 Short  Parameter Description
====================== ====== ========= =======================================
``--help``             ``-h``           show help message and exit.
``--project``          ``-p`` PROJECT   The ID of an existing project. If no
                                        project is specified or if the ID
                                        provided does not belong to an existing
                                        project, a new one will be created.
``--scenario``         ``-s`` SCENARIO  Specify the name of the scenario
                                        created by the import function. Every
                                        import creates a new scenario. If no
                                        name is provided a default name will be
                                        assigned.
``--network``          ``-t`` NETWORK   Specify the file containing network
                                        information. If no file is specified, a
                                        new network will be created using
                                        default values.
``--nodes``            ``-n`` NODES     One or multiple files containing nodes
                                        and attributes.
``--links``            ``-l`` LINKS     One or multiple files containing
                                        information on links.
``--rules``            ``-l`` RULES     File(s) containing rules or constraints
                                        as mathematical expressions.
``--timezone``         ``-z`` TIMEZONE  Specify a timezone as a string
                                        following the Area/Loctation pattern
                                        (e.g.  Europe/London). This timezone
                                        will be used for all timeseries data
                                        that is imported. If you don't specify
                                        a timezone, it defaults to UTC.
``--expand-filenames`` ``-x``           If the import function encounters
                                        something that looks like a filename,
                                        it tries to read the file.
====================== ====== ========= =======================================


File structure
~~~~~~~~~~~~~~

In the node and link file a minimum of information has to be provided in order
to be able to import a complete network. Optionally the files can define any
number of attributes for nodes and links.

For nodes a valid file looks like this::

    Name , x, y, attribute_1, attribute_2, ..., attribute_n, description
    Units,  ,  ,           m,    m^3 s^-1, ...,           -,
    node1, 2, 1,         4.0,      3421.9, ...,  Crop: corn, Irrigation node 1
    node2, 2, 3,         2.4,       988.4, ...,  Crop: rice, Irrigation node 2

For links, the following is a valid file::

    Name ,       from,       to, attribute_1, ..., attribute_n, description
    Units,           ,         ,           m, ...,    m^2 s^-1,
    link1,      node1,    node2,         453, ...,        0.34, Water transfer

It is optional to supply a network file. If you decide to do so, it needs to
follow this structure::

    # A test network created as a set of CSV files
    ID, Name            , attribute_1, ..., Description
    Units,              ,            ,    ,
    1 , My first network, test       ,    , A network create from CSV files


Lines starting with the ``#`` character are ignored.

.. note::

   If you specify a header line using the keywords ``name``, ``x``, ``y`` and
   ``description`` (``name``, ``from``, ``to`` and ``description`` for links)
   the order of the columns does not matter. If you don't specify these
   keywords, the plug-in will assume that the first column specifies the name,
   the second X, the third Y and the last the description (name, from, to and
   description for links).

Please also consider the following:

- A link references to start and end node by name. This also implies that the
  node names defined in the file need to be unique.

- The description should be in the last column. This will lead to more readable
  files, since the description is usually free form text.

- If you specify more than one file containing nodes, common attributes (i.e.
  with the same name) will be considered the same. This results in a unique
  attribute set for nodes. The same applies for links.


TODO
----

- Implement updating of existing scenario.

- Implement rules and constraints

API docs
~~~~~~~~
"""

import argparse as ap
import logging
import os
from datetime import datetime
import pytz

from HydraLib import PluginLib
from HydraLib import units

from suds import WebFault
import numpy

import re


class ImportCSV(object):
    """
    """

    Project = None
    Network = None
    Scenario = None
    Nodes = dict()
    Links = dict()
    Attributes = dict()
    update_network_flag = False
    timezone = pytz.utc
    expand_filenames = False
    unit_class = units.Units()

    basepath = ''

    def __init__(self):
        self.cli = PluginLib.connect()
        self.node_id = PluginLib.temp_ids()
        self.link_id = PluginLib.temp_ids()
        self.attr_id = PluginLib.temp_ids()

    def create_project(self, ID=None):
        if ID is not None:
            try:
                ID = int(ID)
                try:
                    self.Project = self.cli.service.get_project(ID)
                    logging.info('Loading existing project (ID=%s)' % ID)
                    return
                except WebFault:
                    logging.info('Project ID not found. Creating new project')

            except ValueError:
                logging.info('Project ID not valid. Creating new project')

        self.Project = self.cli.factory.create('hyd:Project')
        self.Project.name = "CSV import at %s" % (datetime.now())
        self.Project.description = \
            "Project created by the %s plug-in, %s." % \
            (self.__class__.__name__, datetime.now())
        self.Project = self.cli.service.add_project(self.Project)

    def create_scenario(self, name=None):
        self.Scenario = self.cli.factory.create('hyd:Scenario')
        if name is not None:
            self.Scenario.name = name
        else:
            self.Scenario.name = 'CSV import'

        self.Scenario.description = \
            'Default scenario created by the CSV import plug-in.'
        self.Scenario.id = -1

    def create_network(self, file=None):

        if file is not None:
            self.basepath = os.path.dirname(file)
            with open(file, mode='r') as csv_file:
                self.net_data = csv_file.read().split('\n')
            # Ignore comments
            for i, line in enumerate(self.net_data):
                if len(line) > 0 and line.strip()[0] == '#':
                    self.net_data.pop(i)
            keys = self.net_data[0].split(',')
            units = self.net_data[1].split(',')
            # A network file should only have one line of data
            data = self.net_data[2].split(',')

            # We assume a standard order of the network information (Name,
            # Description, attributes,...).
            id_idx = 0
            name_idx = 1
            desc_idx = 2
            # If the file does not follow the standard, we can at least try to
            # guess what is stored where.
            for i, key in enumerate(keys):
                if key.lower().strip() == 'id':
                    id_idx = i
                elif key.lower().strip() == 'name':
                    name_idx = i
                elif key.lower().strip() == 'description':
                    desc_idx = i

            try:
                ID = int(data[id_idx])
            except ValueError:
                ID = None

            if ID is not None:
                # Check if network exists on the server.
                try:
                    self.Network = self.cli.service.get_network(data[id_idx])
                    # Assign name and description in case anything has changed
                    self.Network.name = data[name_idx].strip()
                    self.Network.description = data[desc_idx].strip()
                    self.update_network_flag = True
                    logging.info('Loading existing network (ID=%s)' % ID)
                    # load existing nodes
                    for node in self.Network.nodes.Node:
                        self.Nodes.update({node.name: node})
                    # load existing links
                    for link in self.Network.links.Link:
                        self.Links.update({link.name: link})
                    # Nodes and links are now deleted from the network, they
                    # will be added later...
                    self.Network.nodes = \
                        self.cli.factory.create('hyd:NodeArray')
                    self.Network.links = \
                        self.cli.factory.create('hyd:LinkArray')
                    # The scenario loaded with the network will be deleted as
                    # well, we create a new one.
                    self.Network.scenarios = \
                        self.cli.factory.create('hyd:ScenarioArray')
                except WebFault:
                    logging.info('Network ID not found. Creating new network.')
                    ID = None

            if ID is None:
                # Create a new network
                self.Network = self.cli.factory.create('hyd:Network')
                self.Network.project_id = self.Project.id
                #self.Network.nodes = \
                #    self.cli.factory.create('hyd:NodeArray')
                #self.Network.links = \
                #    self.cli.factory.create('hyd:LinkArray')
                #self.Network.scenarios = \
                #    self.cli.factory.create('hyd:ScenarioArray')
                self.Network.name = data[name_idx].strip()
                self.Network.description = data[desc_idx].strip()

            # Everything that is not name or description is an attribute
            attrs = dict()

            for i, key in enumerate(keys):
                if i != id_idx and i != name_idx and i != desc_idx:
                    attrs.update({i: key.strip()})

            if len(attrs.keys()) > 0:
                self.Network = self.add_data(self.Network, attrs, data)

        else:
            # Create a new network
            self.Network = self.cli.factory.create('hyd:Network')
            self.Network.project_id = self.Project.id
            #self.Network.nodes = self.cli.factory.create('hyd:NodeArray')
            #self.Network.links = self.cli.factory.create('hyd:LinkArray')
            #self.Network.scenarios = \
            #    self.cli.factory.create('hyd:ScenarioArray')
            self.Network.name = "CSV import"
            self.Network.description = \
                "Network created by the %s plug-in, %s." % \
                (self.__class__.__name__, datetime.now())

    def read_nodes(self, file):
        self.basepath = os.path.dirname(file)
        with open(file, mode='r') as csv_file:
            self.node_data = csv_file.read().split('\n')
            # Ignore comments
            for i, line in enumerate(self.node_data):
                if len(line) > 0 and line.strip()[0] == '#':
                    self.node_data.pop(i)
        self.create_nodes()

    def create_nodes(self):
        keys = self.node_data[0].split(',')
        units = self.node_data[1].split(',')
        data = self.node_data[2:-1]

        name_idx = 0
        desc_idx = -1  # Should be the last one
        x_idx = 1
        y_idx = 2
        # Guess parameter position:
        for i, key in enumerate(keys):
            if key.lower().strip() == 'name':
                name_idx = i
            elif key.lower().strip() == 'description':
                desc_idx = i
            elif key.lower().strip() == 'x':
                x_idx = i
            elif key.lower().strip() == 'y':
                y_idx = i

        attrs = dict()

        for i, key in enumerate(keys):
            if i != name_idx and i != desc_idx and i != x_idx and i != y_idx:
                attrs.update({i: key.strip()})

        for line in data:
            linedata = line.split(',')
            nodename = linedata[name_idx].strip()
            if nodename in self.Nodes.keys():
                node = self.Nodes[nodename]
                logging.info('Node %s exists.' % nodename)
            else:
                node = self.cli.factory.create('hyd:Node')
                node.id = self.node_id.next()
                node.name = nodename
                node.description = linedata[desc_idx].strip()
                try:
                    node.x = float(linedata[x_idx])
                except ValueError:
                    node.x = 0
                    logging.info('X coordinate of node %s is not a number.'
                                 % node.name)
                try:
                    node.y = float(linedata[y_idx])
                except ValueError:
                    node.y = 0
                    logging.info('Y coordinate of node %s is not a number.'
                                 % node.name)

            if len(attrs) > 0:
                node = self.add_data(node, attrs, linedata, units=units)

            self.Nodes.update({node.name: node})

    def read_links(self, file):
        self.basepath = os.path.dirname(file)
        with open(file, mode='r') as csv_file:
            self.link_data = csv_file.read().split('\n')
            # Ignore comments
            for i, line in enumerate(self.link_data):
                if len(line) > 0 and line.strip()[0] == '#':
                    self.link_data.pop(i)
        self.create_links()

    def create_links(self):
        keys = self.link_data[0].split(',')
        units = self.link_data[1].split(',')
        data = self.link_data[2:-1]

        name_idx = 0
        desc_idx = -1  # Should be the last one
        from_idx = 1
        to_idx = 2
        # Guess parameter position:
        for i, key in enumerate(keys):
            if key.lower().strip() == 'name':
                name_idx = i
            elif key.lower().strip() == 'description':
                desc_idx = i
            elif key.lower().strip() == 'from':
                from_idx = i
            elif key.lower().strip() == 'to':
                to_idx = i

        attrs = dict()

        for i, key in enumerate(keys):
            if i != name_idx and i != desc_idx and \
                    i != from_idx and i != to_idx:
                attrs.update({i: key.strip()})

        for line in data:
            linedata = line.split(',')
            linkname = linedata[name_idx].strip()
            if linkname in self.Links.keys():
                link = self.Links[linkname]
                logging.info('Link %s exists.' % linkname)
            else:
                link = self.cli.factory.create('hyd:Link')
                link.id = self.link_id.next()
                link.name = linkname
                link.description = linedata[desc_idx].strip()

                try:
                    fromnode = self.Nodes[linedata[from_idx].strip()]
                    tonode = self.Nodes[linedata[to_idx].strip()]
                    link.node_1_id = fromnode.id
                    link.node_2_id = tonode.id

                except KeyError:
                    logging.info(('Start or end node not found (%s -- %s).' +
                                  ' No link created.') %
                                 (linedata[from_idx].strip(),
                                  linedata[to_idx].strip()))
            if len(attrs) > 0:
                link = self.add_data(link, attrs, linedata, units=units)
            self.Links.update({link.name: link})

    def create_attribute(self, name, unit=None):
        attribute = self.cli.factory.create('hyd:Attr')
        attribute.name = name
        if unit is not None and len(unit.strip()) > 0:
            attribute.dimen = self.unit_class.get_dimension(unit.strip())
        #attribute = self.cli.service.add_attribute(attribute)

        return attribute

    def add_data(self, resource, attrs, data, units=None):
        '''Add the data read for each resource to the resource. This requires
        creating the attributes, resource attributes and a scenario which holds
        the data.'''

        attributes = self.cli.factory.create('hyd:AttrArray')

        # Collect existing resource attributes:
        resource_attrs = dict()

        if resource.attributes is None:
            return resource

        for res_attr in resource.attributes.ResourceAttr:
            resource_attrs.update({res_attr.attr_id: res_attr})

        for i in attrs.keys():
            if attrs[i] in self.Attributes.keys():
                attribute = self.Attributes[attrs[i]]
                #attribute = self.create_attribute(attrs[i])
            else:
                if units is not None:
                    attribute = self.create_attribute(attrs[i], units[i])
                else:
                    attribute = self.create_attribute(attrs[i])
                self.Attributes.update({attrs[i]: attribute})
            attributes.Attr.append(attribute)

        # Add all attributes. If they exist already, we retrieve the real id.
        attributes = self.cli.service.add_attributes(attributes)
        for attr in attributes.Attr:
            self.Attributes.update({attr.name: attr})

        # Add data to each attribute
        for i in attrs.keys():
            attr = self.Attributes[attrs[i]]
            # Attribute might already exist for resource, use it if it does
            if attr.id in resource_attrs.keys():
                res_attr = resource_attrs[attr.id]
            else:
                res_attr = self.cli.factory.create('hyd:ResourceAttr')
                res_attr.id = self.attr_id.next()
                res_attr.attr_id = attr.id

                resource.attributes.ResourceAttr.append(res_attr)

            # create dataset and assign to attribute (if not empty)
            if len(data[i].strip()) > 0:

                if data[i].strip() in ('NULL',
                                       'I AM NOT A NUMBER! I AM A FREE MAN!'):

                    res_attr.attr_is_var = 'Y'

                else:

                    if units is not None:
                        dataset = self.create_dataset(data[i],
                                                      res_attr,
                                                      units[i],
                                                      resource.name)
                    else:
                        dataset = self.create_dataset(data[i],
                                                      res_attr,
                                                      None,
                                                      resource.name)

                    self.Scenario.resourcescenarios.ResourceScenario.append(dataset)

        #resource.attributes = res_attr_array

        return resource

    def read_constraints(self, constraint_file):
        """
            Read constraints which should be in the format:
            (ITEM op (ITEM op (ITEM op ITEM))...)
        """
        with open(constraint_file, mode='r') as csv_file:
            self.constraint_data = csv_file.read()

        constraints = self.constraint_data.split('\n')

        for constraint_str in constraints:
            if constraint_str.strip() == "":
                continue
            #parse the constraint string. that meanse:
            #1: identify the constant
            #2: identify the operation
            #3: identify the individual elements and their structure.

            split_str = constraint_str.split(' ')
            logging.info(constraint_str)
            constant = split_str[-1]
            op = split_str[-2]

            constraint = self.cli.factory.create("hyd:Constraint")
            constraint.constant = constant
            constraint.op = op

            main_group = self.get_group(constraint_str)

            group = self.parse_constraint_group(main_group)

            group_complexmodel = self.convert_to_complexmodel(group)

            constraint.constraintgroup = group_complexmodel

            self.Scenario.constraints.Constraint.append(constraint)

    def convert_to_complexmodel(self, group):

            regex = re.compile('[\[\]]')

            constraint_grp = self.cli.factory.create('hyd:ConstraintGroup')

            op = group[1]
            constraint_grp.op = op

            lhs = group[0]
            rhs = group[2]

            for grp in (lhs, rhs):
                if isinstance(grp, list):
                    grp_cm = self.convert_to_complexmodel(grp)
                    constraint_grp.constraintgroups.ConstraintGroup.append(grp_cm)
                else:

                    constraint_item = self.cli.factory.create('hyd:ConstraintItem')
                    #Check to see if the item  is a constant numeric value
                    if grp.find('[') < 0:
                        try:
                            eval(grp)
                            constraint_item.constant = grp
                        except:
                            raise Exception("Expected a constant value. Got: %s"%grp)
                    else:
                        grp_parts = regex.split(grp)
                        item_type = grp_parts[0]
                        item_name = grp_parts[1]
                        item_attr = grp_parts[3]

                        if item_type == 'NODE':
                            n = self.Nodes.get(item_name)

                            if n is None:
                                raise Exception('Node %s not found!'%(item_name))

                            for ra in n.attributes.ResourceAttr:

                                attr = self.Attributes.get(item_attr)
                                if attr is None:
                                    raise Exception('Attr %s not found!'%(item_attr))

                                if ra.attr_id == attr.id:
                                    constraint_item.resource_attr_id = ra.id

                    constraint_grp.constraintitems.ConstraintItem.append(constraint_item)

            return constraint_grp


    def parse_constraint_group(self, group_str):
        if group_str[0] == '(' and group_str[-1] == ')':
            group_str = group_str[1:-1]

        grp = [None, None, None]

        regex = re.compile('[\+\-\*\/]')

        eq = regex.split(group_str)

        lhs = None
        if group_str.startswith('('):
            lhs = self.get_group(group_str)
            grp[0] = self.parse_constraint_group(self.get_group(group_str))
        else:
            lhs = eq[0].strip()
            grp[0] = lhs

        group_str = group_str.replace(lhs, '', 1)

        op = regex.findall(group_str)[0]

        grp[1] = op

        group_str = group_str.replace(op, '').strip()

        if group_str.startswith('('):
            grp[2] = self.parse_constraint_group(self.get_group(group_str))
        else:
            grp[2] = group_str.strip()

        return grp

    def get_group(self, group_str):
        #When this count equals 0, we have reached
        #the end of the group,
        count = 0

        for i, c in enumerate(group_str):
            #found a sub-group, add 1 to count
            if c == '(':
                count = count + 1
            elif c == ')':
                #found the end of a group.
                count = count - 1
                #is the end of a sub-group or not?
                if count == 0:
                    #not, return the group.
                    return group_str[0:i+1].strip()

        return group_str


    def parse_constraint_item(self, item_str):
        return item_str

    def create_dataset(self, value, resource_attr, unit, resource_name):
        resourcescenario = self.cli.factory.create('hyd:ResourceScenario')
        dataset          = self.cli.factory.create('hyd:Dataset')

        resourcescenario.attr_id = resource_attr.attr_id
        resourcescenario.resource_attr_id = resource_attr.id

        value = value.strip()
        if unit is not None:
            unit = unit.strip()
            if len(unit) == 0:
                unit = None

        try:
            float(value)
            dataset.type = 'scalar'
            scal = self.create_scalar(value)
            dataset.value = scal
        except ValueError:
            try:
                if self.expand_filenames:
                    full_file_path = os.path.join(self.basepath, value)
                    with open(full_file_path) as f:
                        logging.info('Reading data from %s ...' % full_file_path)
                        filedata = f.read()
                    tmp_filedata = filedata.split('\n')
                    filedata = ''
                    for i, line in enumerate(tmp_filedata):
                        #The name of the resource is how to identify the data for it.
                        #Once this the correct line(s) has been identified, remove the
                        #name from the start of the line
                        if len(line) > 0 and line.strip().startswith(resource_name):
                            line = line[line.find(',')+1:]
                            filedata = filedata + line + '\n'
                        else:
                            continue
                    if self.is_timeseries(filedata):
                        dataset.type = 'timeseries'
                        ts = self.create_timeseries(filedata)
                        dataset.value = ts
                    else:
                        dataset.type = 'array'
                        arr = self.create_array(filedata)
                        dataset.value = arr
                else:
                    raise IOError
            except IOError:
                dataset.type = 'descriptor'
                desc = self.create_descriptor(value)
                dataset.value = desc

        dataset.unit = unit
        if unit is not None:
            dataset.dimension = self.unit_class.get_dimension(unit)

        dataset.name = "Import CSV data"

        resourcescenario.value = dataset

        return resourcescenario

    def create_scalar(self, value):
        scalar = self.cli.factory.create('hyd:Scalar')
        scalar.param_value = value
        return scalar

    def create_descriptor(self, value):
        descriptor = self.cli.factory.create('hyd:Descriptor')
        descriptor.desc_val = value
        return descriptor

    def create_timeseries(self, data):
        date = data.split(',', 1)[0].strip()
        timeformat = PluginLib.guess_timefmt(date)
        seasonal = False
        if 'XXXX' in timeformat:
            seasonal = True

        ts_values = []

        timedata = data.split('\n')
        for line in timedata:
            if line != '':
                dataset = line.split(',')
                tstime = datetime.strptime(dataset[0].strip(), timeformat)
                tstime = self.timezone.localize(tstime)

                ts_time = PluginLib.date_to_string(tstime,
                                                          seasonal=seasonal)

                value_length = len(dataset[2:])

                if dataset[1] != '':
                    array_shape = tuple([int(a) for a in
                                         dataset[1].strip().split(" ")])
                else:
                    array_shape = value_length

                ts_value = []
                for i in range(value_length):
                    ts_value.append(float(dataset[i + 2].strip()))

                ts_arr = numpy.array(ts_value)
                ts_arr = numpy.reshape(ts_arr, array_shape)


                ts_values.append({
                    'ts_time' : ts_time,
                    'ts_value' : str(ts_arr.tolist()),
                    })

        timeseries = {'ts_values' : ts_values}

        return timeseries

    def create_array(self, data):
        #Split the line into a list
        dataset = data.split(',')
        #First column is always the array dimensions
        arr_shape = dataset[0]
        #The actual data is everything after column 0
        dataset = [eval(d) for d in dataset[1:]]

        #If the dimensions are not set, we assume the array is 1D
        if arr_shape != '':
            array_shape = tuple([int(a) for a in arr_shape.strip().split(" ")])
        else:
            array_shape = len(dataset)

        #Reshape the array back to its correct dimensions
        array = numpy.array(dataset)
        array = numpy.reshape(array, array_shape)

        arr = self.cli.factory.create('hyd:Array')
        arr.arr_data = str(array.tolist())

        return arr

    def is_timeseries(self, data):
        date = data.split(',', 1)[0].strip()
        timeformat = PluginLib.guess_timefmt(date)
        if timeformat is None:
            return False
        else:
            return True

    def commit(self):
        for node in self.Nodes.values():
            self.Network.nodes.Node.append(node)
        for link in self.Links.values():
            self.Network.links.Link.append(link)
        self.Network.scenarios.Scenario.append(self.Scenario)

        if self.update_network_flag:
            self.Network = self.cli.service.update_network(self.Network)
        else:
            self.Network = self.cli.service.add_network(self.Network)

        logging.info("Network updated. Network ID is %s", self.Network.id)
        logging.info("Network Scenarios are: %s", \
                     [s.id for s in self.Network.scenarios.Scenario])

    def return_xml(self):
        """This is a fist version of a possible XML output.
        """
        from lxml import etree
        # create xml
        root = etree.Element("plugin_result")
        name = etree.SubElement(root, "plugin_name")
        name.text = "ImportCSV"
        mess = etree.SubElement(root, "message")
        mess.text = "Import CSV completed successfully."
        net_id = etree.SubElement(root, "network_id")
        net_id.text = str(self.Network.id)
        for s in self.Network.scenarios.Scenario:
            scen_id = etree.SubElement(root, "scenario_id")
            scen_id.text = str(s.id)

        root.append(etree.Element("errors"))
        root.append(etree.Element("warnings"))
        root.append(etree.Element("files"))

        # validate xml

        # return xml
        print etree.tostring(root, pretty_print=True)


def commandline_parser():
    parser = ap.ArgumentParser(
        description="""Import a network saved in a set of CSV files into Hydra.

Written by Philipp Meier <philipp@diemeiers.ch>
(c) Copyright 2013, University College London.

        """, epilog="For more information visit www.hydra-network.com",
        formatter_class=ap.RawDescriptionHelpFormatter)
    parser.add_argument('-p', '--project',
                        help='''The ID of an existing project. If no project is
                        specified or if the ID provided does not belong to an
                        existing project, a new one will be created.''')
    parser.add_argument('-s', '--scenario',
                        help='''Specify the name of the scenario created by the
                        import function. Every import creates a new scenario.
                        If no name is provided a default name will be assigned.
                        ''')
    parser.add_argument('-t', '--network',
                        help='''Specify the file containing network
                        information. If no file is specified, a new network
                        will be created using default values.''')
    parser.add_argument('-n', '--nodes', nargs='+',
                        help='''One or multiple files containing nodes and
                        attributes.''')
    parser.add_argument('-l', '--links', nargs='+',
                        help='''One or multiple files containing information
                        on links.''')
    parser.add_argument('-r', '--rules', nargs='+',
                        help='''File(s) containing rules or constraints as
                        mathematical expressions.''')
    parser.add_argument('-z', '--timezone',
                        help='''Specify a timezone as a string following the
                        Area/Location pattern (e.g. Europe/London). This
                        timezone will be used for all timeseries data that is
                        imported. If you don't specify a timezone, it defaults
                        to UTC.''')
    parser.add_argument('-x', '--expand-filenames', action='store_true',
                        help='''If the import function encounters something
                        that looks like a filename, it tries to read the file.
                        It also tries to guess if it contains a number, a
                        descriptor, an array or a time series.''')
    return parser


if __name__ == '__main__':
    parser = commandline_parser()
    args = parser.parse_args()
    csv = ImportCSV()

    if args.expand_filenames:
        csv.expand_filenames = True

    if args.timezone is not None:
        csv.timezone = pytz.timezone(args.timezone)

    if args.nodes is not None:
        # Create project and network only when there is actual data to import.
        csv.create_project(ID=args.project)
        csv.create_scenario(name=args.scenario)
        csv.create_network(file=args.network)

        for nodefile in args.nodes:
            csv.read_nodes(nodefile)

        if args.links is not None:
            for linkfile in args.links:
                csv.read_links(linkfile)

        if args.rules is not None:
            for constraintfile in args.rules:
                csv.read_constraints(constraintfile)

        csv.commit()
        csv.return_xml()

    else:
        logging.info('No nodes found. Nothing imported.')
