#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A Hydra plug-in for importing CSV files.

Basics
~~~~~~

The plug-in for exporting CSV files exports a network to a collection of CSV files,
one file per node / link type, one file for network, one file for scenario.
Time series and array values will each have its own file also.

Basic usage::

       ImportCSV.py [-h] [-t NETWORK] [-z TIMEZONE]

Options
~~~~~~~

====================== ====== ========= =======================================
Option                 Short  Parameter Description
====================== ====== ========= =======================================
``--help``             ``-h``           show help message and exit.
``--network``          ``-t`` NETWORK   Specify the file containing network
                                        information. If no file is specified, a
                                        new network will be created using
                                        default values.
``--timezone``         ``-z`` TIMEZONE  Specify a timezone as a string
                                        following the Area/Loctation pattern
                                        (e.g.  Europe/London). This timezone
                                        will be used for all timeseries data
                                        that is exported. If you don't specify
                                        a timezone, it defaults to UTC.
====================== ====== ========= =======================================


File structure
~~~~~~~~~~~~~~

For nodes, the following is an example of what will be exported::

    Name , x, y, attribute_1, attribute_2, ..., attribute_n, description
    Units,  ,  ,           m,    m^3 s^-1, ...,           -,
    node1, 2, 1,         4.0,      3421.9, ...,  Crop: corn, Irrigation node 1
    node2, 2, 3,         2.4,       988.4, ...,  Crop: rice, Irrigation node 2

For links, the following will be and example of what will be exported::

    Name ,       from,       to, attribute_1, ..., attribute_n, description
    Units,           ,         ,           m, ...,    m^2 s^-1,
    link1,      node1,    node2,         453, ...,        0.34, Water transfer

The following network file will be exported:

    ID, Name            , attribute_1, ..., Description
    1 , My first network, test       ,    , A network create from CSV files

.. note::

    Add any other information here...

TODO
----

- Implement updating of existing scenario.

- Implement rules and constraints

API docs
~~~~~~~~
"""

import argparse as ap
import logging
import pytz

from HydraLib import PluginLib
from HydraLib.HydraException import HydraPluginError

import numpy
import os


class ExportCSV(object):
    """
    """

    Network = None
    Scenario = None
    timezone = pytz.utc

    def __init__(self):
        self.client = PluginLib.connect()

        all_attributes = self.client.service.get_attributes()
        self.attributes = {}
        for attr in all_attributes.Attr:
            self.attributes[attr.id] = attr.name

    def export(self, project, network, scenario):
       
        if network is not None:
            #The network ID can be specified to get the network...
            try:
                network_id = int(network)
                network = csv.client.service.get_network(network_id)
            except ValueError:
                #...or the network name can be specified, but with the project ID.
                project_id = project
                network_name = network
                network = csv.client.service.get_network_by_name(project_id, network_name)
        else:
            raise Exception("A network ID must be specified!")

        network_dir = "network_%s"%(network.id)

        if not os.path.exists(network_dir):
            os.mkdir(network_dir)

        network.network_dir = network_dir

        if network.scenarios is None:
            raise Exception("Network %s has no scenarios!"%(network))

        if scenario is not None:
            for scenario in network.scenarios.Scenario:
                if int(scenario.id) == int(args.scenario):
                    logging.info("Exporting Scenario %s"%(scenario.name))
                    csv.export_network(network, scenario)
                    csv.export_constraints(scenario)
                    break
            else:
                raise Exception("No scenario with ID %s found"%(args.scenario))
        else:
            logging.info("No Scenario specified, exporting them all!")
            for scenario in network.scenarios.Scenario:
                logging.info("Exporting Scenario %s"%(scenario.name))
                csv.export_network(network, scenario)
                csv.export_constraints(scenario)

    def export_network(self, network, scenario):
        logging.info("\n************NETWORK****************")
       
        scenario.target_dir = os.path.join(network.network_dir, scenario.name.replace(' ', '_'))

        if not os.path.exists(scenario.target_dir):
            os.mkdir(scenario.target_dir)

        network_file = open(os.path.join(scenario.target_dir, "network.csv"), 'w')

        network_attributes = self.get_resource_attributes([network])
       
        network_attributes_string = ""
        if len(network_attributes) > 0:
            network_attributes_string = ',%s'%(','.join(network_attributes.values()))

        network_heading   = "ID, Description, Name %s\n" % (network_attributes_string)

        network_attr_units = []
        for attr_id in network_attributes.keys():
            network_attr_units.append(self.get_attr_unit(scenario, attr_id))

        network_units_heading  = "Units,,,%s\n"%(','.join(network_attr_units))

        values = ["" for attr_id in network_attributes.keys()]

        if network.attributes is not None:
            for r_attr in network.attributes.ResourceAttr:
                attr_name = network_attributes[r_attr.attr_id]
                value = self.get_attr_value(scenario, r_attr, attr_name, network.name)
                values[network_attributes.keys().index(r_attr.attr_id)] = value
        
        network_entry = "%(id)s,%(description)s,%(name)s,%(values)s\n"%{
            "id"          : network.id,
            "description" : network.description,
            "name"        : network.name,
            "values"      : ",%s"%(",".join(values)) if len(values) > 0 else "",
        }

        network_file.write(network_heading)
        network_file.write(network_units_heading)
        network_file.write(network_entry)
        logging.info("networks written to file: %s", network_file.name)

        node_map = dict()

        if network.nodes:
            node_map = self.export_nodes(scenario, network.nodes.Node)
        else:
            logging.warning("Network has no nodes!")

        link_map = dict()
        if network.links:
            link_map = self.export_links(scenario, network.links.Link, node_map)
        else:
            logging.warning("Network has no links!")

        if network.resourcegroups:
            self.export_resourcegroups(scenario, network.resourcegroups.ResourceGroup, node_map, link_map)
        else:
            logging.warning("Network has no resourcegroups.")

        logging.info("Network export complete")

    def export_nodes(self, scenario, nodes):
        logging.info("\n************NODES****************")

        #return this so that the link export can easily access 
        #the names of the links.
        id_name_map = dict()

        #For simplicity, export to a single node & link file.
        #We assume here that fewer files is simpler.
        node_file = open(os.path.join(scenario.target_dir, "nodes.csv"), 'w')

        node_attributes = self.get_resource_attributes(nodes)
       
        node_attributes_string = ""
        if len(node_attributes) > 0:
            node_attributes_string = ',%s'%(','.join(node_attributes.values()))

        node_heading   = "Name, x, y %s, description\n"%(node_attributes_string)

        node_attr_units = []
        for attr_id in node_attributes.keys():
            node_attr_units.append(self.get_attr_unit(scenario, attr_id))

        node_units_heading  = "Units,,,%s\n"%(','.join(node_attr_units) if node_attr_units else ',')

        node_entries = []
        for node in nodes:

            id_name_map[node.id] = node.name

            values = ["" for attr_id in node_attributes.keys()]
            
            if node.attributes is not None:
                for r_attr in node.attributes.ResourceAttr:
                    attr_name = node_attributes[r_attr.attr_id]
                    value = self.get_attr_value(scenario, r_attr, attr_name, node.name)
                    values[node_attributes.keys().index(r_attr.attr_id)] = value
            
            node_entry = "%(name)s,%(x)s,%(y)s%(values)s,%(description)s\n"%{
                "name"        : node.name,
                "x"           : node.x,
                "y"           : node.y,
                "values"      : ",%s"%(",".join(values)) if len(values) > 0 else "",
                "description" : node.description,
            }
            node_entries.append(node_entry)

        node_file.write(node_heading)
        node_file.write(node_units_heading)
        node_file.writelines(node_entries)
        logging.info("Nodes written to file: %s", node_file.name)
        
        return id_name_map

    def export_links(self, scenario, links, node_map):
        logging.info("\n************LINKS****************")

        #return this so that the group export can easily access 
        #the names of the links.
        id_name_map = dict()

        #For simplicity, export to a single link file.
        #We assume here that fewer files is simpler.
        link_file = open(os.path.join(scenario.target_dir, "links.csv"), 'w')

        link_attributes = self.get_resource_attributes(links)
       
        link_attributes_string = ""
        if len(link_attributes) > 0:
            link_attributes_string = ',%s'%(','.join(link_attributes.values()))

        link_heading   = "Name, from, to %s, description\n" % (link_attributes_string)


        link_attr_units = []
        for attr_id in link_attributes.keys():
            link_attr_units.append(self.get_attr_unit(scenario, attr_id))

        link_units_heading  = "Units,,,%s\n"%(','.join(link_attr_units) if link_attr_units else ',')

        link_entries = []
        for link in links:
            
            id_name_map[link.id] = link.name

            values = ["" for attr_id in link_attributes.keys()]
            if link.attributes is not None:
                for r_attr in link.attributes.ResourceAttr:
                    attr_name = link_attributes[r_attr.attr_id]
                    value = self.get_attr_value(scenario, r_attr, attr_name, link.name)
                    values[link_attributes.keys().index(r_attr.attr_id)] = value

            link_entry = "%(name)s,%(from)s,%(to)s%(values)s,%(description)s\n"%{
                "name"        : link.name,
                "from"        : node_map[link.node_1_id],
                "to"          : node_map[link.node_2_id],
                "values"      : ",%s"%(",".join(values)) if len(values) > 0 else "",
                "description" : link.description,
            }
            link_entries.append(link_entry)

        link_file.write(link_heading)
        link_file.write(link_units_heading)
        link_file.writelines(link_entries)
        logging.info("Links written to file: %s", link_file.name)
        return id_name_map

    def export_resourcegroups(self, scenario, resourcegroups, node_map, link_map):
        """
            Export resource groups into two files. 
            1:groups.csv defining the group name, description and any attributes.
            2:group_members.csv defining the contents of each group for this scenario
        """
        logging.info("\n************RESOURCE GROUPS****************")

        group_file = open(os.path.join(scenario.target_dir, "groups.csv"), 'w')
        group_attributes = self.get_resource_attributes(resourcegroups)

        group_attributes_string = ""
        if len(group_attributes) > 0:
            group_attributes_string = '%s'%(','.join(group_attributes.values()))

        group_attr_units = []
        for attr_id in group_attributes.keys():
            group_attr_units.append(self.get_attr_unit(scenario, attr_id))

        group_heading   = "Name, %s, description\n" % (group_attributes_string)
        group_units_heading  = "Units,%s\n"%(','.join(group_attr_units) if group_attr_units else ',')

        group_entries = []
        id_name_map = dict()
        for group in resourcegroups:
            id_name_map[group.id] = group.name

            values = ["" for attr_id in group_attributes.keys()]
            if group.attributes is not None:
                for r_attr in group.attributes.ResourceAttr:
                    attr_name = group_attributes[r_attr.attr_id]
                    value = self.get_attr_value(scenario, r_attr, attr_name, group.name)
                    values[group_attributes.keys().index(r_attr.attr_id)] = value

            group_entry = "%(name)s,%(values)s,%(description)s\n"%{
                "name"        : group.name,
                "values"      : "%s"%(",".join(values)) if len(values) > 0 else "",
                "description" : group.description,
            }
            group_entries.append(group_entry)

        group_file.write(group_heading)
        group_file.write(group_units_heading)
        group_file.writelines(group_entries)
        logging.info("groups written to file: %s", group_file.name)

        self.export_resourcegroupitems(scenario, id_name_map, node_map, link_map)


    def export_resourcegroupitems(self, scenario, group_map, node_map, link_map):
        """
            Export the members of a group in a given scenario.
        """
        group_member_file = open(os.path.join(scenario.target_dir, "group_members.csv"), 'w')

        group_member_heading   = "Name, Type, Member\n"
        group_member_entries   = []
        for group_member in scenario.resourcegroupitems.ResourceGroupItem:
            group_name = group_map[group_member.group_id]
            member_type = group_member.ref_key
            if member_type == 'LINK':
                member_name = link_map[group_member.ref_id]
            elif member_type == 'NODE':
                member_name = node_map[group_member.ref_id]
            elif member_type == 'GROUP':
                member_name = group_map[group_member.ref_id]
            else:
                raise HydraPluginError('Unrecognised group member type: %s'%(member_type))

            group_member_str = "%(group)s, %(type)s, %(member_name)s\n" % {
                'group': group_name,
                'type' : member_type,
                'member_name' : member_name,
            }
            group_member_entries.append(group_member_str)

        group_member_file.write(group_member_heading)
        group_member_file.writelines(group_member_entries)


    def export_constraints(self, scenario):
        """
            Export the constraints in a scenario to 'constraints.csv'
            Each constraint looks like a mathematical equation, with some stuff
            on the left, an operation in the middle and a constant on the right.
            The 'stuff' on the left looks something like:
            
            (TYPE[NAME][ATTRIBUTE] op TYPE[NAME][ATTRIBUTE]...) for example:
            
            The following equation states that the sum of the flow of 
            nodes a and B must be equal to that of node c.

            ((NODE[Node A][Flow] + NODE[Node B][Flow]) - NODE[Node C][Flow]) == 0.0
        
        """
        logging.info("\n************CONSTRAINTS****************")

        if scenario.constraints is not None:
            constraint_file = open(os.path.join(scenario.target_dir, "constraints.csv"), 'w')
            for constraint in scenario.constraints.Constraint:
                constraint_line = "%s %s %s\n" % (
                    constraint.value, constraint.op, constraint.constant)

                constraint_file.write(constraint_line)

            constraint_file.close()
            logging.info("Constraints written to file: %s", constraint_file.name)

    def get_resource_attributes(self, resources):
        #get every attribute across every resource
        attributes = {}
        for resource in resources:
            if resource.attributes is not None:
                for r_attr in resource.attributes.ResourceAttr:
                    if r_attr.attr_id not in attributes.keys():
                        attr_name = self.attributes[r_attr.attr_id]
                        attributes[r_attr.attr_id] = attr_name
        return attributes 

    def get_attr_unit(self, scenario, attr_id):
        """
            Returns the unit of a given resource attribute within a scenario
        """

        for rs in scenario.resourcescenarios.ResourceScenario:
            if rs.attr_id == attr_id:
                if rs.value.unit is not None:
                    return rs.value.unit

        logging.warning("Unit not found in scenario %s for attr: %s"%(scenario.id, attr_id))
        return ''

    def get_attr_value(self, scenario, resource_attr, attr_name, resource_name):
        """
            Returns the value of a given resource attribute within a scenario
        """

        r_attr_id = resource_attr.id
        attr_id   = resource_attr.attr_id

        if resource_attr.attr_is_var == 'Y':
            return 'NULL'

        for rs in scenario.resourcescenarios.ResourceScenario:
            if rs.resource_attr_id == r_attr_id:
                if rs.value.type == 'descriptor':
                    value = rs.value.value.desc_val
                elif rs.value.type == 'array':
                    value = rs.value.value.arr_data
                    file_name = "array_%s_%s.csv"%(resource_attr.ref_key, attr_name) 
                    file_loc = os.path.join(scenario.target_dir, file_name)
                    if os.path.exists(file_loc):
                        arr_file      = open(file_loc, 'a')
                    else:
                        arr_file      = open(file_loc, 'w')
                    np_val = numpy.array(eval(repr(value)))
                    shape = np_val.shape
                    n = 1
                    shape_str = []
                    for x in shape:
                        n = n * x
                        shape_str.append(str(x))
                    one_dimensional_val = np_val.reshape(1, n)
                    arr_file.write("%s,%s,%s\n"%
                                ( 
                                    resource_name,
                                    ' '.join(shape_str), 
                                    ','.join([str(x) for x in one_dimensional_val.tolist()[0]]))
                                 )
                    
                    arr_file.close()
                    value = file_name
                elif rs.value.type == 'scalar':
                    value = rs.value.value.param_value
                elif rs.value.type == 'timeseries':
                    value = rs.value.value.ts_values
                    file_name = "timeseries_%s_%s.csv"%(resource_attr.ref_key, attr_name) 
                    file_loc = os.path.join(scenario.target_dir, file_name)
                    if os.path.exists(file_loc):
                        ts_file      = open(file_loc, 'a')
                    else:
                        ts_file      = open(file_loc, 'w')
                    
                    for ts in value:
                        ts_time = ts['ts_time']
                        ts_val  = ts['ts_value']
                        np_val = numpy.array(eval(ts_val))
                        shape = np_val.shape
                        n = 1
                        shape_str = []
                        for x in shape:
                            n = n * x
                            shape_str.append(str(x))
                        one_dimensional_val = np_val.reshape(1, n)
                        ts_file.write("%s,%s,%s,%s\n"%
                                     ( resource_name,
                                       ts_time, 
                                       ' '.join(shape_str), 
                                       ','.join([str(x) for x in one_dimensional_val.tolist()[0]])))
                        
                    ts_file.close()
                    value = file_name

                elif rs.value.type == 'eqtimeseries':
                    value = rs.value.value.arr_data
                    file_name = "eq_timeseries_%s_%s.csv"%(resource_attr.ref_key, attr_name) 
                    file_loc = os.path.join(scenario.target_dir, file_name)
                    if os.path.exists(file_loc):
                        arr_file      = open(file_loc, 'a')
                    else:
                        arr_file      = open(file_loc, 'w')
                    np_val = numpy.array(eval(value.__repr__()))
                    shape = np_val.shape
                    n = 1
                    shape_str = []
                    for x in shape:
                        n = n * x
                        shape_str.append(str(x))
                    one_dimensional_val = np_val.reshape(1, n)
                    arr_file.write("%s,%s,%s\n"%
                                ( 
                                    resource_name,
                                    rs.value.start_time,
                                    rs.value.frequency,
                                    ' '.join(shape_str), 
                                    ','.join([str(x) for x in one_dimensional_val.tolist()[0]]))
                                 )
                    
                    arr_file.close()
                    value = file_name
                return str(value)
        return ''
        #raise Exception("Value not found in scenario %s for resource attr: %s"%(scenario.id, r_attr_id))

def commandline_parser():
    parser = ap.ArgumentParser(
        description="""Export a network in Hydra to a set of CSV files.

Written by Stephen Knox <s.knox@ucl.ac.uk>
(c) Copyright 2013, University College London.
        """, epilog="For more information visit www.hydra-network.com",
        formatter_class=ap.RawDescriptionHelpFormatter)
    parser.add_argument('-p', '--project',
                        help='''Specify the ID of the project. Only necessary
                        if the network_id is unknown
                        ''')
    parser.add_argument('-t', '--network',
                        help='''Specify the network_id of the network to be exported.
                        If the network_id is not known, specify the network name. In
                        this case, a project ID must also be provided''')
    parser.add_argument('-s', '--scenario',
                        help='''Specify the ID of the scenario to be exported. If no
                        scenario is specified, all scenarios in the network will be
                        exported.
                        ''')
    parser.add_argument('-z', '--timezone',
                        help='''Specify a timezone as a string following the
                        Area/Location pattern (e.g. Europe/London). This
                        timezone will be used for all timeseries data that is
                        imported. If you don't specify a timezone, it defaults
                        to UTC.''')
    return parser


if __name__ == '__main__':
    parser = commandline_parser()
    args = parser.parse_args()
    csv = ExportCSV()

    if args.timezone is not None:
        csv.timezone = pytz.timezone(args.timezone)

    csv.export(args.project, args.network, args.scenario)

    logging.info("Export Complete.")
