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

        print self.client.last_received()

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
        logging.info("************NETWORK****************\n")
       
        scenario.target_dir = os.path.join(network.network_dir, scenario.name.replace(' ', '_'))

        if not os.path.exists(scenario.target_dir):
            os.mkdir(scenario.target_dir)

        network_file = open(os.path.join(scenario.target_dir, "network.csv"), 'w')

        network_attribute_ids, network_attribute_names = self.get_resource_attributes([network])
       
        network_attributes_string = ""
        if len(network_attribute_names) > 0:
            network_attributes_string = ',%s'%(','.join(network_attribute_names))

        network_heading   = "ID, Description, Name %s\n" % (network_attributes_string)

        network_attr_units = []
        for attr_id in network_attribute_ids:
            network_attr_units.append(self.get_attr_unit(scenario, attr_id))

        network_units_heading  = "Units,,,%s\n"%(','.join(network_attr_units))

        attrs = [attr_id for attr_id in network_attribute_ids]
        values = ["" for attr_id in network_attribute_ids]
        if network.attributes is not None:
            for r_attr in network.attributes.ResourceAttr:
                value = self.get_attr_value(scenario, r_attr)
                values[attrs.index(r_attr.id)] = value
        print network 
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

        if network.links:
            self.export_links(scenario, network.links.Link, node_map)
        else:
            logging.warning("Network has no links!")

    def export_nodes(self, scenario, nodes):
        logging.info("************NODES****************\n")

        #return this so that the link export can easily access 
        #the names of the links.
        id_name_map = dict()

        #For simplicity, export to a single node & link file.
        #We assume here that fewer files is simpler.
        node_file = open(os.path.join(scenario.target_dir, "nodes.csv"), 'w')

        node_attribute_ids, node_attribute_names = self.get_resource_attributes(nodes)
       
        node_attributes_string = ""
        if len(node_attribute_names) > 0:
            node_attributes_string = ',%s'%(','.join(node_attribute_names))

        node_heading   = "Name, x, y %s, description\n"%(node_attributes_string)

        node_attr_units = []
        for attr_id in node_attribute_ids:
            node_attr_units.append(self.get_attr_unit(scenario, attr_id))

        node_units_heading  = "Units,,,%s\n"%(','.join(node_attr_units) if node_attr_units else ',')

        node_entries = []
        for node in nodes:

            id_name_map[node.id] = node.name

            attrs = [attr_id for attr_id in node_attribute_ids]
            
            values = ["" for attr_id in node_attribute_ids]
            
            if node.attributes is not None:
                for r_attr in node.attributes.ResourceAttr:
                    value = self.get_attr_value(scenario, r_attr)
                    values[attrs.index(r_attr.id)] = value
            
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
        logging.info("\n\n************LINKS****************")

        #For simplicity, export to a single link file.
        #We assume here that fewer files is simpler.
        link_file = open(os.path.join(scenario.target_dir, "links.csv"), 'w')

        link_attribute_ids, link_attribute_names = self.get_resource_attributes(links)
       
        link_attributes_string = ""
        if len(link_attribute_names) > 0:
            link_attributes_string = ',%s'%(','.join(link_attribute_names))

        link_heading   = "Name, from, to %s, description\n" % (link_attributes_string)


        link_attr_units = []
        for attr_id in link_attribute_ids:
            link_attr_units.append(self.get_attr_unit(scenario, attr_id))

        link_units_heading  = "Units,,,%s\n"%(','.join(link_attr_units) if link_attr_units else ',')

        link_entries = []
        for link in links:
            attrs = [attr_id for attr_id in link_attribute_ids]
            values = ["" for attr_id in link_attribute_ids]
            if link.attributes is not None:
                for r_attr in link.attributes.ResourceAttr:
                    value = self.get_attr_value(scenario, r_attr)
                    values[attrs.index(r_attr.id)] = value

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
        logging.info("\n\n************CONSTRAINTS****************")

        if scenario.constraints is not None:
            constraint_file = open(os.path.join(scenario.target_dir, "constraints.csv"), 'w')
            for constraint in scenario.constraints.Constraint:
                constraint_line = "%s %s %s\n" % (
                    constraint.value, constraint.op, constraint.constant)

                constraint_file.write(constraint_line)

            constraint_file.close()

    def get_resource_attributes(self, resources):
        #get every attribute across every resource
        attribute_names = []
        resource_attribute_ids = []
        for resource in resources:
            if resource.attributes is not None:
                for r_attr in resource.attributes.ResourceAttr:
                    attribute_names.append(self.attributes[r_attr.attr_id])
                    resource_attribute_ids.append(r_attr.id)
        return resource_attribute_ids, attribute_names

    def get_attr_unit(self, scenario, r_attr_id):
        for rs in scenario.resourcescenarios.ResourceScenario:
            if rs.resource_attr_id == r_attr_id:
                if rs.value.unit is not None:
                    return rs.value.unit

        logging.warning("Unit not found in scenario %s for resource attr: %s"%(scenario.id, r_attr_id))
        return 'NULL'

    def get_attr_value(self, scenario, resource_attr):
        r_attr_id = resource_attr.id

        if resource_attr.attr_is_var == 'Y':
            return 'NULL'

        for rs in scenario.resourcescenarios.ResourceScenario:
            if rs.resource_attr_id == r_attr_id:
                if rs.value.type == 'descriptor':
                    value = rs.value.value.desc_val
                elif rs.value.type == 'array':
                    value = rs.value.value.arr_data
                    file_name = "array_%s.csv"%(rs.value.name) 
                    ts_file = open(os.path.join(scenario.target_dir, file_name), 'w')
                    np_val = numpy.array(eval(value.__repr__()))
                    shape = np_val.shape
                    n = 1
                    shape_str = []
                    for x in shape:
                        n = n * x
                        shape_str.append(str(x))
                    one_dimensional_val = np_val.reshape(1, n)
                    ts_file.write("%s,%s\n"%
                                (
                                    ' '.join(shape_str), 
                                    ','.join([str(x) for x in one_dimensional_val.tolist()[0]]))
                                 )
                    
                    ts_file.close()
                    value = file_name
                elif rs.value.type == 'scalar':
                    value = rs.value.value.param_value
                elif rs.value.type == 'timeseries':
                    value = rs.value.value.ts_values
                    file_name = "timeseries_%s.csv"%(value[0].ts_time.replace('.','_')) 
                    ts_file = open(os.path.join(scenario.target_dir, file_name), 'w')
                    for ts in value:
                        ts_time = ts['ts_time']
                        ts_val  = ts['ts_value']
                        np_val = numpy.array(eval(ts_val)[0])
                        shape = np_val.shape
                        n = 1
                        shape_str = []
                        for x in shape:
                            n = n * x
                            shape_str.append(str(x))
                        one_dimensional_val = np_val.reshape(1, n)
                        ts_file.write("%s,%s,%s\n"%
                                      (ts_time, 
                                       ' '.join(shape_str), 
                                       ','.join([str(x) for x in one_dimensional_val.tolist()[0]])))
                        
                    ts_file.close()
                    value = file_name
                elif rs.value.type == 'eqtimeseries':
                    value = rs.value.value.arr_data
                return str(value)

        raise Exception("Value not found in scenario %s for resource attr: %s"%(scenario.id, r_attr_id))

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
