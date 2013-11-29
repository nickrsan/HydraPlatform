#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Export a template from the database or from an existing network, from a node
or a link.
"""

from lxml import etree
import argparse

from HydraLib import PluginLib

from TemplateLib import HydraTemplateGroup


def template_from_node(net, node_id):
    template_node = None
    for node in net.nodes.Node:
        if node.id == node_id:
            template_node = node


def template_from_network(net, network_id):
    pass


def template_from_link(net, link_id):
    pass


def export_template(cli, template_group_id):
    pass


def commandline_parser():
    parser = argparse.ArgumentParser(
        description="""Export a template or a template group to an XML file.

Written by Philipp Meier <philipp@diemeiers.ch>
(c) Copyright 2013, University College London.
(c) Copyright 2013, University of Manchester.
        """, epilog="For more information visit www.hydra-network.com",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    # Mandatory arguments
    parser.add_argument('-g', '--group',
                        help='ID of the template group that will be exported.')
    parser.add_argument('-t', '--network',
                        help='''ID of the network from which a template group
                        will be created.''')
    parser.add_argument('-n', '--node',
                        help='''ID of the node from which a template will be
                        created.''')
    parser.add_argument('-l', '--link',
                        help='''ID of the link from which a template will be
                        created.''')
    parser.add_argument('-o', '--output',
                        help='''Filename of the output file.''')

    return parser


if __name__ == '__main__':
    parser = commandline_parser()
    args = parser.parse_args()

    cli = PluginLib.connect()
    net = cli.service.get_network()

    if args.network is not None:
        template = template_from_network(net, int(args.netwrok))
    elif args.node is not None:
        template = template_from_node(net, int(args.node))
    elif args.link is not None:
        template = template_from_link(net, int(args.link))
    else:
        pass

    template.export(args.output)
