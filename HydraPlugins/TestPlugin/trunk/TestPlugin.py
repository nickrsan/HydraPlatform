#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A Hydra plug-in for importing CSV files.

Basics
~~~~~~

Basic usage::

       ImportCSV.py [-h] [-n NETWORK]

Options
~~~~~~~

====================== ====== ========= =======================================
Option                 Short  Parameter Description
====================== ====== ========= =======================================
``--help``             ``-h``           show help message and exit.
``--network_id``       ``-n`` NETWORK   Specify the network ID to check
===================== ====== ========= =======================================
"""

from HydraLib import PluginLib

import argparse as ap
from datetime import datetime


def commandline_parser():
    parser = ap.ArgumentParser(
        description="""Return the nodes and links of a network in hydra.

Written by Stephen Knox <s.knox@ucl.ac.uk>
(c) Copyright 2013, University College London.
        """, epilog="For more information visit www.hydra-network.com",
        formatter_class=ap.RawDescriptionHelpFormatter)
    parser.add_argument('-n', '--network_id',
                        help='''Specify the ID of the network
                        to retrieve''')
    return parser
 
def run(network_id):

    client = PluginLib.connect()
    
 
    errors   = []
    warnings = []
    message = "The network is not there I'm afraid."
    start = datetime.now()
    test = client.service.test_get_all_node_data(16, 8)
    print "Call took %s"%(datetime.now()-start)
    #try:
     #   network = client.service.get_network(int(network_id))
    #    message = "Yup, network is definitely there."
    #except Exception, e:
    #    errors.append(e)    

    #xml_result = PluginLib.create_xml_response('Test Plugin', network_id, errors, warnings, message) 

    #PluginLib.write_xml_result('Test Plugin', xml_result)

    print "Finished!"
    

if __name__ == "__main__":
    parser = commandline_parser()
    args = parser.parse_args()

    if args.network_id is not None:
        run(args.network_id)
    else:
        print "ERROR!: %s is not correct"%(args.network_id)
