#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A Hydra plug-in for importing CSV files.

Basics
~~~~~~

There are two ways of importing network information stored in CSV files:

#. All the information is stored in one single structured file.

   Basic usage::

       ./ImportCSV.py [OPTIONS] <filename>


#. The network information is stored in different files.

   Basic usage::

       ./ImportCSV.py [OPTIONS] --nodes=<nodefile> --links=<linkfile> \\
               --rules=<rulefile>

Options
~~~~~~~

File structure
~~~~~~~~~~~~~~

If you decide to use one single structured file it has to follow a certain
structure. The different sections need to be divided by keywords. If you import
from separate files this structure keywords are not needed. Each keyword can
occur once or multiple times. It has to be one of the following::

    [nodes]
    [links]
    [rules]

In each section a minimum of information has to be provided in order to be able
to import a complete network. Optionally the file can define any number of
attributes for nodes and links.

For nodes::

    Name, x, y, description, attribute_1, attribute_2, ...

For links::

    Name, start_node, end_node, attribute_1, attribute_2, ...

The nodes a link is connecting to need to be referenced by name. This also
implies that the node names defined in the file need to be unique.

"""

import sys
import os

import PluginLib


class ImportCSV(object):
    """"""

    def __init__(self, filename):
        self.cli = PluginLib.connect()
        with open(filename, mode='r') as csv_file:
            self.data = csv_file.read()

    def read_nodes(self):
        pass

    def read_links(self):
        pass

    def read_attributes(self):
        pass

    def read_constraints(self):
        pass


if __name__ == '__main__':
    csv = ImportCSV(sys.argv[1])
