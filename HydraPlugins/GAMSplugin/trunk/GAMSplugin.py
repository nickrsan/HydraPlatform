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

    def gams_names_for_links(self):
        """Add a string to each link that can be used directly in GAMS code in
        order to define a link."""
        for i, link in enumerate(self.links):
            self.links[i].gams_name = link.from_node + ' . ' + link.to_node

    def ebsd_names_for_links(self):
        """Add a string to each link that can be used directly in EBSD models
        in order to define a link."""
        for i, link in enumerate(self.links):
            self.links[i].ebsd_name = link.from_node + ' . ' + \
                link.name + ' . ' + link.to_node


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
