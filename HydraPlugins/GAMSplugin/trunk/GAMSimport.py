#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A Hydra plug-in to export a network and a scenario to a set of files, which
can be imported into a GAMS model.

Basics
~~~~~~

The GAMS import and export plug-in provides pre- and post-processing facilities
for GAMS models. The basic idea is that this plug-in exports data and
constraints from Hydra to a text file which can be imported into an existing
GAMS model using the ``$ import`` statement. It should also provide a GAMS
script handling the output of data from GAMS to a text file. That way we can
guarantee that results from GAMS can be imported back into Hydra in a
onsistent way.

Input data for GAMS
-------------------

There are four types of parameters that can be exported: scalars, descriptors,
time series and arrays.

Constraints
-----------

Output data
-----------


Options
~~~~~~~

====================== ====== ========== ======================================
Option                 Short  Parameter  Description
====================== ====== ========== ======================================
--network              -n     NETWORK    ID of the network that will be
                                         exported.
--scenario             -s     SCENARIO   ID of the scenario that will be
                                         exported.
--output               -o     OUTPUT     Filename of the output file.
--start-date           -st    START_DATE Start date of the time period used for
                                         simulation.
--end-date             -en    END_DATE   End date of the time period used for
                                         simulation.
--time-step            -dt    TIME_STEP  Time step used for simulation.
--node-type-attr       -nt    NODE_TYPE_ATTR The name of the attribute
                                         specifying the node type.
--link-type-attr       -lt    LINK_TYPE_ATTR The name of the attribute
                                         specifying the link type.
--group-nodes-by       -gn    GROUP_ATTR Group nodes by this attribute(s).
--group_links-by       -gl    GROUP_ATTR Group links by this attribute(s).
====================== ====== ========== ======================================


API docs
~~~~~~~~
"""


class GAMSimport(object):
    pass
