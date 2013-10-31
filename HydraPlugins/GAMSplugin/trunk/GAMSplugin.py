#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

Command line options:

--start-time
--end-time

--group-by-attribute (to create subsets)

--export-results-to-excel

"""

class GAMSplugin(object):
    pass


def commandline_parser():
    parser = ap.ArgumentParser(
        description="""Import a network saved in a set of CSV files into Hydra.

Written by Philipp Meier <philipp@diemeiers.ch>
(c) Copyright 2013, University College London.
        """, epilog="For more information visit www.hydra-network.com",
        formatter_class=ap.RawDescriptionHelpFormatter)
