#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides facilities for unit conversion and consistency checking
between units and dimensions.
"""

import os
from ConfigParser import NoSectionError

from util import load_config
from lxml import etree
from HydraLib import hydra_logging
import logging

hydra_logging.init(level='INFO')


class Units(object):
    """
    This class provides functionality for unit conversion and checking of
    consistency between units and dimensions. Unit conversion factors are
    defined in a static built-in XML file and in a custom file defined by
    the user. The location of the unit conversion file provided by the user
    is specified in the config file in section [unit conversion]. This section
    and a file specifying custom unit conversion factors are optional.
    """

    unittree = None
    dimensions = dict()
    units = dict()

    def __init__(self):
        try:
            config = load_config()
            builtin_unitfile = \
                os.path.join(os.path.dirname(os.path.realpath(__file__)),
                             'static',
                             'unit_definitions.xml')
            user_unitfile = config.get('unit conversion', 'file')
        except NoSectionError:
            user_unitfile = None

        with open(builtin_unitfile) as f:
            self.unittree = etree.parse(f).getroot()

        if user_unitfile is not None:
            try:
                with open(user_unitfile) as f:
                    usertree = etree.parse(f).getroot()
                    for element in usertree:
                        self.unittree.append(element)
            except IOError:
                logging.info("Custom unit conversion file '%s' does not exist."
                             % user_unitfile)

        for element in self.unittree:
            dimension = element.get('name')
            self.dimensions.update({dimension: []})
            for unit in element:
                self.dimensions[dimension].append(unit.get('abbr'))
                self.units.update({unit.get('abbr'):
                                   (float(unit.get('lf')),
                                    float(unit.get('cf')))})

    def check_consistency(self, unit, dimension):
        """Check whether a specified unit is consistent with the physical
        dimension asked for by the attribute or the dataset.
        """
        unit, factor = self.parse_unit(unit)
        return unit in self.dimensions['dimension']

    def get_dimension(self, unit):
        """Return the physical dimension a given unit refers to.
        """

        unit, factor = self.parse_unit(unit)
        for dim in self.dimensions.keys():
            if unit in self.dimensions[dim]:
                return dim

    def convert(self, value, unit1, unit2):
        """Convert
        """
        unit1, factor1 = self.parse_unit(unit1)
        unit2, factor2 = self.parse_unit(unit2)
        conv_factor1 = self.units[unit1]
        conv_factor2 = self.units[unit2]

        return (conv_factor1[0] / conv_factor2[0] * (factor1 * value) \
            + (conv_factor1[1] - conv_factor2[1]) / conv_factor2[0]) / \
            factor2

    def parse_unit(self, unit):
        """
        """
        try:
            float(unit[0])
            factor, unit = unit.split()
            return unit, float(factor)
        except ValueError:
            return unit, 1.0


if __name__ == '__main__':
    units = Units()
    for dim in units.unittree:
        print '**' + dim.get('name') + '**'
        for unit in dim:
            print unit.get('name'), unit.get('abbr'), unit.get('lf'), \
                unit.get('cf'), unit.get('info')

    print units.convert(200, 'm^3', 'ac-ft')
