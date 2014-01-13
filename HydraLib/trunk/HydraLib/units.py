#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""This module provides facilities for unit conversion and consistency checking
between units and dimensions.
"""

import os
from copy import deepcopy
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
    is specified in the config file in section ``[unit conversion]``. This
    section and a file specifying custom unit conversion factors are optional.
    """

    unittree = None
    usertree = None
    dimensions = dict()
    units = dict()
    userunits = []
    userdimensions = []
    static_dimensions = []
    unit_description = dict()

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

        for element in self.unittree:
            self.static_dimensions.append(element.get('name'))

        if user_unitfile is not None:
            try:
                with open(user_unitfile) as f:
                    self.usertree = etree.parse(f).getroot()
                for element in self.usertree:
                    self.unittree.append(deepcopy(element))
                    self.userdimensions.append(element.get('name'))
                    for subelement in element:
                        self.userunits.append(subelement.get('abbr'))
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
                self.unit_description.update({unit.get('abbr'):
                                              unit.get('name')})

    def check_consistency(self, unit, dimension):
        """Check whether a specified unit is consistent with the physical
        dimension asked for by the attribute or the dataset.
        """
        unit, factor = self.parse_unit(unit)
        return unit in self.dimensions[dimension]

    def get_dimension(self, unit):
        """Return the physical dimension a given unit refers to.
        """

        unit, factor = self.parse_unit(unit)
        for dim in self.dimensions.keys():
            if unit in self.dimensions[dim]:
                return dim

    def convert(self, value, unit1, unit2):
        """Convert a value from one unit to another one. The two units must
        represent the same physical dimension.
        """
        if self.get_dimension(unit1) == self.get_dimension(unit2):
            unit1, factor1 = self.parse_unit(unit1)
            unit2, factor2 = self.parse_unit(unit2)
            conv_factor1 = self.units[unit1]
            conv_factor2 = self.units[unit2]

            return (conv_factor1[0] / conv_factor2[0] * (factor1 * value)
                    + (conv_factor1[1] - conv_factor2[1]) / conv_factor2[0]) /\
                factor2
        else:
            logging.info("Unit conversion: dimensions are not consistent.")

    def parse_unit(self, unit):
        """Helper function that extracts constant factors from unit
        specifications. This allows to specify units similar to this: 10^6 m^3.
        """
        try:
            float(unit[0])
            factor, unit = unit.split()
            return unit, float(factor)
        except ValueError:
            return unit, 1.0

    def get_dimensions(self):
        """Get a list of all dimenstions listed in one of the xml files.
        """
        return self.dimensions.keys()

    def get_units(self, dimension):
        """Get a list of all units describing one specific dimension.
        """
        unitlist = []
        for unit in self.dimensions[dimension]:
            unitdict = dict()
            unitdict.update({'abbr': unit})
            unitdict.update({'name': self.unit_description[unit]})
            unitdict.update({'lf': self.units[unit][0]})
            unitdict.update({'cf': self.units[unit][1]})
            unitdict.update({'dimension': dimension})
            unitlist.append(unitdict)
        return unitlist

    def add_dimension(self, dimension):
        """Add a dimension to the custom xml file as listed in the config file.
        """
        if dimension not in self.dimensions.keys():
            self.usertree.append(etree.Element('dimension', name=dimension))
            self.dimensions.update({dimension: []})
            self.userdimensions.append(dimension)
            return True
        else:
            return False

    def delete_dimension(self, dimension):
        """Delete a dimension from the custom XML file.
        """
        if dimension in self.userdimensions:
            # Delete units from the dimension
            for unit in self.dimensions[dimension]:
                if unit in self.userunits:
                    delunit = {'abbr': unit, 'dimension': dimension}
                    self.delete_unit(delunit)
            # delete dimension from internal variables
            idx = self.userdimensions.index(dimension)
            del self.userdimensions[idx]
            if dimension not in self.static_dimensions:
                del self.dimensions[dimension]
            # Delete dimension form XML tree
            for element in self.usertree:
                if element.get('name') == dimension:
                    self.usertree.remove(element)
                    break
            return True
        else:
            return False

    def add_unit(self, dimension, unit):
        """Add a unit and conversion factor to a specific dimension. The new
        unit will be written to the custom XML-file.
        """
        if dimension in self.dimensions.keys() and \
                unit['abbr'] not in self.dimensions[dimension]:

            # Update internal variables:
            self.dimensions[dimension].append(unit['abbr'])
            self.units.update({unit['abbr']:
                               (float(unit['lf']), float(unit['cf']))})
            self.unit_description.update({unit['abbr']: unit['name']})
            self.userunits.append(unit['abbr'])
            # Update XML tree
            # 'info' is the only field that is allowed to be empty
            if 'info' not in unit.keys() or unit['info'] is None:
                unit['info'] = ''
            element_index = None
            for i, element in enumerate(self.usertree):
                if element.get('name') == dimension:
                    element_index = i
                    break
            if element_index is not None:
                self.usertree[element_index].append(
                    etree.Element('unit', name=unit['name'], abbr=unit['abbr'],
                                  lf=str(unit['lf']), cf=str(unit['cf']),
                                  info=unit['info']))
            else:
                dimension_element = etree.Element('dimension', name=dimension)
                dimension_element.append(
                    etree.Element('unit', name=unit['name'], abbr=unit['abbr'],
                                  lf=str(unit['lf']), cf=str(unit['cf']),
                                  info=unit['info']))
                self.usertree.append(dimension_element)
                self.userdimensions.append(dimension)
        else:
            return False

    def update_unit(self, dimension, unit):
        """Update a unit in the custom file. Please note that units in the
        built-in file can not be updated.
        """
        if dimension in self.dimensions.keys() and \
                unit['abbr'] in self.userunits:

            # update internal variables
            self.dimensions[dimension].append(unit['abbr'])
            self.units.update({unit['abbr']:
                               (float(unit['lf']), float(unit['cf']))})
            self.unit_description.update({unit['abbr']: unit['name']})
            # Update XML tree
            if 'info' not in unit.keys() or unit['info'] is None:
                unit['info'] = ''
            element_index = None
            for i, element in enumerate(self.usertree):
                if element.get('name') == dimension:
                    element_index = i
                    break
            if element_index is not None:
                for unit_element in self.usertree[element_index]:
                    if unit_element.get('abbr') == unit['abbr']:
                        self.usertree[element_index].remove(unit_element)
                self.usertree[element_index].append(
                    etree.Element('unit', name=unit['name'], abbr=unit['abbr'],
                                  lf=str(unit['lf']), cf=str(unit['cf']),
                                  info=unit['info']))
                return True
            else:
                return False
        else:
            return False

    def delete_unit(self, unit):
        """Delete a unit from the custom file.
        """
        if unit['abbr'] in self.userunits:
            self.userunits.remove(unit['abbr'])
            self.dimensions[unit['dimension']].remove(unit['abbr'])
            del self.units[unit['abbr']]
            del self.unit_description[unit['abbr']]
            # Update XML tree
            element_index = None
            for i, element in enumerate(self.usertree):
                if element.get('name') == unit['dimension']:
                    element_index = i
                    break
            if element_index is not None:
                for unit_element in self.usertree[element_index]:
                    if unit_element.get('abbr') == unit['abbr']:
                        self.usertree[element_index].remove(unit_element)
                return True
            else:
                return False
        else:
            return False

    def save_user_file(self):
        """Save units or dimensions added to the server to the custom XML file.
        """
        user_unitfile = self.usertree.base
        with open(user_unitfile, 'w') as f:
            f.write(etree.tostring(self.usertree, pretty_print=True))

if __name__ == '__main__':
    units = Units()
    for dim in units.unittree:
        print '**' + dim.get('name') + '**'
        for unit in dim:
            print unit.get('name'), unit.get('abbr'), unit.get('lf'), \
                unit.get('cf'), unit.get('info')

    print units.convert(200, 'm^3', 'ac-ft')
