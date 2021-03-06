#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) Copyright 2013, 2014, University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
"""This module provides facilities for unit conversion and consistency checking
between units and dimensions.
"""

import os
from copy import deepcopy
from HydraLib.HydraException import HydraError

import config
from lxml import etree
import logging

log = logging.getLogger(__name__)

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
    unit_info = dict()

    def __init__(self):
        default_user_file_location = os.path.realpath(\
            os.path.join(os.path.dirname(os.path.realpath(__file__)),
                         '../../'
                         'static',
                         'user_units.xml'))

        user_unitfile = config.get("unit_conversion",
                                       "user_file",
                                       default_user_file_location)

        #If the user unit file doesn't exist, create it.
        if not os.path.exists(user_unitfile):
            open(user_unitfile, 'a').close()

        default_builtin_unitfile_location = \
                os.path.join(os.path.dirname(os.path.realpath(__file__)),
                            '../../'
                             'static',
                             'unit_definitions.xml')
            
        builtin_unitfile = config.get("unit_conversion",
                                       "default_file",
                                       default_builtin_unitfile_location)

        log.info("Default unitfile: %s", builtin_unitfile)
        log.info("User unitfile: %s", user_unitfile)

        with open(builtin_unitfile) as f:
            self.unittree = etree.parse(f).getroot()

        for element in self.unittree:
            self.static_dimensions.append(element.get('name'))

        with open(user_unitfile) as f:
            self.usertree = etree.parse(f).getroot()
      
        with open(builtin_unitfile) as f:
            self.unittree = etree.parse(f).getroot()
      
        for element in self.usertree:
            self.unittree.append(deepcopy(element))
            self.userdimensions.append(element.get('name'))
            for subelement in element:
                self.userunits.append(subelement.get('abbr'))

        for element in self.unittree:
            dimension = element.get('name')
            if dimension not in self.dimensions.keys():
                self.dimensions.update({dimension: []})
            for unit in element:
                self.dimensions[dimension].append(unit.get('abbr'))
                self.units.update({unit.get('abbr'):
                                   (float(unit.get('lf')),
                                    float(unit.get('cf')))})
                self.unit_description.update({unit.get('abbr'):
                                              unit.get('name')})
                self.unit_info.update({unit.get('abbr'):
                                       unit.get('info')})

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
        raise HydraError('Unit %s not found.'%(unit))

    def convert(self, values, unit1, unit2):
        """Convert a value from one unit to another one. The two units must
        represent the same physical dimension.
        """
        if self.get_dimension(unit1) == self.get_dimension(unit2):
            unit1, factor1 = self.parse_unit(unit1)
            unit2, factor2 = self.parse_unit(unit2)
            conv_factor1 = self.units[unit1]
            conv_factor2 = self.units[unit2]

            if isinstance(values, float):
                return (conv_factor1[0] / conv_factor2[0] * (factor1 * values)
                        + (conv_factor1[1] - conv_factor2[1])
                        / conv_factor2[0]) / factor2
            elif isinstance(values, list):
                return [(conv_factor1[0] / conv_factor2[0] * (factor1 * value)
                        + (conv_factor1[1] - conv_factor2[1])
                        / conv_factor2[0]) / factor2 for value in values]
        else:
            raise HydraError("Unit conversion: dimensions are not consistent.")

    def parse_unit(self, unit):
        """Helper function that extracts constant factors from unit
        specifications. This allows to specify units similar to this: 10^6 m^3.
        """
        try:
            float(unit[0])
            factor, unit = unit.split(' ', 1)
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
            unitdict.update({'info': self.unit_info[unit]})
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

            # 'info' is the only field that is allowed to be empty
            if 'info' not in unit.keys() or unit['info'] is None:
                unit['info'] = ''
            # Update internal variables:
            self.dimensions[dimension].append(unit['abbr'])
            self.units.update({unit['abbr']:
                               (float(unit['lf']), float(unit['cf']))})
            self.unit_description.update({unit['abbr']: unit['name']})
            self.userunits.append(unit['abbr'])
            self.unit_info.update({unit['abbr']: unit['info']})
            # Update XML tree
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
            raise HydraError('Unit %s with dimension %s not found.'%(unit,dimension))

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

def validate_resource_attributes(resource, attributes, template, check_unit=True, exact_match=False):
    """
        Validate that the resource provided matches the template.
        Only passes if the resource contains ONLY the attributes specified
        in the template.

        The template should take the form of a dictionary, as should the
        resources.

        *check_unit*:  Make sure that if a unit is specified in the template, it 
                     is the same in the data 
        *exact_match*: Enure that all the attributes in the template are in 
                     the data also. By default this is false, meaning a subset 
                     of the template attributes may be specified in the data.
                     An attribute specified in the data *must* be defined in 
                     the template.

        @returns a list of error messages. An empty list indicates no
        errors were found.
    """
    errors = []
    #is it a node or link?
    res_type = 'GROUP'
    if resource.get('x') is not None:
        res_type = 'NODE'
    elif resource.get('node_1_id') is not None:
        res_type = 'LINK'
    elif resource.get('nodes') is not None:
        res_type = 'NETWORK'

    #Find all the node/link/network definitions in the template
    tmpl_res = template['resources'][res_type]

    #the user specified type of the resource
    res_user_type = resource.get('type')

    #Check the user specified type is in the template
    if res_user_type is None:
        errors.append("No type specified on resource %s"%(resource['name']))

    elif tmpl_res.get(res_user_type) is None:
        errors.append("Resource %s is defined as having type %s but "
                      "this type is not specified in the template."%
                      (resource['name'], res_user_type))

    #It is in the template. Now check all the attributes are correct.
    tmpl_attrs = tmpl_res.get(res_user_type)['attributes']

    attrs = {}
    for a in attributes.values():
        attrs[a['id']] = a

    for a in tmpl_attrs.values():
        if a.get('id') is not None:
            attrs[a['id']] = {'name':a['name'], 'unit':a.get('unit'), 'dimen':a.get('dimension')}

    if exact_match is True:
        #Check that all the attributes in the template are in the data.
        #get all the attribute names from the template
        tmpl_attr_names = set(tmpl_attrs.keys())
        #get all the attribute names from the data for this resource
        resource_attr_names = []
        for ra in resource['attributes']:
            attr_name = attrs[ra['attr_id']]['name']
            resource_attr_names.append(attr_name)
        resource_attr_names = set(resource_attr_names)

        #Compare the two lists to ensure they are the same (using sets is easier)
        in_tmpl_not_in_resource = tmpl_attr_names - resource_attr_names
        in_resource_not_in_tmpl = resource_attr_names - tmpl_attr_names

        if len(in_tmpl_not_in_resource) > 0:
            errors.append("Template has defined attributes %s for type %s but they are not"
                            " specified in the Data."%(','.join(in_tmpl_not_in_resource),
                                                    res_user_type ))

        if len(in_resource_not_in_tmpl) > 0:
            errors.append("Resource %s (type %s) has defined attributes %s but this is not"
                            " specified in the Template."%(resource['name'],
                                                        res_user_type,
                                                        ','.join(in_resource_not_in_tmpl)))

    #Check that each of the attributes specified on the resource are valid.
    for res_attr in resource['attributes']:

        attr = attrs.get(res_attr['attr_id'])

        if attr is None:
            errors.append("An attribute mismatch has occurred. Attr %s is not "
                          "defined in the data but is present on resource %s"
                          %(res_attr['attr_id'], resource['name']))
            continue 

        #If an attribute is not specified in the template, then throw an error
        if tmpl_attrs.get(attr['name']) is None:
            errors.append("Resource %s has defined attribute %s but this is not"
                          " specified in the Template."%(resource['name'], attr['name']))
        else:
            #If the dimensions or units don't match, throw an error

            tmpl_attr = tmpl_attrs[attr['name']]

            if tmpl_attr.get('data_type') is not None:
                if res_attr.get('data_type') is not None:
                    if tmpl_attr.get('data_type') != res_attr.get('data_type'):
                        errors.append("Error in data. Template says that %s on %s is a %s, but data suggests it is a %s"%
                            (attr['name'], resource['name'], tmpl_attr.get('data_type'), res_attr.get('data_type')))

            attr_dimen = "dimensionless" if attr.get('dimen') is None else attr.get('dimen')
            tmpl_attr_dimen = "dimensionless" if tmpl_attr.get('dimension') is None else tmpl_attr.get('dimension')
            
            if attr_dimen.lower() != tmpl_attr_dimen.lower():
                errors.append("Dimension mismatch on resource %s for attribute %s"
                              " (template says %s on type %s, data says %s)"%
                              (resource['name'], attr.get('name'), 
                               tmpl_attr.get('dimension'), res_user_type, attr_dimen))

            if check_unit is True:
                if tmpl_attr.get('unit') is not None:
                    if attr.get('unit') != tmpl_attr.get('unit'):
                        errors.append("Unit mismatch for resource %s with unit %s "
                                      "(template says %s for type %s)"
                                      %(resource['name'], attr.get('unit'),
                                        tmpl_attr.get('unit'), res_user_type))

    return errors

if __name__ == '__main__':
    units = Units()
    for dim in units.unittree:
        print '**' + dim.get('name') + '**'
        for unit in dim:
            print unit.get('name'), unit.get('abbr'), unit.get('lf'), \
                unit.get('cf'), unit.get('info')

    print units.convert(200, 'm^3', 'ac-ft')
