# (c) Copyright 2013, 2014, University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from db import HydraIface
from HydraLib import units
from HydraLib.HydraException import HydraError
from HydraLib.util import array_dim
from HydraLib.util import arr_to_vector
from HydraLib.util import vector_to_arr
import logging
log = logging.getLogger(__name__)

global hydra_units
hydra_units = units.Units()


def add_dimension(dimension,**kwargs):
    """Add a physical dimensions (such as ``Volume`` or ``Speed``) to the
    servers list of dimensions. If the dimension already exists, nothing is
    done.
    """
    result = hydra_units.add_dimension(dimension)
    hydra_units.save_user_file()
    return result

def delete_dimension(dimension,**kwargs):
    """Delete a physical dimension from the list of dimensions. Please note
    that deleting works only for dimensions listed in the custom file.
    """
    result = hydra_units.delete_dimension(dimension)
    hydra_units.save_user_file()
    return result

def add_unit(unitdict,**kwargs):
    """Add a physical unit to the servers list of units. The Hydra server
    provides a Unit object which should be used to add a unit.

    A minimal example:

    .. code-block:: python

        new_unit = dict(
            name      = 'Teaspoons per second',
            abbr      = 'tsp s^-1',
            cf        = 0,               # Constant conversion factor
            lf        = 1.47867648e-05,  # Linear conversion factor
            dimension = 'Volumetric flow rate',
            info      = 'A flow of one teaspoon per second.',
        )
        add_unit(new_unit)
    """
    if unitdict['dimension'] is None:
        unitdict['dimension'] = hydra_units.get_dimension(unitdict['abbr'])
    hydra_units.add_unit(unitdict['dimension'], unitdict)
    hydra_units.save_user_file()
    return True

def update_unit(unitdict,**kwargs):
    """Update an existing unit added to the custom unit collection. Please
    not that units built in to the library can not be updated.
    """
    if unitdict['dimension'] is None:
        unitdict['dimension'] = hydra_units.get_dimension(unitdict['abbr'])
    result = hydra_units.update_unit(unitdict['dimension'], unitdict)
    hydra_units.save_user_file()
    return result

def delete_unit(unitdict,**kwargs):
    """Delete a unit from the custom unit collection.
    """
    if unitdict['dimension'] is None:
        unitdict['dimension'] = hydra_units.get_dimension(unitdict['abbr'])
    result = hydra_units.delete_unit(unitdict)
    hydra_units.save_user_file()
    return result

def convert_units(values, unit1, unit2,**kwargs):
    """Convert a value from one unit to another one.

    Example::

        >>> cli = PluginLib.connect()
        >>> cli.service.convert_units(20.0, 'm', 'km')
        0.02
    """
    float_values = [float(value) for value in values]
    return hydra_units.convert(float_values, unit1, unit2)

def convert_dataset(dataset_id, to_unit,**kwargs):
    """Convert a whole dataset (specified by 'dataset_id' to new unit
    ('to_unit').
    """
    ds_i = HydraIface.Dataset(dataset_id=dataset_id)
    dataset_type = ds_i.db.data_type

    dsval = ds_i.get_val()
    old_unit = ds_i.db.data_units

    if old_unit is not None:
        if dataset_type == 'scalar':
            new_val = hydra_units.convert(float(dsval), old_unit, to_unit)
        elif dataset_type == 'array':
            dim = array_dim(dsval)
            vecdata = arr_to_vector(dsval)
            newvec = hydra_units.convert(vecdata, old_unit, to_unit)
            new_val = vector_to_arr(newvec, dim)
        elif dataset_type == 'timeseries':
            new_val = []
            for ts_data in dsval:
                dim = array_dim(ts_data[1])
                vecdata = arr_to_vector(ts_data[1])
                newvec = hydra_units.convert(vecdata, old_unit, to_unit)
                newarr = vector_to_arr(newvec, dim)
                new_val.append(ts_data[0], newarr)
        elif dataset_type == 'eqtimeseries':
            pass
        elif dataset_type == 'descriptor':
            raise HydraError('Cannot convert descriptor.')

        ds_i.db.data_units = to_unit
        ds_i.set_val(dataset_type, new_val)
        ds_i.set_hash(new_val)
        ds_i.save()

        return ds_i.db.dataset_id

    else:
        raise HydraError('Dataset has no units.')

def get_dimension(unit1,**kwargs):
    """Get the corresponding physical dimension for a given unit.

    Example::

        >>> cli = PluginLib.connect()
        >>> cli.service.get_dimension('m')
        Length
    """
    return hydra_units.get_dimension(unit1)

def get_dimensions(**kwargs):
    """Get a list of all physical dimensions available on the server.
    """
    dim_list = hydra_units.get_dimensions()
    return dim_list

def get_units(dimension,**kwargs):
    """Get a list of all units corresponding to a physical dimension.
    """
    unit_list = hydra_units.get_units(dimension)
    unit_dict_list = []
    for unit in unit_list:
        cm_unit = dict(
            name = unit['name'],
            abbr = unit['abbr'],
            lf = unit['lf'],
            cf = unit['cf'],
            dimension = unit['dimension'],
            info = unit['info'],
        )
        unit_dict_list.append(cm_unit)
    return unit_dict_list

def check_consistency(unit, dimension,**kwargs):
    """Check if a given units corresponds to a physical dimension.
    """
    return hydra_units.check_consistency(unit, dimension)