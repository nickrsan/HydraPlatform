#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from HydraLib.HydraException import HydraError
from spyne.model.primitive import String, Boolean, Decimal, AnyDict
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from spyne.util.dictdoc import get_object_as_dict
from hydra_base import HydraService, ObjectNotFoundError
from hydra_complexmodels import Unit

from HydraLib import units

global hydra_units
hydra_units = units.Units()


class UnitService(HydraService):
    """
    """

    @rpc(String, _returns=Boolean)
    def add_dimension(ctx, dimension):
        """Add a physical dimensions (such as ``Volume`` or ``Speed``) to the
        servers list of dimensions. If the dimension already exists, nothing is
        done.
        """
        result = hydra_units.add_dimension(dimension)
        hydra_units.save_user_file()
        return result

    @rpc(String, _returns=Boolean)
    def delete_dimension(ctx, dimension):
        """Delete a physical dimension from the list of dimensions. Please note
        that deleting works only for dimensions listed in the custom file.
        """
        result = hydra_units.delete_dimension(dimension)
        hydra_units.save_user_file()
        return result

    @rpc(Unit, _returns=Boolean)
    def add_unit(ctx, unit):
        """Add a physical unit to the servers list of units. The Hydra server
        provides a complex model ``Unit`` which should be used to add a unit.

        A minimal example:

        .. code-block:: python

            from HydraLib import PluginLib

            cli = PluginLib.connect()

            new_unit = cli.factory.create('hyd:Unit')
            new_unit.name = 'Teaspoons per second'
            new_unit.abbr = 'tsp s^-1'
            new_unit.cf = 0               # Constant conversion factor
            new_unit.lf = 1.47867648e-05  # Linear conversion factor
            new_unit.dimension = 'Volumetric flow rate'
            new_unit.info = 'A flow of one tablespoon per second.'

            cli.service.add_unit(new_unit)
        """
        # Convert the complex model into a dict
        unitdict = get_object_as_dict(unit, Unit)
        if unit.dimension is None:
            unitdict['dimension'] = hydra_units.get_dimension(unit.abbr)
        hydra_units.add_unit(unitdict['dimension'], unitdict)
        hydra_units.save_user_file()
        return True

    @rpc(Unit, _returns=Boolean)
    def update_unit(ctx, unit):
        """Update an existing unit added to the custom unit collection. Please
        not that units built in to the library can not be updated.
        """
        unitdict = get_object_as_dict(unit, Unit)
        if unit.dimension is None:
            unitdict['dimension'] = hydra_units.get_dimension(unit.abbr)
        result = hydra_units.update_unit(unitdict['dimension'], unitdict)
        hydra_units.save_user_file()
        return result

    @rpc(Unit, _returns=Boolean)
    def delete_unit(ctx, unit):
        """Delete a unit from the custom unit collection.
        """
        unitdict = get_object_as_dict(unit, Unit)
        if unit.dimension is None:
            unitdict['dimension'] = hydra_units.get_dimension(unit.abbr)
        result = hydra_units.delete_unit(unitdict)
        hydra_units.save_user_file()
        return result

    @rpc(Decimal, String, String, _returns=Decimal)
    def convert_units(ctx, value, unit1, unit2):
        """Convert a value from one unit to another one.

        Example::

            >>> cli = PluginLib.connect()
            >>> cli.service.convert_units(20.0, 'm', 'km')
            0.02
        """
        value = float(value)
        return hydra_units.convert(value, unit1, unit2)

    @rpc(String, _returns=String)
    def get_dimension(ctx, unit1):
        """Get the corresponding physical dimension for a given unit.

        Example::

            >>> cli = PluginLib.connect()
            >>> cli.service.get_dimension('m')
            Length
        """
        return hydra_units.get_dimension(unit1)

    @rpc(_returns=SpyneArray(String))
    def get_dimensions(ctx):
        """Get a list of all physical dimensions available on the server.
        """
        dim_list = hydra_units.get_dimensions()
        return dim_list

    @rpc(String, _returns=SpyneArray(Unit))
    def get_units(ctx, dimension):
        """Get a list of all units corresponding to a physical dimension.
        """
        unit_list = hydra_units.get_units(dimension)
        complex_model_list = []
        for unit in unit_list:
            cm_unit = Unit()
            cm_unit.name = unit['name']
            cm_unit.abbr = unit['abbr']
            cm_unit.lf = unit['lf']
            cm_unit.cf = unit['cf']
            cm_unit.dimension = unit['dimension']
            cm_unit.info = unit['info']
            complex_model_list.append(cm_unit)
        return complex_model_list

    @rpc(String, String, _returns=Boolean)
    def check_consistency(ctx, unit, dimension):
        """Check if a given units corresponds to a physical dimension.
        """
        return hydra_units.check_consistency(unit, dimension)
