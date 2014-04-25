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
#
from db import HydraIface

from hydra_base import HydraService
from db import IfaceLib

from db import hdb

def get_resource(ref_key, ref_id):
    if ref_key == 'NODE':
        return HydraIface.Node(node_id = ref_id)
    elif ref_key == 'LINK':
        return HydraIface.Link(link_id = ref_id)
    elif ref_key == 'NETWORK':
        return HydraIface.Network(network_id = ref_id)
    elif ref_key == 'SCENARIO':
        return HydraIface.Scenario(scenario_id = ref_id)
    elif ref_key == 'PROJECT':
        return HydraIface.Project(project_id = ref_id)
    else:
        return None

def get_attribute_by_id(ID):
    """
        Get a specific attribute by its ID.
    """

    sql = """
    select
        attr_id,
        attr_name,
        attr_dimen
    from
        tAttr
    where
        attr_id = %s
    """ % ID

    rs = HydraIface.execute(sql)

    if len(rs) == 0:
       return None
    else:
        x = HydraIface.Attr()
        x.attr_name  = rs[0].attr_name
        x.attr_dimen = rs[0].attr_dimen
        x.attr_id    = rs[0].attr_id
        return x

def get_attribute_by_name_and_dimension(name, dimension):
    """
        Get a specific attribute by its name.
    """

    dimension_str = "and attr_dimen='%s'"%dimension if dimension is not None else '' 

    sql = """
        select
            attr_id,
            attr_name,
            attr_dimen
        from
            tAttr
        where
            attr_name      = '%s'
            %s
    """ % (name, dimension_str)

    rs = HydraIface.execute(sql)

    if len(rs) == 0:
       return None
    else:
        x = HydraIface.Attr()
        x.attr_name  = rs[0].attr_name
        x.attr_dimen = rs[0].attr_dimen
        x.attr_id    = rs[0].attr_id
        return x

def add_attribute(attr):
    """
    Add a generic attribute, which can then be used in creating
    a resource attribute, and put into a type.

    .. code-block:: python

        (Attr){
            id = 1020
            name = "Test Attr"
            dimen = "very big"
        }

    """
    x = HydraIface.Attr()
    x.db.attr_name = attr.name
    x.db.attr_dimen = attr.dimen
    x.save()
    return x

def add_attributes(attrs):
    """
    Add a generic attribute, which can then be used in creating
    a resource attribute, and put into a type.

    .. code-block:: python

        (Attr){
            id = 1020
            name = "Test Attr"
            dimen = "very big"
        }

    """

    #Check to see if any of the attributs being added are already there.
    #If they are there already, don't add a new one. If an attribute
    #with the same name is there already but with a different dimension,
    #add a new attribute.
    sql = """
        select
            attr_id,
            attr_name,
            attr_dimen
        from
            tAttr
    """

    rs = HydraIface.execute(sql)

    attrs_to_add = []
    for potential_new_attr in attrs:
        for r in rs:
            if potential_new_attr.name == r.attr_name and \
               potential_new_attr.dimen == r.attr_dimen:
                #raise HydraError("Attribute %s already exists but "
                #                    "with a different dimension: %s",\
                #                    r.attr_name, r.attr_dimen)
                break
        else:
            attrs_to_add.append(potential_new_attr)

    iface_attrs = []
    for attr in attrs_to_add:
        x = HydraIface.Attr()
        x.db.attr_name = attr.name
        x.db.attr_dimen = attr.dimen
        iface_attrs.append(x)

    IfaceLib.bulk_insert(iface_attrs, 'tAttr')

    sql = """
        select
            attr_id,
            attr_name,
            attr_dimen
        from
            tAttr
    """

    rs = HydraIface.execute(sql)

    all_attrs = []
    for r in rs:
        x = HydraIface.Attr()
        x.attr_name  = r.attr_name
        x.attr_dimen = r.attr_dimen
        x.attr_id    = r.attr_id
        all_attrs.append(x)

    new_attrs = []
    for attr in all_attrs:
        for new_attr in attrs:
            if new_attr.name == attr.name and new_attr.dimen == attr.dimen:
                new_attrs.append(attr)
                break

    return new_attrs

def get_attributes():
    """
        Get all attributes
    """


    sql = """
        select
            attr_id,
            attr_name,
            attr_dimen
        from
            tAttr
    """

    rs = HydraIface.execute(sql)

    attrs = []
    for r in rs:
        x = HydraIface.Attr()
        x.attr_name  = r.attr_name
        x.attr_dimen = r.attr_dimen
        x.attr_id    = r.attr_id
        attrs.append(x)

    return attrs

def get_attribute_by_id(ID):
    """
        Get a specific attribute by its ID.
    """
    return get_attribute_by_id(ID)

def get_attribute(name, dimension):
    """
        Get a specific attribute by its name.
    """
    return get_attribute_by_name_and_dimension(name, dimension)

def delete_attribute(attr_id):
    """
        Set the status of an attribute to 'X'
    """
    success = True
    x = HydraIface.Attr(attr_id = attr_id)
    x.db.status = 'X'
    x.save()
    hdb.commit()
    return success


def add_resource_attribute(resource_type, resource_id, attr_id, is_var):
    """
        Add a resource attribute attribute to a resource.

        attr_is_var indicates whether the attribute is a variable or not --
        this is used in simulation to indicate that this value is expected
        to be filled in by the simulator.
    """
    resource_i = get_resource(resource_type, resource_id)
    resource_i.load()

    attr_is_var = 'Y' if is_var else 'N'

    resource_i.add_attribute(attr_id, attr_is_var)

    return resource_i


def add_node_attrs_from_type(type_id, resource_type, resource_id):
    """
        adds all the attributes defined by a type to a node.
    """
    type_i = HydraIface.TemplateType(type_id)
    type_i.load()

    resource_i = get_resource(resource_type, resource_id)

    for item in type_i.typeattrs:
        resource_i.add_attribute(item.db.attr_id)

    return resource_i
