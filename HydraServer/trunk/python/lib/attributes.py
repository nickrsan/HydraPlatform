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

from db import IfaceLib

from db import hdb

import logging
log = logging.getLogger(__name__)

def _get_resource(ref_key, ref_id):
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

def get_attribute_by_id(attr_id, **kwargs):
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
    """ % attr_id

    rs = HydraIface.execute(sql)

    if len(rs) == 0:
       return None
    else:
        attr_i = HydraIface.Attr()
        attr_i.attr_name  = rs[0].attr_name
        attr_i.attr_dimen = rs[0].attr_dimen
        attr_i.attr_id    = rs[0].attr_id
        return attr_i.get_as_dict()

def get_attribute_by_name_and_dimension(name, dimension,**kwargs):
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
        attr_i = HydraIface.Attr()
        attr_i.db.attr_name  = rs[0].attr_name
        attr_i.db.attr_dimen = rs[0].attr_dimen
        attr_i.db.attr_id    = rs[0].attr_id
        return attr_i.get_as_dict()

def add_attribute(attr,**kwargs):
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
    log.debug("Adding attribute: %s", attr.name)
    attr_i = HydraIface.Attr()
    attr_i.db.attr_name = attr.name
    attr_i.db.attr_dimen = attr.dimen
    attr_i.save()
    return attr_i.get_as_dict()

def add_attributes(attrs,**kwargs):
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

    log.debug("Adding s: %s", [attr.name for attr in attrs])
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
        attr_i = HydraIface.Attr()
        attr_i.db.attr_name = attr.name
        attr_i.db.attr_dimen = attr.dimen
        iface_attrs.append(attr_i)

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
        attr_i = HydraIface.Attr()
        attr_i.db.attr_name  = r.attr_name
        attr_i.db.attr_dimen = r.attr_dimen
        attr_i.db.attr_id    = r.attr_id
        all_attrs.append(attr_i)

    new_attrs = []
    for attr in all_attrs:
        for new_attr in attrs:
            if new_attr.name == attr.db.attr_name and new_attr.dimen == attr.db.attr_dimen:
                new_attrs.append(attr.get_as_dict())
                break

    return new_attrs

def get_attributes(**kwargs):
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
        x.db.attr_name  = r.attr_name
        x.db.attr_dimen = r.attr_dimen
        x.db.attr_id    = r.attr_id
        attrs.append(x.get_as_dict())

    return attrs

def delete_attribute(attr_id,**kwargs):
    """
        Set the status of an attribute to 'X'
    """
    success = True
    x = HydraIface.Attr(attr_id = attr_id)
    x.db.status = 'X'
    x.save()
    hdb.commit()
    return success


def add_resource_attribute(resource_type, resource_id, attr_id, is_var,**kwargs):
    """
        Add a resource attribute attribute to a resource.

        attr_is_var indicates whether the attribute is a variable or not --
        this is used in simulation to indicate that this value is expected
        to be filled in by the simulator.
    """
    resource_i = _get_resource(resource_type, resource_id)
    resource_i.load()

    attr_is_var = 'Y' if is_var else 'N'

    resource_i.add_attribute(attr_id, attr_is_var)

    return resource_i.get_as_dict()


def add_node_attrs_from_type(type_id, resource_type, resource_id,**kwargs):
    """
        adds all the attributes defined by a type to a node.
    """
    type_i = HydraIface.TemplateType(type_id)
    type_i.load()

    resource_i = _get_resource(resource_type, resource_id)

    for item in type_i.typeattrs:
        resource_i.add_attribute(item.db.attr_id)

    return resource_i.get_as_dict()
