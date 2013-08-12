from spyne.model.complex import Array as SpyneArray, ComplexModel 
from spyne.model.primitive import String, Integer, Decimal, DateTime, Boolean, AnyDict, AnyXml

#This is on hold until we come up with a solution
#for dynamically generating the classes below.
#global typemap
#typemap = {
#    'varchar'    : String,
#    'int'       : Integer,
#    'double'    : Decimal,
#    'blob'      : SpyneArray,
#    'timestamp' : DateTime,
#}
#
#def get_spyne_type(db_type_dict):
#    type_info = []
#    for col_name, col_type in db_type_dict.items():
#        base_type = col_type.split('(')[0]
#        spyne_type = typemap[base_type]
#        type_info.append((col_name, spyne_type))
#    return type_info

def parse_value(value):
    """
    Turn a complex model object into a hydraiface - friendly value.
    returns a tuple containing:
    (data_type, units, name, dimension, value)
    """
    pass

class Descriptor(ComplexModel):
   _type_info = [
        ('desc_val', String),
    ]

class TimeSeries(ComplexModel):
    _type_info = [
        ('ts_time', DateTime),
        ('ts_value', AnyDict),
    ]

class EqTimeSeries(ComplexModel):
    _type_info = [
        ('start_time', DateTime),
        ('frequency', Decimal),
        ('arr_data', AnyDict),
    ]

class Scalar(ComplexModel):
    _type_info = [
        ('param_value', Decimal),
    ]

class Array(ComplexModel):
    _type_info = [
        ('arr_data', AnyDict),
    ]

class Attr(ComplexModel):
    _type_info = [
        ('attr_id', Integer),
        ('attr_name', String),
        ('attr_dimen', String),
    ]

class ResourceAttr(ComplexModel):
    _type_info = [
        ('resource_attr_id', Integer),
        ('attr_id',          Integer),
        ('ref_id',           Integer),
        ('ref_key',          String),
        ('attr_is_var',      Boolean),
    ]

class ResourceTemplateGroup(ComplexModel):
    _type_info = [
        ('group_id', Integer),
        ('group_name', String),
    ]

class ResourceTemplate(ComplexModel):
    _type_info = [
        ('template_name', Integer),
        ('template_id', Integer),
        ('group_id',    Integer)
    ]

class ResourceTemplateItem(ComplexModel):
    _type_info = [
        ('attr_id', Integer),
        ('template_id', Integer),
    ]

class Node(ComplexModel):
    _type_info = [
        ('node_id', Integer),
        ('node_name', String),
        ('node_description', String),
        ('node_x', Decimal),
        ('node_y', Decimal),
        ('attributes', SpyneArray(ResourceAttr)),
    ]

class Link(ComplexModel):
    _type_info = [
        ('link_id',          Integer),
        ('link_name',        String),
        ('link_description', String),
        ('node_1_id',        Integer),
        ('node_2_id',        Integer),
        ('attributes',       SpyneArray(ResourceAttr)),
    ]

class ScenarioAttr(ComplexModel):
    _type_info = [
        ('resource_attr_id', Integer),
        ('attr_id', Integer),
        ('value', AnyXml)
    ]

class Scenario(ComplexModel):
    _type_info = [
        ('network_id', Integer),
        ('scenario_id', Integer),
        ('scenario_name', String),
        ('scenario_description', String),
        ('attributes', SpyneArray(ResourceAttr)),
        ('data', SpyneArray(ScenarioAttr)),
    ]

class Network(ComplexModel):
    _type_info = [
        ('project_id',          Integer),
        ('network_id',          Integer),
        ('network_name',        String),
        ('network_description', String),
        ('attributes',          SpyneArray(ResourceAttr)),
        ('scenarios',           SpyneArray(Scenario)),
        ('nodes',               SpyneArray(Node)),
        ('links',               SpyneArray(Link)),
    ]

class Project(ComplexModel):
    _type_info = [
        ('project_id', Integer),
        ('project_name', String),
        ('project_description', String),
        ('attributes', SpyneArray(ResourceAttr)),

    ]

class ConstraintItem(ComplexModel):
    _type_info = [
        ('constraint_id', Integer),
        ('item_id', Integer),
        ('resource_attr_id', Integer),
        ('resource_attr', ResourceAttr),
    ]

class ConstraintGroup(ComplexModel):
    _type_info = [
        ('constraint_id', Integer),
        ('group_id', Integer),
        ('op', String),
        ('ref_key_1', String),
        ('ref_id_1', Integer),
        ('ref_key_2', String),
        ('ref_id_2', Integer),
    ]

class Constraint(ComplexModel):
    _type_info = [
        ('constraint_id', Integer),
        ('scenario_id', Integer),
        ('group_id', Integer),
        ('constant', Decimal),
        ('op', String),
        ('groups', SpyneArray(ConstraintGroup)),
        ('items', SpyneArray(ConstraintItem)),
    ]

class User(ComplexModel):
    _type_info = [
        ('user_id', Integer),
        ('username', String),
        ('password', String),
    ]

class Perm(ComplexModel):
    _type_info = [
        ('perm_id', Integer),
        ('perm_name', String),
    ]

class Role(ComplexModel):
    _type_info = [
        ('role_id',     Integer),
        ('role_name',   String),
        ('permissions', SpyneArray(Perm)),
        ('users',       SpyneArray(User)),
    ]

class RoleUser(ComplexModel):
    _type_info = [
        ('role_id', Integer),
        ('', String),
    ]

class Test(ComplexModel):
    _type_info = [
        ('test_entry_1', Integer),
        ('test_entry_2', String),
    ]
