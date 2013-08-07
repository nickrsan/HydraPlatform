from spyne.model.complex import Array, ComplexModel
from spyne.model.primitive import String, Integer, Decimal

#This is on hold until we come up with a solution
#for dynamically generating the classes below.
#global typemap
#typemap = {
#    'varchar'    : String,
#    'int'       : Integer,
#    'double'    : Decimal,
#    'blob'      : Array,
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

class Attribute(ComplexModel):
    _type_info = [
        ('attribute_id', Integer),
        ('attribute_name', String),
    ]

class Scenario(ComplexModel):
    _type_info = [
        ('scenario_id', Integer),
        ('scenario_name', String),
        ('scenario_description', String),
        ('attributes', Array(Attribute)),
    ]

class Node(ComplexModel):
    _type_info = [
        ('node_id', Integer),
        ('node_name', String),
        ('node_description', String),
        ('node_x', Decimal),
        ('node_y', Decimal),
        ('attributes', Array(Attribute)),
    ]

class Link(ComplexModel):
    _type_info = [
        ('link_id',          Integer),
        ('link_name',        String),
        ('link_description', String),
        ('node_1_id',        Integer),
        ('node_2_id',        Integer),
        ('attributes',       Array(Attribute)),
    ]

class Network(ComplexModel):
    _type_info = [
        ('project_id',          Integer),
        ('network_id',          Integer),
        ('network_name',        String),
        ('network_description', String),
        ('attributes',          Array(Attribute)),
        ('scenarios',           Array(Scenario)),
        ('nodes',               Array(Node)),
        ('links',               Array(Link)),
    ]

class Project(ComplexModel):
    _type_info = [
        ('project_id', Integer),
        ('project_name', String),
        ('project_description', String),
        ('attributes', Array(Attribute)),

    ]
