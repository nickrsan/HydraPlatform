from spyne.model.complex import Array, ComplexModel
from spyne.model.primitive import String, Integer, Double

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
        ('node_x', Double),
        ('node_y', Double),
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
