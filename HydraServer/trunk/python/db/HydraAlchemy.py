from sqlalchemy import Column,\
ForeignKey,\
text,\
Integer,\
Numeric,\
String,\
LargeBinary,\
BigInteger,\
DateTime,\
Text

from sqlalchemy.orm import relationship, backref

from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()


class Array(Base):

    __tablename__='tArray'

    data_id = Column(Integer(), primary_key=True, nullable=False)
    arr_data = Column(LargeBinary(),  nullable=False)

class Attr(Base):

    __tablename__='tAttr'

    attr_id = Column(Integer(), primary_key=True, nullable=False)
    attr_name = Column(String(45),  nullable=False)
    attr_dimen = Column(String(45))
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_DateTime'))

class AttrMap(Base):

    __tablename__='tAttrMap'

    attr_id_a = Column(Integer(), ForeignKey('tAttr.attr_id'), primary_key=True, nullable=False)
    attr_id_b = Column(Integer(), ForeignKey('tAttr.attr_id'), primary_key=True, nullable=False)

    attr_a = relationship("Attr", foreign_keys=[attr_id_a], backref=backref('maps_to', order_by=attr_id_a))
    attr_b = relationship("Attr", foreign_keys=[attr_id_b], backref=backref('maps_from', order_by=attr_id_b))


class Constraint(Base):

    __tablename__='tConstraint'

    constraint_id = Column(Integer(), primary_key=True, nullable=False)
    scenario_id = Column(Integer(), ForeignKey('tScenario.scenario_id'),  nullable=False)
    group_id = Column(Integer())
    constant = Column(Numeric(asdecimal=True),  nullable=False)
    op = Column(String(10),  nullable=False)

    scenario = relationship('Scenario', backref=backref("constraints", order_by=constraint_id))


class ConstraintGroup(Base):

    __tablename__='tConstraintGroup'

    group_id = Column(Integer(), primary_key=True, nullable=False)
    constraint_id = Column(Integer(), ForeignKey('tConstraint.constraint_id'),  nullable=False)
    ref_key_1 = Column(String(45),  nullable=False)
    ref_id_1 = Column(Integer(),  nullable=False)
    ref_key_2 = Column(String(45))
    ref_id_2 = Column(Integer())
    op = Column(String(10))
    
    constraint = relationship('Constraint', backref=backref("groups", order_by=group_id))
    

class ConstraintItem(Base):

    __tablename__='tConstraintItem'

    item_id = Column(Integer(), primary_key=True, nullable=False)
    constraint_id = Column(Integer(), ForeignKey('tConstraint.constraint_id'),  nullable=False)
    resource_attr_id = Column(Integer(), ForeignKey('tResourceAttr.resource_attr_id'))
    constant = Column(LargeBinary())
    
    resourceattr = relationship('ResourceAttr', backref=backref("constraints", order_by=constraint_id))
    constraint = relationship('Constraint', backref=backref("items", order_by=item_id))

class Dataset(Base):

    __tablename__='tDataset'

    dataset_id = Column(Integer(), primary_key=True, nullable=False)
    data_id = Column(Integer(),  nullable=False)
    data_type = Column(String(45),  nullable=False)
    data_units = Column(String(45))
    data_dimen = Column(String(45))
    data_name = Column(String(45),  nullable=False)
    data_hash = Column(BigInteger(),  nullable=False)
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_DateTime'))
    created_by = Column(Integer(), ForeignKey('tUser.user_id'))
    locked = Column(String(1),  nullable=False, default=text(u"'N'"))
    
    
    useruser = relationship('User', backref=backref("datasets", order_by=dataset_id))
    
class DatasetGroup(Base):

    __tablename__='tDatasetGroup'

    group_id = Column(Integer(), primary_key=True, nullable=False)
    group_name = Column(String(45),  nullable=False)
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_DateTime'))
    
class DatasetGroupItem(Base):

    __tablename__='tDatasetGroupItem'

    group_id = Column(Integer(), ForeignKey('tDatasetGroup.group_id'), primary_key=True, nullable=False)
    dataset_id = Column(Integer(), ForeignKey('tDataset.dataset_id'), primary_key=True, nullable=False)
    
    group = relationship('DatasetGroup', backref=backref("items", order_by=dataset_id))
    dataset = relationship('Dataset', backref=backref("groupitems", order_by=dataset_id))

class Descriptor(Base):

    __tablename__='tDescriptor'

    data_id = Column(Integer(), primary_key=True, nullable=False)
    desc_val = Column(String(1000),  nullable=False)

class EqTimeSeries(Base):

    __tablename__='tEqTimeSeries'

    data_id = Column(Integer(), primary_key=True, nullable=False)
    start_time = Column(Numeric(precision=30, scale=20),  nullable=False)
    frequency = Column(Numeric(asdecimal=True),  nullable=False)
    arr_data = Column(LargeBinary(),  nullable=False)

class Link(Base):

    __tablename__='tLink'

    link_id = Column(Integer(), primary_key=True, nullable=False)
    network_id = Column(Integer(), ForeignKey('tNetwork.network_id'), nullable=False)
    status = Column(String(1),  nullable=False, default=text(u"'A'"))
    node_1_id = Column(Integer(), ForeignKey('tNode.node_id'), nullable=False)
    node_2_id = Column(Integer(), ForeignKey('tNode.node_id'), nullable=False)
    link_name = Column(String(45))
    link_description = Column(String(1000))
    link_layout = Column(Text())
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_DateTime'))
   
    network = relationship('Network', backref=backref("links", order_by=network_id))
    node_a = relationship('Node', foreign_keys=[node_1_id], backref=backref("links_to", order_by=link_id))
    node_b = relationship('Node', foreign_keys=[node_2_id], backref=backref("links_from", order_by=link_id))

class Metadata(Base):

    __tablename__='tMetadata'

    dataset_id = Column(Integer(), ForeignKey('tDataset.dataset_id'), primary_key=True, nullable=False)
    metadata_name = Column(String(45), primary_key=True, nullable=False)
    metadata_val = Column(LargeBinary(),  nullable=False)
    
    

class Network(Base):

    __tablename__='tNetwork'

    network_id = Column(Integer(), primary_key=True, nullable=False)
    network_name = Column(String(45),  nullable=False)
    network_description = Column(String(1000))
    network_layout = Column(Text())
    project_id = Column(Integer(), ForeignKey('tProject.project_id'),  nullable=False)
    status = Column(String(1),  nullable=False, default=text(u"'A'"))
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_DateTime'))
    projection = Column(String(1000))
    created_by = Column(Integer(), ForeignKey('tUser.user_id'))
    
    project = relationship('Project', backref=backref("networks", order_by=network_id))

class Node(Base):

    __tablename__='tNode'

    node_id = Column(Integer(), primary_key=True, nullable=False)
    network_id = Column(Integer(), ForeignKey('tNetwork.network_id'), nullable=False)
    node_description = Column(String(1000))
    node_name = Column(String(45),  nullable=False)
    status = Column(String(1),  nullable=False, default=text(u"'A'"))
    node_x = Column(Numeric(asdecimal=True))
    node_y = Column(Numeric(asdecimal=True))
    node_layout = Column(Text())
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_DateTime'))
    
    network = relationship('Network', backref=backref("nodes", order_by=network_id))

class Owner(Base):

    __tablename__='tOwner'

    user_id = Column(Integer(), ForeignKey('tUser.user_id'), primary_key=True, nullable=False)
    ref_key = Column(String(45), primary_key=True, nullable=False)
    ref_id = Column(Integer(), primary_key=True, nullable=False)
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_DateTime'))
    view = Column(String(1),  nullable=False)
    edit = Column(String(1),  nullable=False)
    share = Column(String(1),  nullable=False)
    
    user = relationship('User')
    
class Perm(Base):

    __tablename__='tPerm'

    perm_id = Column(Integer(), primary_key=True, nullable=False)
    perm_code = Column(String(45),  nullable=False)
    perm_name = Column(String(45),  nullable=False)
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_DateTime'))
    
class Project(Base):

    __tablename__='tProject'

    project_id = Column(Integer(), primary_key=True, nullable=False)
    project_name = Column(String(45),  nullable=False)
    project_description = Column(String(1000))
    status = Column(String(1),  nullable=False, default=text(u"'A'"))
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_DateTime'))
    created_by = Column(Integer(), ForeignKey('tUser.user_id'))
    
    user = relationship('User', backref=backref("projects", order_by=project_id))
    
class ResourceAttr(Base):

    __tablename__='tResourceAttr'

    resource_attr_id = Column(Integer(), primary_key=True, nullable=False)
    attr_id = Column(Integer(), ForeignKey('tAttr.attr_id'),  nullable=False)
    ref_key = Column(String(45),  nullable=False)
    ref_id = Column(Integer(),  nullable=False)
    attr_is_var = Column(String(1),  nullable=False, default=text(u"'N'"))
    
    attr = relationship('Attr')

class ResourceGroup(Base):

    __tablename__='tResourceGroup'

    group_id = Column(Integer(), primary_key=True, nullable=False)
    group_name = Column(String(45),  nullable=False)
    group_description = Column(String(1000))
    status = Column(String(1))
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_DateTime'))
    network_id = Column(Integer(), ForeignKey('tNetwork.network_id'),  nullable=False)
    
    network = relationship('Network', backref=backref("resourcegroups", order_by=group_id))
    
class ResourceGroupItem(Base):

    __tablename__='tResourceGroupItem'

    item_id = Column(Integer(), primary_key=True, nullable=False)
    ref_id = Column(Integer(),  nullable=False)
    ref_key = Column(String(45),  nullable=False)
    group_id = Column(Integer(), ForeignKey('tResourceGroup.group_id'), nullable=False)
    scenario_id = Column(Integer(), ForeignKey('tScenario.scenario_id'),  nullable=False)
    
    group = relationship('ResourceGroup', backref=backref("items", order_by=group_id))
    scenario = relationship('Scenario')

class ResourceScenario(Base):

    __tablename__='tResourceScenario'

    dataset_id = Column(Integer(), ForeignKey('tDataset.dataset_id'), nullable=False)
    scenario_id = Column(Integer(), ForeignKey('tScenario.scenario_id'), primary_key=True, nullable=False)
    resource_attr_id = Column(Integer(), ForeignKey('tResourceAttr.resource_attr_id'), primary_key=True, nullable=False)
    
    dataset      = relationship('Dataset')
    scenario     = relationship('Scenario', backref=backref("resourcescenarios", order_by=resource_attr_id))
    resourceattr = relationship('ResourceAttr')

class ResourceType(Base):

    __tablename__='tResourceType'

    ref_key = Column(String(45), primary_key=True, nullable=False)
    ref_id = Column(Integer(), primary_key=True, nullable=False)
    type_id = Column(Integer(), ForeignKey('tTemplateType.type_id'), primary_key=True, nullable=False)
    
    templatetype = relationship('TemplateType')
    
class Role(Base):

    __tablename__='tRole'

    role_id = Column(Integer(), primary_key=True, nullable=False)
    role_code = Column(String(45),  nullable=False)
    role_name = Column(String(45),  nullable=False)
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_DateTime'))
    
class RolePerm(Base):

    __tablename__='tRolePerm'

    perm_id = Column(Integer(), ForeignKey('tPerm.perm_id'), primary_key=True, nullable=False)
    role_id = Column(Integer(), ForeignKey('tRole.role_id'), primary_key=True, nullable=False)
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_DateTime'))
    
    perm = relationship('Perm')
    role = relationship('Role')
    
class RoleUser(Base):

    __tablename__='tRoleUser'

    user_id = Column(Integer(), ForeignKey('tUser.user_id'), primary_key=True, nullable=False)
    role_id = Column(Integer(), ForeignKey('tRole.role_id'), primary_key=True, nullable=False)
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_DateTime'))
   
    user = relationship('User')
    role = relationship('Role')

class Scalar(Base):

    __tablename__='tScalar'

    data_id = Column(Integer(), primary_key=True, nullable=False)
    param_value = Column(Numeric(asdecimal=True),  nullable=False)

class Scenario(Base):

    __tablename__='tScenario'

    scenario_id = Column(Integer(), primary_key=True, nullable=False)
    scenario_name = Column(String(45),  nullable=False)
    scenario_description = Column(String(1000))
    status = Column(String(1),  nullable=False, default=text(u"'A'"))
    network_id = Column(Integer(), ForeignKey('tNetwork.network_id'))
    start_time = Column(Numeric(precision=30, scale=20))
    end_time = Column(Numeric(precision=30, scale=20))
    time_step = Column(String(60))
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_DateTime'))
    
    network = relationship('Network', backref=backref("scenarios", order_by=scenario_id))

class Template(Base):

    __tablename__='tTemplate'

    template_id = Column(Integer(), primary_key=True, nullable=False)
    template_name = Column(String(45),  nullable=False)
    layout = Column(Text())
    
class TemplateType(Base):

    __tablename__='tTemplateType'

    type_id = Column(Integer(), primary_key=True, nullable=False)
    type_name = Column(String(45),  nullable=False)
    template_id = Column(Integer(), ForeignKey('tTemplate.template_id'))
    alias = Column(String(45))
    layout = Column(Text())
    
    template = relationship('Template', backref=backref("types", order_by=type_id))
    
class TimeSeries(Base):

    __tablename__='tTimeSeries'

    data_id = Column(Integer(), primary_key=True, nullable=False)

class TimeSeriesData(Base):

    __tablename__='tTimeSeriesData'

    data_id = Column(Integer(), ForeignKey('tTimeSeries.data_id'), primary_key=True, nullable=False)
    ts_time = Column(Numeric(precision=30, scale=20), primary_key=True, nullable=False)
    ts_value = Column(LargeBinary(),  nullable=False)
    
    timeseries = relationship('TimeSeries', backref=backref("data", order_by=data_id))

class TypeAttr(Base):

    __tablename__='tTypeAttr'

    attr_id = Column(Integer(), ForeignKey('tAttr.attr_id'), primary_key=True, nullable=False)
    type_id = Column(Integer(), ForeignKey('tTemplateType.type_id'), primary_key=True, nullable=False)
    default_dataset_id = Column(Integer(), ForeignKey('tDataset.dataset_id'))
    attr_is_var = Column(String(1), default=text(u"'N'"))
    data_type = Column(String(45))
    dimension = Column(String(45))
    
    attr = relationship('Attr')
    templatetype = relationship('TemplateType', backref=backref("attrs", order_by=attr_id))
    default_dataset = relationship('Dataset')
    

class User(Base):

    __tablename__='tUser'

    user_id = Column(Integer(), primary_key=True, nullable=False)
    username = Column(String(45),  nullable=False)
    password = Column(String(1000),  nullable=False)
    display_name = Column(String(60),  nullable=False, default=text(u"''"))
    last_login = Column(DateTime())
    last_edit = Column(DateTime())
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_DateTime'))
