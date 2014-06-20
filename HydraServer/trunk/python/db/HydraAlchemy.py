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

from decimal import Decimal

from HydraLib.HydraException import HydraError, ResourceNotFoundError, PermissionError, OwnershipError

from sqlalchemy.orm import relationship, backref

from HydraLib.util import ordinal_to_timestamp

from db import DeclarativeBase as Base, DBSession

from sqlalchemy.sql.expression import case
from sqlalchemy import and_
import logging
log = logging.getLogger(__name__)

def get_timestamp(ordinal):
    if ordinal is None:
        return None
    timestamp = str(ordinal_to_timestamp(ordinal))
    return timestamp


def get_types_by_attr(resource):
    """
        Using the attributes of the resource, get all the
        types that this resource matches.
        @returns a dictionary, keyed on the template name, with the
        value being the list of type names which match the resources
        attributes.
    """

    #Create a list of all of this resources attributes.
    attr_ids = []
    for attr in resource.attributes:
        attr_ids.append(attr.attr_id)
    all_attr_ids = set(attr_ids)

    rs = DBSession.query(
        Template.template_name,
        Template.template_id,
        TemplateType.type_id,
        TemplateType.type_name,
        TypeAttr.attr_id).filter(Template.template_id==TemplateType.template_id,
                                TemplateType.type_id==TypeAttr.type_id).all()

    template_dict   = {}
    type_name_map = {}
    for r in rs:
        type_name_map[r.type_id] = r.type_name

        if template_dict.get(r.template_id):
            if template_dict[r.template_id].get(r.type_id):
                template_dict[r.template_id]['types'][r.type_id].add(r.attr_id)
            else:
                template_dict[r.template_id]['types'][r.type_id] = set([r.attr_id])
        else:
            template_dict[r.template_id] = {
                                        'template_name' : r.template_name,
                                        'types'  : {r.type_id:set([r.attr_id])}
                                     }

    resource_type_templates = {}
    #Find which type IDS this resources matches by checking if all
    #the types attributes are in the resources attribute list.
    for tmpl_id, tmpl in template_dict.items():
        template_name = tmpl['template_name']
        tmpl_types = tmpl['types']
        resource_types = []
        for type_id, type_attrs in tmpl_types.items():
            if type_attrs.issubset(all_attr_ids):
                resource_types.append((type_id, type_name_map[type_id]))

        if len(resource_types) > 0:
            resource_type_templates[tmpl_id] = {'template_name' : template_name,
                                                'types'  : resource_types
                                               }

    return resource_type_templates



#***************************************************
#Data
#***************************************************

class Dataset(Base):

    __tablename__='tDataset'

    dataset_id = Column(Integer(), primary_key=True, nullable=False)
    data_type = Column(String(45),  nullable=False)
    data_units = Column(String(45))
    data_dimen = Column(String(45))
    data_name = Column(String(45),  nullable=False)
    data_hash = Column(BigInteger(),  nullable=False)
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_TIMESTAMP'))
    created_by = Column(Integer(), ForeignKey('tUser.user_id'))
    locked = Column(String(1),  nullable=False, default=text(u"'N'"))
   
    start_time = Column(Numeric(precision=30, scale=20),  nullable=False)
    frequency = Column(Numeric(asdecimal=True),  nullable=False)
    value = Column('value', LargeBinary(),  nullable=False)

    useruser = relationship('User', backref=backref("datasets", order_by=dataset_id))

    def set_metadata(self, metadata_dict):
        if metadata_dict is None:
            return
        existing_metadata = []
        for m in self.metadata:
            existing_metadata.append(m.metadata_name)
            if m.metadata_name in metadata_dict.keys():
                if m.metadata_val != metadata_dict[m.metadata_name]:
                    m.metadata_val = metadata_dict[m.metadata_name]
        
        for k, v in metadata_dict.items():
            if k not in existing_metadata:
                m_i = Metadata(metadata_name=k,metadata_val=v)
                self.metadata.append(m_i)

    def get_val(self, timestamp=None):
        """
            If a timestamp is passed to this function, 
            return the values appropriate to the requested times.

            If the timestamp is *before* the start of the timeseries data, return None
            If the timestamp is *after* the end of the timeseries data, return the last
            value.
        """
        if self.data_type == 'array':
            return list(self.value)
        elif self.data_type == 'descriptor':
            return str(self.value)
        elif self.data_type == 'eqtimeseries':
            return (self.start_time, self.frequency, self.value)
        elif self.data_type == 'scalar':
            return Decimal(str(self.value))
        elif self.data_type == 'timeseries':
            if timestamp is None:
                return self.timeseriesdata
            else:
                ts_val_dict = {}
                for ts in self.timeseriesdata:
                    ts_val_dict[ts.ts_time] = ts.ts_value
                sorted_times = ts_val_dict.keys()
                sorted_times.sort()
                sorted_times.reverse()

                if isinstance(timestamp, list):
                    #return value will now be a list of actual values instead
                    #of a list of tuples.
                    val = []
                    for t in timestamp:
                        for time in sorted_times:
                            if t >= time:
                                val.append(ts_val_dict[time])
                                break
                        else:
                            val.append(None)

                else:
                    for time in sorted_times:
                        if timestamp >= time:
                            val =  ts_val_dict[time]
                            break
                    else:
                        val = None
                return val

    def set_val(self, data_type, val):
        if data_type in ('descriptor','scalar','array'):
            self.value = val
        elif data_type == 'eqtimeseries':
            self.start_time = val[0]
            self.frequency  = val[1]
            self.value      = val[2]
        elif data_type == 'timeseries':
            existing_vals = {}
            for datum in self.timeseriesdata:
                existing_vals[datum.ts_time] = datum
            for time, value in val:
                if time in existing_vals.keys():
                    existing_vals[time].ts_val = value
                else:
                    ts_val = TimeSeriesData()
                    ts_val.ts_time = time
                    ts_val.ts_value = value
                    self.timeseriesdata.append(ts_val)
        else:
            raise HydraError("Invalid data type %s"%(data_type,))

    def set_hash(self, val):

        metadata = self.get_metadata_as_dict()

        hash_string = "%s %s %s %s %s, %s"%(self.data_name,
                                       self.data_units,
                                       self.data_dimen,
                                       self.data_type,
                                       str(val),
                                       metadata)
        data_hash  = hash(hash_string)

        self.data_hash = data_hash
        return data_hash
   
    def get_metadata_as_dict(self):
        metadata = {}
        for r in self.metadata:
            val = r.metadata_val
            try:
                val = eval(r.metadata_val)
            except:
                val = str(r.metadata_val)

            metadata[str(r.metadata_name)] = val

        return metadata

    def set_owner(self, user_id, read='Y', write='Y', share='Y'):
        owner = DatasetOwner()
        owner.dataset_id = self.dataset_id 
        owner.user_id = int(user_id)
        owner.view  = read
        owner.edit  = write
        owner.share = share
        self.owners.append(owner)

        return owner

    def check_read_permission(self, user_id):
        """
            Check whether this user can read this dataset 
        """

        for owner in self.owners:
            if int(owner.user_id) == int(user_id):
                if owner.view == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have read"
                             " access on dataset %s" %
                             (user_id, self.dataset_id))

    def check_user(self, user_id):
        """
            Check whether this user can read this dataset 
        """

        for owner in self.owners:
            if int(owner.user_id) == int(user_id):
                if owner.view == 'Y':
                    return True
        return False

    def check_write_permission(self, user_id):
        """
            Check whether this user can write this dataset
        """

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.edit == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have edit"
                             " access on dataset %s" %
                             (user_id, self.dataset_id))

    def check_share_permission(self, user_id):
        """
            Check whether this user can write this dataset 
        """

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.share == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have share"
                             " access on dataset %s" %
                             (user_id, self.dataset_id))

class DatasetGroup(Base):

    __tablename__='tDatasetGroup'

    group_id = Column(Integer(), primary_key=True, nullable=False)
    group_name = Column(String(45),  nullable=False)
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_TIMESTAMP'))
    
class DatasetGroupItem(Base):

    __tablename__='tDatasetGroupItem'

    group_id = Column(Integer(), ForeignKey('tDatasetGroup.group_id'), primary_key=True, nullable=False)
    dataset_id = Column(Integer(), ForeignKey('tDataset.dataset_id'), primary_key=True, nullable=False)
    
    group = relationship('DatasetGroup', backref=backref("items", order_by=dataset_id))
    dataset = relationship('Dataset', backref=backref("groupitems", order_by=dataset_id))


class TimeSeriesData(Base):

    __tablename__='tTimeSeriesData'

    dataset_id = Column(Integer(), ForeignKey('tDataset.dataset_id'), primary_key=True, nullable=False)
    ts_time = Column(Numeric(precision=30, scale=20), primary_key=True, nullable=False)
    ts_value = Column(LargeBinary(),  nullable=False)
    
    timeseries = relationship('Dataset', backref=backref("timeseriesdata", order_by=dataset_id))

class Metadata(Base):

    __tablename__='tMetadata'

    dataset_id = Column(Integer(), ForeignKey('tDataset.dataset_id'), primary_key=True, nullable=False)
    metadata_name = Column(String(45), primary_key=True, nullable=False)
    metadata_val = Column(LargeBinary(),  nullable=False)
    
    dataset = relationship('Dataset', backref=backref("metadata", order_by=dataset_id))



#********************************************************
#Attributes & Templates
#********************************************************

class Attr(Base):

    __tablename__='tAttr'

    attr_id = Column(Integer(), primary_key=True, nullable=False)
    attr_name = Column(String(45),  nullable=False)
    attr_dimen = Column(String(45))
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_TIMESTAMP'))

class AttrMap(Base):

    __tablename__='tAttrMap'

    attr_id_a = Column(Integer(), ForeignKey('tAttr.attr_id'), primary_key=True, nullable=False)
    attr_id_b = Column(Integer(), ForeignKey('tAttr.attr_id'), primary_key=True, nullable=False)

    attr_a = relationship("Attr", foreign_keys=[attr_id_a], backref=backref('maps_to', order_by=attr_id_a))
    attr_b = relationship("Attr", foreign_keys=[attr_id_b], backref=backref('maps_from', order_by=attr_id_b))

class Template(Base):

    __tablename__='tTemplate'

    template_id = Column(Integer(), primary_key=True, nullable=False)
    template_name = Column(String(45),  nullable=False)
    layout = Column(Text(1000))
    
class TemplateType(Base):

    __tablename__='tTemplateType'

    type_id = Column(Integer(), primary_key=True, nullable=False)
    type_name = Column(String(45),  nullable=False)
    template_id = Column(Integer(), ForeignKey('tTemplate.template_id'))
    alias = Column(String(100))
    layout = Column(Text(1000))
    
    template = relationship('Template', backref=backref("templatetypes", order_by=type_id))
    
class TypeAttr(Base):

    __tablename__='tTypeAttr'

    attr_id = Column(Integer(), ForeignKey('tAttr.attr_id'), primary_key=True, nullable=False)
    type_id = Column(Integer(), ForeignKey('tTemplateType.type_id'), primary_key=True, nullable=False)
    default_dataset_id = Column(Integer(), ForeignKey('tDataset.dataset_id'))
    attr_is_var = Column(String(1), default=text(u"'N'"))
    data_type = Column(String(45))
    dimension = Column(String(45))
    
    attr = relationship('Attr')
    templatetype = relationship('TemplateType', backref=backref("typeattrs", order_by=attr_id))
    default_dataset = relationship('Dataset')
    

class ResourceAttr(Base):

    __tablename__='tResourceAttr'

    resource_attr_id = Column(Integer(), primary_key=True, nullable=False)
    attr_id = Column(Integer(), ForeignKey('tAttr.attr_id'),  nullable=False)
    ref_key = Column(String(45),  nullable=False)
    network_id  = Column(Integer(),  ForeignKey('tNetwork.network_id'), nullable=True,)
    project_id  = Column(Integer(),  ForeignKey('tProject.project_id'), nullable=True,)
    node_id     = Column(Integer(),  ForeignKey('tNode.node_id'), nullable=True)
    link_id     = Column(Integer(),  ForeignKey('tLink.link_id'), nullable=True)
    group_id    = Column(Integer(),  ForeignKey('tResourceGroup.group_id'), nullable=True)
    attr_is_var = Column(String(1),  nullable=False, default=text(u"'N'"))
    
    attr = relationship('Attr')
    project = relationship('Project', backref=backref('attributes', uselist=True), uselist=False, lazy='joined')
    network = relationship('Network', backref=backref('attributes', uselist=True), uselist=False, lazy='joined')
    node = relationship('Node', backref=backref('attributes', uselist=True), uselist=False, lazy='joined')
    link = relationship('Link', backref=backref('attributes', uselist=True), uselist=False, lazy='joined')
    resourcegroup = relationship('ResourceGroup', backref=backref('attributes', uselist=True), uselist=False, lazy='joined')


    def get_resource(self):
        ref_key = self.ref_key
        if ref_key == 'NETWORK':
            return self.network
        elif ref_key == 'NODE':
            return self.node
        elif ref_key == 'LINK':
            return self.link
        elif ref_key == 'GROUP':
            return self.group
        elif ref_key == 'PROJECT':
            return self.project

    def get_resource_id(self):
        ref_key = self.ref_key
        if ref_key == 'NETWORK':
            return self.network_id
        elif ref_key == 'NODE':
            return self.node_id
        elif ref_key == 'LINK':
            return self.link_id
        elif ref_key == 'GROUP':
            return self.group_id
        elif ref_key == 'PROJECT':
            return self.project_id

class ResourceType(Base):

    __tablename__='tResourceType'
    resource_type_id = Column(Integer, primary_key=True, nullable=False)
    type_id = Column(Integer(), ForeignKey('tTemplateType.type_id'), primary_key=False, nullable=False)
    ref_key = Column(String(45),nullable=False)
    network_id  = Column(Integer(),  ForeignKey('tNetwork.network_id'), nullable=True,)
    node_id     = Column(Integer(),  ForeignKey('tNode.node_id'), nullable=True)
    link_id     = Column(Integer(),  ForeignKey('tLink.link_id'), nullable=True)
    group_id    = Column(Integer(),  ForeignKey('tResourceGroup.group_id'), nullable=True)

    
    templatetype = relationship('TemplateType')

    network = relationship('Network', backref=backref('types', uselist=True), uselist=False)
    node = relationship('Node', backref=backref('types', uselist=True), uselist=False)
    link = relationship('Link', backref=backref('types', uselist=True), uselist=False)
    resourcegroup = relationship('ResourceGroup', backref=backref('types', uselist=True), uselist=False)

    def get_resource(self):
        ref_key = self.ref_key
        if ref_key == 'PROJECT':
            return self.project
        elif ref_key == 'NETWORK':
            return self.network
        elif ref_key == 'NODE':
            return self.node
        elif ref_key == 'LINK':
            return self.link
        elif ref_key == 'GROUP':
            return self.group

    def get_resource_id(self):
        ref_key = self.ref_key
        if ref_key == 'PROJECT':
            return self.project_id
        elif ref_key == 'NETWORK':
            return self.network_id
        elif ref_key == 'NODE':
            return self.node_id
        elif ref_key == 'LINK':
            return self.link_id
        elif ref_key == 'GROUP':
            return self.group_id

#*****************************************************
# Topology & Scenarios
#*****************************************************

class Project(Base):

    __tablename__='tProject'
    ref_key = 'PROJECT'

    project_id = Column(Integer(), primary_key=True, nullable=False)
    project_name = Column(String(45),  nullable=False)
    project_description = Column(String(1000))
    status = Column(String(1),  nullable=False, default=text(u"'A'"))
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_TIMESTAMP'))
    created_by = Column(Integer(), ForeignKey('tUser.user_id'))
    
    user = relationship('User', backref=backref("projects", order_by=project_id))

    def get_attributes(self):
        project_ras = DBSession.query(ResourceAttr.resource_attr_id).filter(ResourceAttr.project_id==1).all()
        project_rs = DBSession.query(ResourceScenario).filter(ResourceScenario.resource_attr_id in project_ras).all()

        return project_rs

    def add_attribute(self, attr_id, attr_is_var='N'):
        attr = ResourceAttr()
        attr.attr_id = attr_id
        attr.attr_is_var = attr_is_var
        attr.ref_key = self.ref_key
        attr.project_id  = self.project_id
        self.attributes.append(attr)

        return attr

    def set_owner(self, user_id, read='Y', write='Y', share='Y'):
        owner = ProjectOwner()
        owner.project_id = self.project_id 
        owner.user_id = int(user_id)
        owner.view  = read
        owner.edit  = write
        owner.share = share
        self.owners.append(owner)

        return owner

    def check_read_permission(self, user_id):
        """
            Check whether this user can read this project
        """

        for owner in self.owners:
            if int(owner.user_id) == int(user_id):
                if owner.view == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have read"
                             " access on project %s" %
                             (user_id, self.project_id))

    def check_write_permission(self, user_id):
        """
            Check whether this user can write this project
        """

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.edit == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have edit"
                             " access on project %s" %
                             (user_id, self.project_id))

    def check_share_permission(self, user_id):
        """
            Check whether this user can write this project
        """

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.share == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have share"
                             " access on project %s" %
                             (user_id, self.project_id))



class Network(Base):

    __tablename__='tNetwork'
    ref_key = 'NETWORK'

    network_id = Column(Integer(), primary_key=True, nullable=False)
    network_name = Column(String(45),  nullable=False)
    network_description = Column(String(1000))
    network_layout = Column(Text(1000))
    project_id = Column(Integer(), ForeignKey('tProject.project_id'),  nullable=False)
    status = Column(String(1),  nullable=False, default=text(u"'A'"))
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_TIMESTAMP'))
    projection = Column(String(1000))
    created_by = Column(Integer(), ForeignKey('tUser.user_id'))
    
    project = relationship('Project', backref=backref("networks", order_by=network_id))

    def add_attribute(self, attr_id, attr_is_var='N'):
        attr = ResourceAttr()
        attr.attr_id = attr_id
        attr.attr_is_var = attr_is_var
        attr.ref_key = self.ref_key
        attr.network_id  = self.network_id
        self.attributes.append(attr)

        return attr

    def add_link(self, name, desc, layout, node_1, node_2):
        """
            Add a link to a network. Links are what effectively
            define the network topology, by associating two already
            existing nodes.
        """
        l = Link()
        l.link_name        = name
        l.link_description = desc
        l.link_layout      = str(layout)
        l.node_a           = node_1
        l.node_b           = node_2

        DBSession.add(l)
        
        self.links.append(l)

        return l


    def add_node(self, name, desc, layout, node_x, node_y):
        """
            Add a node to a network.
        """
        node = Node()
        node.node_name        = name
        node.node_description = desc
        node.node_layout      = str(layout)
        node.node_x           = node_x
        node.node_y           = node_y

        #Do not call save here because it is likely that we may want
        #to bulk insert nodes, not one at a time.

        DBSession.add(node)
        
        self.nodes.append(node)

        return node

    def add_group(self, name, desc, status):
        """
            Add a new group to a network.
        """
        group_i                      = ResourceGroup()
        group_i.group_name        = name
        group_i.group_description = desc
        group_i.status            = status

        DBSession.add(group_i)
        
        self.resourcegroups.append(group_i)


        return group_i

    def set_owner(self, user_id, read='Y', write='Y', share='Y'):
        owner = NetworkOwner()
        owner.network_id = self.network_id 
        owner.user_id = int(user_id)
        owner.view  = read
        owner.edit  = write
        owner.share = share
        self.owners.append(owner)

        return owner

    def check_read_permission(self, user_id):
        """
            Check whether this user can read this network 
        """

        for owner in self.owners:
            if int(owner.user_id) == int(user_id):
                if owner.view == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have read"
                             " access on network %s" %
                             (user_id, self.network_id))

    def check_write_permission(self, user_id):
        """
            Check whether this user can write this project
        """

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.edit == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have edit"
                             " access on network %s" %
                             (user_id, self.network_id))

    def check_share_permission(self, user_id):
        """
            Check whether this user can write this project
        """

        for owner in self.owners:
            if owner.user_id == int(user_id):
                if owner.view == 'Y' and owner.share == 'Y':
                    break
        else:
            raise PermissionError("Permission denied. User %s does not have share"
                             " access on network %s" %
                             (user_id, self.network_id))

class Link(Base):

    __tablename__='tLink'

    ref_key = 'LINK'

    link_id = Column(Integer(), primary_key=True, nullable=False)
    network_id = Column(Integer(), ForeignKey('tNetwork.network_id'), nullable=False)
    status = Column(String(1),  nullable=False, default=text(u"'A'"))
    node_1_id = Column(Integer(), ForeignKey('tNode.node_id'), nullable=False)
    node_2_id = Column(Integer(), ForeignKey('tNode.node_id'), nullable=False)
    link_name = Column(String(45))
    link_description = Column(String(1000))
    link_layout = Column(Text(1000))
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_TIMESTAMP'))
   
    network = relationship('Network', backref=backref("links", order_by=network_id), lazy='joined')
    node_a = relationship('Node', foreign_keys=[node_1_id], backref=backref("links_to", order_by=link_id))
    node_b = relationship('Node', foreign_keys=[node_2_id], backref=backref("links_from", order_by=link_id))

    def add_attribute(self, attr_id, attr_is_var='N'):
        attr = ResourceAttr()
        attr.attr_id = attr_id
        attr.attr_is_var = attr_is_var
        attr.ref_key = self.ref_key
        attr.link_id  = self.link_id
        self.attributes.append(attr)

        return attr

class Node(Base):

    __tablename__='tNode'
    ref_key = 'NODE'

    node_id = Column(Integer(), primary_key=True, nullable=False)
    network_id = Column(Integer(), ForeignKey('tNetwork.network_id'), nullable=False)
    node_description = Column(String(1000))
    node_name = Column(String(45),  nullable=False)
    status = Column(String(1),  nullable=False, default=text(u"'A'"))
    node_x = Column(Numeric(asdecimal=True))
    node_y = Column(Numeric(asdecimal=True))
    node_layout = Column(Text(1000))
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_TIMESTAMP'))
    
    network = relationship('Network', backref=backref("nodes", order_by=network_id), lazy='joined')

    def add_attribute(self, attr_id, attr_is_var='N'):
        attr = ResourceAttr()
        attr.attr_id = attr_id
        attr.attr_is_var = attr_is_var
        attr.ref_key = self.ref_key
        attr.node_id  = self.node_id
        self.attributes.append(attr)

        return attr

class ResourceGroup(Base):

    __tablename__='tResourceGroup'

    ref_key = 'GROUP'
    group_id = Column(Integer(), primary_key=True, nullable=False)
    group_name = Column(String(45),  nullable=False)
    group_description = Column(String(1000))
    status = Column(String(1))
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_TIMESTAMP'))
    network_id = Column(Integer(), ForeignKey('tNetwork.network_id'),  nullable=False)
    
    network = relationship('Network', backref=backref("resourcegroups", order_by=group_id), lazy='joined')
    
    def add_attribute(self, attr_id, attr_is_var='N'):
        attr = ResourceAttr()
        attr.attr_id = attr_id
        attr.attr_is_var = attr_is_var
        attr.ref_key = self.ref_key
        attr.group_id  = self.group_id
        self.attributes.append(attr)

        return attr
    
class ResourceGroupItem(Base):

    __tablename__='tResourceGroupItem'

    item_id = Column(Integer(), primary_key=True, nullable=False)
    ref_key = Column(String(45),  nullable=False)

    node_id     = Column(Integer(),  ForeignKey('tNode.node_id'), nullable=False)
    link_id     = Column(Integer(),  ForeignKey('tLink.link_id'), nullable=False)
    subgroup_id = Column(Integer(),  ForeignKey('tResourceGroup.group_id'), nullable=False)

    group_id = Column(Integer(), ForeignKey('tResourceGroup.group_id'), nullable=False)
    scenario_id = Column(Integer(), ForeignKey('tScenario.scenario_id'),  nullable=False)
    
    group = relationship('ResourceGroup', foreign_keys=[group_id], backref=backref("items", order_by=group_id))
    scenario = relationship('Scenario', backref=backref("resourcegroupitems", order_by=item_id))

    node = relationship('Node')
    link = relationship('Link')
    subgroup = relationship('ResourceGroup', foreign_keys=[subgroup_id])


    def get_resource(self):
        ref_key = self.ref_key
        if ref_key == 'NODE':
            return self.node
        elif ref_key == 'LINK':
            return self.link
        elif ref_key == 'GROUP':
            return self.subgroup

    def get_resource_id(self):
        ref_key = self.ref_key
        if ref_key == 'NODE':
            return self.node_id
        elif ref_key == 'LINK':
            return self.link_id
        elif ref_key == 'GROUP':
            return self.subgroup_id

class ResourceScenario(Base):

    __tablename__='tResourceScenario'

    dataset_id = Column(Integer(), ForeignKey('tDataset.dataset_id'), nullable=False)
    scenario_id = Column(Integer(), ForeignKey('tScenario.scenario_id'), primary_key=True, nullable=False)
    resource_attr_id = Column(Integer(), ForeignKey('tResourceAttr.resource_attr_id'), primary_key=True, nullable=False)
    
    dataset      = relationship('Dataset', backref=backref("resourcescenarios", order_by=dataset_id))
    scenario     = relationship('Scenario', backref=backref("resourcescenarios", order_by=resource_attr_id, lazy='joined'))
    resourceattr = relationship('ResourceAttr', lazy='joined')


    def get_dataset(self, user_id):
        dataset = DBSession.query(Dataset.dataset_id,
                Dataset.data_type,
                Dataset.data_units,
                Dataset.data_dimen,
                Dataset.data_name,
                Dataset.locked,
                case([(and_(Dataset.locked=='Y', DatasetOwner.user_id is not None), None)], 
                        else_=Dataset.start_time).label('start_time'),
                case([(and_(Dataset.locked=='Y', DatasetOwner.user_id is not None), None)], 
                        else_=Dataset.frequency).label('frequency'),
                case([(and_(Dataset.locked=='Y', DatasetOwner.user_id is not None), None)], 
                        else_=Dataset.value).label('value')).filter(
                Dataset.dataset_id==self.dataset_id).outerjoin(DatasetOwner, 
                                    and_(Dataset.dataset_id==DatasetOwner.dataset_id, 
                                    DatasetOwner.user_id==user_id)).one()

        return dataset

class Scenario(Base):

    __tablename__='tScenario'

    scenario_id = Column(Integer(), primary_key=True, nullable=False)
    scenario_name = Column(String(45),  nullable=False)
    scenario_description = Column(String(1000))
    status = Column(String(1),  nullable=False, default=text(u"'A'"))
    network_id = Column(Integer(), ForeignKey('tNetwork.network_id'))
    start_time = Column(Numeric(precision=30, scale=20))
    end_time = Column(Numeric(precision=30, scale=20))
    locked = Column(String(1),  nullable=False, default=text(u"'N'"))
    time_step = Column(String(60))
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_TIMESTAMP'))
    
    network = relationship('Network', backref=backref("scenarios", order_by=scenario_id))

    def add_resource_scenario(self, resource_attr, dataset=None):
        rs_i = ResourceScenario()
        rs_i.resource_attr_id = resource_attr.resource_attr_id
        rs_i.dataset_id       = dataset.dataset_id
        rs_i.dataset = dataset
        rs_i.resourceattr = resource_attr 
        self.resourcescenarios.append(rs_i)

    def add_resourcegroup_item(self, ref_key, resource, group_id):
        group_item_i = ResourceGroupItem()
        group_item_i.group_id = group_id
        group_item_i.ref_key  = ref_key
        if ref_key == 'GROUP':
            group_item_i.subgroup = resource
        elif ref_key == 'NODE':
            group_item_i.node     = resource
        elif ref_key == 'LINK':
            group_item_i.link     = resource
        self.resourcegroupitems.append(group_item_i)
#***************************************************
#Ownership & Permissions
#***************************************************
class ProjectOwner(Base):

    __tablename__='tProjectOwner'

    user_id = Column(Integer(), ForeignKey('tUser.user_id'), primary_key=True, nullable=False)
    project_id = Column(Integer(), ForeignKey('tProject.project_id'), primary_key=True, nullable=False)
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_TIMESTAMP'))
    view = Column(String(1),  nullable=False)
    edit = Column(String(1),  nullable=False)
    share = Column(String(1),  nullable=False)
    
    user = relationship('User')
    project = relationship('Project', backref=backref('owners', order_by=user_id, uselist=True))

class NetworkOwner(Base):

    __tablename__='tNetworkOwner'

    user_id = Column(Integer(), ForeignKey('tUser.user_id'), primary_key=True, nullable=False)
    network_id = Column(Integer(), ForeignKey('tNetwork.network_id'), primary_key=True, nullable=False)
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_TIMESTAMP'))
    view = Column(String(1),  nullable=False)
    edit = Column(String(1),  nullable=False)
    share = Column(String(1),  nullable=False)
    
    user = relationship('User')
    network = relationship('Network', backref=backref('owners', order_by=user_id, uselist=True))

class DatasetOwner(Base):

    __tablename__='tDatasetOwner'

    user_id = Column(Integer(), ForeignKey('tUser.user_id'), primary_key=True, nullable=False)
    dataset_id = Column(Integer(), ForeignKey('tDataset.dataset_id'), primary_key=True, nullable=False)
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_TIMESTAMP'))
    view = Column(String(1),  nullable=False)
    edit = Column(String(1),  nullable=False)
    share = Column(String(1),  nullable=False)
    
    user = relationship('User')
    dataset = relationship('Dataset', backref=backref('owners', order_by=user_id, uselist=True))

class Perm(Base):

    __tablename__='tPerm'

    perm_id = Column(Integer(), primary_key=True, nullable=False)
    perm_code = Column(String(45),  nullable=False)
    perm_name = Column(String(45),  nullable=False)
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_TIMESTAMP'))
    roleperms = relationship('RolePerm', lazy='joined')
    
class Role(Base):

    __tablename__='tRole'

    role_id = Column(Integer(), primary_key=True, nullable=False)
    role_code = Column(String(45),  nullable=False)
    role_name = Column(String(45),  nullable=False)
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_TIMESTAMP'))
    roleperms = relationship('RolePerm', lazy='joined', cascade='all')
    roleusers = relationship('RoleUser', lazy='joined', cascade='all')
    
class RolePerm(Base):

    __tablename__='tRolePerm'

    perm_id = Column(Integer(), ForeignKey('tPerm.perm_id'), primary_key=True, nullable=False)
    role_id = Column(Integer(), ForeignKey('tRole.role_id'), primary_key=True, nullable=False)
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_TIMESTAMP'))
    
    perm = relationship('Perm', lazy='joined')
    role = relationship('Role', lazy='joined')
    
class RoleUser(Base):

    __tablename__='tRoleUser'

    user_id = Column(Integer(), ForeignKey('tUser.user_id'), primary_key=True, nullable=False)
    role_id = Column(Integer(), ForeignKey('tRole.role_id'), primary_key=True, nullable=False)
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_TIMESTAMP'))
   
    user = relationship('User', lazy='joined')
    role = relationship('Role', lazy='joined')

class User(Base):

    __tablename__='tUser'

    user_id = Column(Integer(), primary_key=True, nullable=False)
    username = Column(String(45),  nullable=False)
    password = Column(String(1000),  nullable=False)
    display_name = Column(String(60),  nullable=False, default=text(u"''"))
    last_login = Column(DateTime())
    last_edit = Column(DateTime())
    cr_date = Column(DateTime(),  nullable=False, default=text(u'CURRENT_TIMESTAMP'))
    roleusers = relationship('RoleUser', lazy='joined')
