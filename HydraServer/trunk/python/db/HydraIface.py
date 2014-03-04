import logging
import datetime
from decimal import Decimal
from HydraLib.util import convert_ordinal_to_datetime

from HydraLib.HydraException import HydraError

import IfaceLib
from IfaceLib import IfaceBase, execute

def init(cnx):
    IfaceLib.init(cnx, db_hierarchy)

def add_dataset(data_type, val, units, dimension, name="", dataset_id=None, user_id=None):
    """
        Data can exist without scenarios. This is the mechanism whereby
        single pieces of data can be added without doing it through a scenario.

        A typical use of this would be for setting default values on types.
    """

    if data_type == 'scalar':
        val = Decimal(val)

    hash_string = "%s %s %s %s %s"
    data_hash  = hash(hash_string%(name, units, dimension, data_type, str(val)))

    existing_dataset_id = get_data_from_hash(data_hash)

    if existing_dataset_id is not None:
        return existing_dataset_id
    else:
        d = Dataset(dataset_id=dataset_id)

        d.set_val(data_type, val)

        d.db.data_type  = data_type
        d.db.data_units = units
        d.db.data_name  = name
        d.db.data_dimen = dimension
        d.db.data_hash  = data_hash
        d.db.created_by = user_id
        d.save()

        return d.db.dataset_id

def get_data_from_hash(data_hash):
    sql = """
        select
            dataset_id
        from
            tDataset
        where
            data_hash = %s
    """ % (data_hash)

    rs = execute(sql)

    if len(rs) > 0:
        return rs[0].dataset_id
    else:
        return None

class GenericResource(IfaceBase):
    """
        A superclass for all 'resource' types -- Network, Node, Link, Scenario,
        Group and Project.
    """
    def __init__(self, parent, class_name, ref_key, ref_id=None):
        self.parent = parent

        self.ref_key = ref_key
        self.ref_id  = ref_id

        IfaceBase.__init__(self, parent, class_name)

        self.attributes = self.get_attributes()

    def save(self):
        super(GenericResource, self).save()
        pk = self.db.__getattr__(self.db.pk_attrs[0])
        self.ref_id = pk

    def load(self):
        result = super(GenericResource, self).load()

        self.get_ref_id()

        return result

    def get_ref_id(self):
        pk = self.db.__getattr__(self.db.pk_attrs[0])
        self.ref_id = pk
        return self.ref_id


    def delete(self, purge_data=True):
        for attr in self.attributes:
            attr.delete(purge_data=purge_data)

        super(GenericResource, self).delete()

    def get_attributes(self):
        """
            Get the resource attributes for this resource.
            @returns list of ResourceAttr objects.
        """

        if self.ref_id is None:
            return []
        attributes = []
        sql = """
                    select
                        attr_id,
                        resource_attr_id
                    from
                        tResourceAttr
                    where
                        ref_id = %(ref_id)s
                    and ref_key = '%(ref_key)s'
            """ % dict(ref_key = self.ref_key, ref_id = self.ref_id)

        rs = execute(sql)

        for r in rs:
            ra = ResourceAttr(resource_attr_id=r.resource_attr_id)
            ra.load()
            attributes.append(ra)

        self.attributes = attributes

        return attributes

    def add_attribute(self, attr_id, attr_is_var='N'):
        """
            Get a Resource attribute with give attr ID.
        """
        attr = ResourceAttr()
        attr.db.attr_id = attr_id
        attr.db.attr_is_var = attr_is_var
        attr.db.ref_key = self.ref_key
        attr.db.ref_id  = self.get_ref_id()
    #    attr.save()
        self.attributes.append(attr)

        return attr

    def get_data_from_hash(self, data_hash):
        sql = """
            select
                dataset_id
            from
                tDataset
            where
                data_hash = %s
        """ % (data_hash)

        rs = execute(sql)

        if len(rs) > 0:
            return rs[0].dataset_id
        else:
            return None

    def assign_value(self, scenario_id, resource_attr_id, data_type, val,
                     units, name, dimension, new=False, user_id=None):
        """
            Insert or update a piece of data in a scenario. the 'new' flag
            indicates that the data is new, thus allowing us to avoid unnecessary
            queries for non-existant data. If this flag is not True, a check
            will be performed in the DB for its existance.
        """

        #cater for a project -- if the scenario ID is null, set it to 1.
        if scenario_id is None:
            scenario_id = 1

        if scenario_id == 1 and self.name != 'Project':
            raise HydraError("An error has occurred while setting"
                             "resource attribute %s this data."
                             "Scenario 1 is reserved for project attributes."
                             %(resource_attr_id))


        rs = ResourceScenario()
        rs.db.scenario_id=scenario_id
        rs.db.resource_attr_id=resource_attr_id
        rs.load()

        hash_string = "%s %s %s %s %s"%(name, units, dimension, data_type, str(val))
        data_hash  = hash(hash_string)
        existing_dataset_id = get_data_from_hash(data_hash)

        if existing_dataset_id is not None:
            rs.db.dataset_id = existing_dataset_id
        else:
            dataset_id = None
            #if this is definitely not new data, fetch the
            #dataset id from the DB.
            if new is not True:
                data_in_db = rs.load()
                #Was the 'new' flag correct?
                if data_in_db is True:
                    dataset_id = rs.db.dataset_id

            dataset_id = add_dataset(data_type,
                                     val,
                                     units,
                                     dimension,
                                     name=name,
                                     dataset_id=dataset_id,
                                     user_id=user_id)

            rs.db.dataset_id = dataset_id

        rs.save()

        return rs


    def get_types_by_attr(self):
        """
            Using the attributes of the resource, get all the
            types that this resource matches.
            @returns a dictionary, keyed on the template name, with the
            value being the list of type names which match the resources
            attributes.
        """

        #Create a list of all of this resources attributes.
        attr_ids = []
        for attr in self.get_attributes():
            attr_ids.append(attr.db.attr_id)
        all_attr_ids = set(attr_ids)

        #Get all template information.
        template_sql = """
            select
                tmpl.template_name,
                tmpl.template_id,
                typ.type_id,
                typ.type_name,
                tattr.attr_id
            from
                tTemplate tmpl,
                tTemplateType typ,
                tTypeAttr tattr
            where
                tmpl.template_id   = typ.template_id
                and typ.type_id = tattr.type_id
        """
        rs = execute(template_sql)

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

    def get_templates_and_types(self):
        """
        """
        sql = """
            select
                rt.type_id,
                type.type_name,
                tmpl.template_name,
                tmpl.template_id
            from
                tResourceType rt,
                tTemplateType type,
                tTemplate tmpl
            where
                rt.ref_key       = '%s'
            and rt.ref_id        = %s
            and rt.type_id       = type.type_id
            and tmpl.template_id = type.template_id
        """ % (self.ref_key, self.ref_id)

        rs = execute(sql)

        template_dict   = {}
        for r in rs:
            if template_dict.get(r.template_id):
                template_dict[r.template_id]['types'].append((r.type_id, r.type_name))
            else:
                template_dict[r.template_id] = {
                                            'template_name' : r.template_name,
                                            'types'  : [(r.type_id, r.type_name)]
                                         }

        return template_dict

    def get_as_dict(self, **kwargs):
        """
            Turns object into a dictionary structure so it can be easily
            transformed into soap objects in the soap library
        """
        time = kwargs.get('time', False)

        if time is True:
            start = datetime.datetime.now()

        obj_dict = super(GenericResource, self).get_as_dict(**kwargs)

        obj_dict.update(
            dict(
                attributes  = [],
                types   = [],
            )
        )

        if self.attributes == []:
            self.get_attributes()

        for attr in self.attributes:
            if self.name == 'Project':
                rs_i = ResourceScenario(scenario_id = 1, resource_attr_id=attr.db.resource_attr_id)
                obj_dict['attributes'].append(rs_i.get_as_dict(**kwargs))
            else:
                obj_dict['attributes'].append(attr.get_as_dict(**kwargs))

        #It is not true to the structure of the DB...
        templates = self.get_templates_and_types()

        type_list = []
        for template_id, template in templates.items():
            template_name = template['template_name']
            types  = template['types']

            for type_id, type_name in types:
                type_summary = dict(
                    object_type = "TypeSummary",
                    template_id      = template_id,
                    template_name    = template_name,
                    type_id = type_id,
                    type_name = type_name,
                    )
                type_list.append(type_summary)
        obj_dict['types'] = type_list

        if time:
            logging.info("get_as_dict of %s took: %s ", self.name, datetime.datetime.now()-start)

        return obj_dict

    def set_ownership(self, user_id, read='Y', write='Y', share='Y'):
        owner = Owner()
        owner.db.ref_key = self.ref_key

        if self.ref_key not in ('PROJECT', 'NETWORK'):
            raise HydraError("Only resources of type NETWORK and PROJECT may have an owner.")

        owner.db.ref_id = self.ref_id
        owner.db.user_id = int(user_id)
        owner.load()
        owner.db.view  = read
        owner.db.edit  = write
        owner.db.share = share

        owner.save()

        return owner

    def get_owners(self):
        if self.ref_key not in ('PROJECT', 'NETWORK'):
            return []

        sql = """
            select
                user_id
            from
                tOwner
            where
                ref_key = '%s'
            and ref_id  = %s
        """%(self.ref_key, self.ref_id)

        rs = execute(sql)

        return [r.user_id for r in rs]

    def check_read_permission(self, user_id):
        """
            Check whether this user can read this network
        """

        if self.ref_key not in ('PROJECT', 'NETWORK'):
            return

        sql = """
            select
                user_id
            from
                tOwner
            where
                ref_key = '%s'
            and ref_id  = %s
            and view    = 'Y'
            and user_id = %s
        """%(self.ref_key, self.ref_id, user_id)

        rs = execute(sql)

        if len(rs) == 0:
            raise HydraError("Permission denied. User %s does not have read"
                             " access on %s %s" %
                             (user_id, self.ref_key, self.ref_id))

    def check_write_permission(self, user_id):
        """
            Check whether this user can read this network
        """

        if self.ref_key not in ('PROJECT', 'NETWORK'):
            return

        sql = """
            select
                user_id
            from
                tOwner
            where
                ref_key = '%s'
            and ref_id  = %s
            and view    = 'Y'
            and edit    = 'Y'
            and user_id = %s
        """%(self.ref_key, self.ref_id, user_id)

        rs = execute(sql)

        if len(rs) == 0:
            raise HydraError("Permission denied. User %s does not have write"
                             " access on %s %s" %
                             (user_id, self.ref_key, self.ref_id))

    def check_share_permission(self, user_id):
        """
            Check whether this user can read this network
        """

        if self.ref_key not in ('PROJECT', 'NETWORK'):
            return

        sql = """
            select
                user_id
            from
                tOwner
            where
                ref_key = '%s'
            and ref_id  = %s
            and view    = 'Y'
            and share   = 'Y'
            and user_id = %s
        """%(self.ref_key, self.ref_id, user_id)

        rs = execute(sql)

        if len(rs) == 0:
            raise HydraError("Permission denied. User %s does not have share"
                             " access on %s %s" %
                             (user_id, self.ref_key, self.ref_id))

class Project(GenericResource):
    """
        A logical container for a piece of work.
        Contains networks and scenarios.

        A project cannot have scenarios (that's what networks are for), but
        they can have attributes. The way around this is to set aside
        scenario 1 as the container for all project data.

    """
    def __init__(self, project_id = None):
        GenericResource.__init__(self, None, self.__class__.__name__, 'PROJECT', ref_id=project_id)

        self.db.project_id = project_id
        self.networks = []
        if project_id is not None:
            self.load()


class Scenario(GenericResource):
    """
        A set of nodes and links
    """
    def __init__(self, network = None, scenario_id = None):
        GenericResource.__init__(self, network, self.__class__.__name__, 'SCENARIO', ref_id=scenario_id)

        self.db.scenario_id = scenario_id
        if scenario_id is not None:
            self.load()

    def get_resourcegroupitems(self):
        sql = """
            select
                item_id
            from
                tResourceGroupItem
            where
                scenario_id = %s
        """ % (self.db.scenario_id)

        rs = execute(sql)
        resourcegroupitems = []
        for r in rs:
            item_i = ResourceGroupItem(item_id = r.item_id)
            resourcegroupitems.append(item_i)
        self.resourcegroupitems = resourcegroupitems
        return self.resourcegroupitems

    def get_as_dict(self, **kwargs):
        obj_dict = super(Scenario, self).get_as_dict(**kwargs)

        if self.db.start_time is not None:
            obj_dict['start_time'] = self.get_timestamp(self.db.start_time)
        if self.db.end_time is not None:
            obj_dict['end_time']   = self.get_timestamp(self.db.end_time)

        dict_items = []
        for rgi in self.get_resourcegroupitems():
            dict_items.append(rgi.get_as_dict(**kwargs))

        obj_dict['resourcegroupitems'] = dict_items

        return obj_dict

    def get_resource_items(self):
        self.resourcegroupitems = []
        sql = """
            select
                item_id
            from
                tResourceGroupItem
            where
                scenario_id = %s
        """ % (self.db.scenario_id)

        rs = execute(sql)

        for r in rs:
            item_i = ResourceGroupItem(item_id = r.item_id)
            item_i.load()
            self.resourcegroupitems.append(item_i)

        return self.resourcegroupitems

    def get_timestamp(self, ordinal):
        timestamp = convert_ordinal_to_datetime(ordinal)
        timestamp = self.time_format.format(timestamp.year,
                                            timestamp.month,
                                            timestamp.day,
                                            timestamp.hour,
                                            timestamp.minute,
                                            timestamp.second,
                                            timestamp.microsecond)
        return timestamp

class Network(GenericResource):
    """
        A set of nodes and links
    """
    def __init__(self, project = None, network_id = None):
        GenericResource.__init__(self,project, self.__class__.__name__, 'NETWORK', ref_id=network_id)

        self.project = project
        self.db.network_id = network_id
        self.nodes = []
        self.links = []
        self.resourcegroups = []
        if network_id is not None:
            self.load()

    def load_all(self, scenario_ids=[]):
        """
            Overwrite the base load_all function
            to allow specific scenarios to be specified.
            THis is done to aid in performance, so unwanted
            data is not loaded.
        """
        if scenario_ids is not None and len(scenario_ids) > 0:
            #Remove scenarios as children of this network so they
            #are not all loaded in load all. Then only load the ones
            #we are interested in.
            all_children = self.child_info.copy()
            del(all_children['tScenario'])
            self.children = all_children

            starttime = datetime.datetime.now()
            load_ok = super(Network, self).load_all()
            endtime = datetime.datetime.now()
            logging.info('#-- super(Network, self).load_all(): %s' % str(endtime-starttime))

            if load_ok is False:
                return False

            scenarios = []
            for s_id in scenario_ids:
                s_i = Scenario(self, scenario_id=s_id)
                if s_i.db.network_id != self.db.network_id:
                    raise HydraError("Scenario %s is not in network %s"%
                                    (s_id, self.db.network_id))
                s_i.load_all()
                scenarios.append(s_i)
            self.scenarios = scenarios
            return True
        else:
            self.child_info = self.get_children()
            return super(Network, self).load_all()

    def get_all_scenarios(self):
        """
            Return all scenarios in a network
        """
        self.load_all()
        return self.scenarios

    def get_scenarios(self, scenario_ids):
        """
            Get the scenarios for this network
        """
        scenarios = []
        for scenario_id in scenario_ids:
            s_i = Scenario(scenario_id=scenario_id)
            if s_i.db.network_id != self.db.network_id:
                raise HydraError("Scenario %s is not in network %s"%
                                 (scenario_id, self.db.network_id))
            scenarios.append(s_i)

        return scenarios

    def add_link(self, name, desc, layout, node_1_id, node_2_id):
        """
            Add a link to a network. Links are what effectively
            define the network topology, by associating two already
            existing nodes.
        """
        l = Link(network=self)
        l.db.link_name        = name
        l.db.link_description = desc
        l.db.link_layout      = layout
        l.db.node_1_id        = node_1_id
        l.db.node_2_id        = node_2_id
        l.db.network_id       = self.db.network_id

        #Do not call save here because it is likely that we may want
        #to bulk insert links, not one at a time.

        self.links.append(l)

        return l


    def add_node(self, name, desc, layout, node_x, node_y):
        """
            Add a node to a network.
        """
        node_i = Node(network=self)
        node_i.db.node_name        = name
        node_i.db.node_description = desc
        node_i.db.node_layout      = layout
        node_i.db.node_x           = node_x
        node_i.db.node_y           = node_y
        node_i.db.network_id       = self.db.network_id

        #Do not call save here because it is likely that we may want
        #to bulk insert nodes, not one at a time.

        self.nodes.append(node_i)

        return node_i

    def add_group(self, name, desc, status):
        """
            Add a new group to a network.
        """
        group_i                      = ResourceGroup(network=self)
        group_i.db.group_name        = name
        group_i.db.group_description = desc
        group_i.db.status            = status
        group_i.db.network_id        = self.db.network_id

        self.resourcegroups.append(group_i)

        return group_i

    def get_link(self, link_id):
        """
        Return a link with id link_id if it is in this
        network
        """
        link = None
        for l in self.links:
                if l.db.link_id == link_id:
                    l.load()
                    link = l
                    break

        return link

    def get_node(self, node_id):
        """
        Return node object with id node_id if it is
        in this network.
        """
        node = None
        for n in self.nodes:
            if n.db.node_id == node_id:
                node = n
                node.load()
                break

        return node

    def get_group(self, group_id):
        """
            Return group object with group_id if it is in this network
        """
        group_i = None
        for g in self.resourcegroups:
            if g.db.group_id == group_id:
                group_i = g
                group_i.load()
                break

        return group_i

class Node(GenericResource):
    """
        Representation of a resource.
    """
    def __init__(self, network = None, node_id = None):
        GenericResource.__init__(self,network, self.__class__.__name__, 'NODE', ref_id=node_id)

        self.network = network
        self.db.node_id = node_id
        if node_id is not None:
            self.load()

class Link(GenericResource):
    """
        Representation of a connection between nodes.
    """
    def __init__(self,network = None, link_id = None):
        GenericResource.__init__(self,network, self.__class__.__name__, 'LINK', ref_id=link_id)

        self.db.link_id = link_id
        self.network    = network
        if link_id is not None:
            self.load()

class ResourceGroup(GenericResource):
    """
        A container for references to nodes and links.
        Used by models when constructing constraints.
    """
    def __init__(self, network = None, group_id = None):
        GenericResource.__init__(self, network, self.__class__.__name__, 'GROUP', ref_id=group_id)

        self.db.group_id = group_id
        if group_id is not None:
            self.load()

class ResourceGroupItem(IfaceBase):
    """
        A set of nodes and links
    """
    def __init__(self, group = None, item_id=None, group_id=None, ref_key = None, ref_id = None):
        IfaceBase.__init__(self, group, self.__class__.__name__)

        self.db.item_id = item_id
        self.db.ref_key = ref_key
        self.db.ref_id = ref_id
        self.db.group_id = group_id

        if item_id is None and None not in (group_id, ref_key, ref_id):
            self.get_pk()

        if self.db.item_id is not None:
            self.load()

    def get_pk(self):
        sql = """
            select
               item_id
            from
                tResourceGroupItem
            where
                group_id = %s
            and ref_key  = '%s'
            and ref_id   = %s
        """ % (self.db.group_id, self.db.ref_key, self.db.ref_id)

        rs = execute(sql)
        if len(rs) > 0:
            self.db.item = rs[0].item
        else:
            return None

        return self.db.resource_group_id

class Attr(IfaceBase):
    """
        An attribute is a piece of data, with a name and a dimension.
        For example, an attribute might be 'volume' and 'metres-cubed'
        A piece of information is associated with a resource using a resource
        attribute.
    """
    def __init__(self, attr_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.attr_id = attr_id

        if attr_id is not None:
            self.load()

class AttrMap(IfaceBase):
    """
       Defines equality between attributes ('volume' is equivalent to 'vol')
    """
    def __init__(self, attr_id_a = None, attr_id_b = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.attr_id_a = attr_id_a
        self.db.attr_id_b = attr_id_b

        if attr_id_a is not None and attr_id_b is not None:
            self.load()

class ResourceAttr(IfaceBase):
    """
        A 'resource' can be either a node, link or network.
        A resource attribute is a instance of an attribute for
        a given resource.
    """
    def __init__(self, attr = None, resource_attr_id = None):
        IfaceBase.__init__(self, attr, self.__class__.__name__)

        self.db.resource_attr_id = resource_attr_id

        if resource_attr_id is not None:
            self.load()

    def get_resource(self):
        ref_key_map = {
            'NODE'     : Node,
            'LINK'     : Link,
            'NETWORK'  : Network,
            'PROJECT'  : Project,
            'GROUP'    : ResourceGroup,
        }

        if ref_key_map.get(self.db.ref_key) is None:
            raise HydraError("%s can not have attributes!"%(self.db.ref_key))

        obj = ref_key_map[self.db.ref_key]()
        obj.db.__setattr__(obj.db.pk_attrs[0], self.db.ref_id)
        obj.load()
        return obj

    def get_data(self):
        """
            Get all the resource scenario objects associated with this
            resource attribute.
        """
        return self.resourcescenarios

    def get_constraint_items(self):
        sql = """
            select
                item_id
            from
                tConstraintItem
            where
                resource_attr_id = %s
        """ % (self.db.resource_attr_id)

        rs = execute(sql)
        constraintitems = []
        for r in rs:
            item_i = ConstraintItem(item_id = r.item_id)
            item_i.load()
            constraintitems.append(item_i)

        return constraintitems

    def get_resource_scenarios(self):
        sql = """
            select
                scenario_id
            from
                tResourceScenario
            where
                resource_attr_id = %s
        """ % (self.db.resource_attr_id)

        rs = execute(sql)
        resourcescenarios = []
        for r in rs:
            resource_scenario = ResourceScenario(scenario_id=r.scenario_id, resource_attr_id = self.db.resource_attr_id)
            resource_scenario.load()
            resourcescenarios.append(resource_scenario)

        return resourcescenarios

    def get_attr(self):
        return Attr(attr_id=self.db.attr_id)

    def delete(self, purge_data=False):
        #If there are any constraints associated with this resource attribute, it cannot be deleted
        if len(self.constraintitems) > 0:
            constraints = [ci.db.constraint_id for ci in self.constraintitems]
            raise HydraError("Resource attribute cannot be deleted. "
                             "It is referened by constraints: %s "\
                             %constraints)

        for resource_scenario in self.resourcescenarios:
            #We can only purge data if there are no other resource
            #attributes associated with this data.
            if purge_data == True:
                d = resource_scenario.dataset
                d.load()
                #If there is only 1 resource attribute for this
                #piece of data, it's OK to remove it.
                if len(d.resourcescenarios) == 1:
                    #Delete the data entry first
                    resource_scenario.dataset.delete_val()
                    #then delete the scenario data
                    d.delete()
            #delete the reference to the resource attribute
            resource_scenario.delete()
        #delete the resource attribute
        super(ResourceAttr, self).delete()


class TypeAttr(IfaceBase):
    """
        A resource type item is a link between a resource type
        and attributes.
    """
    def __init__(self, templatetype=None, attr_id = None, type_id = None):
        IfaceBase.__init__(self, templatetype, self.__class__.__name__)

        self.db.attr_id = attr_id
        self.db.type_id = type_id

        if attr_id is not None and type_id is not None:
            self.load()

class TemplateType(IfaceBase):
    """
        A resource type is a grouping of attributes which define
        a resource. For example, a "reservoir" type may have "volume",
        "discharge" and "daily throughput".
    """
    def __init__(self, template=None, type_id = None):
        IfaceBase.__init__(self, template, self.__class__.__name__)

        self.db.type_id = type_id
        self.typeattrs = []

        if type_id is not None:
            self.load()

    def add_typeattr(self, attr_id):
        """
            Add a resource type item to a resource type.
        """
        tattr_i = TypeAttr(templatetype=self)
        tattr_i.db.attr_id = attr_id
        tattr_i.db.type_id = self.db.type_id

        #If the item already exists, there's no need to add it again.
        if tattr_i.load() == False:
            tattr_i.save()
            self.typeattrs.append(tattr_i)

        return tattr_i

    def remove_typeattr(self, attr_id):
        """
            remove a type attribute from a template type.
        """
        #Only remove the item if it is there.
        for tattr_i in self.typeattrs:
            if attr_id == tattr_i.db.attr_id:
                self.typeattrs.remove(tattr_i)
                tattr_i.save()

        return tattr_i

    def delete(self):
        for type_attr in self.typeattrs:
            type_attr.delete()

        super(TemplateType, self).delete()

class Template(IfaceBase):
    """
        A template is a set of types, usually categorised
        by the plugin which they were defined for.
    """
    def __init__(self, template_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.template_id = template_id
        if template_id is not None:
            self.load()


    def add_templatetype(self, name):
        type_i = TemplateType(template=self)
        type_i.db.template_id = self.db.template_id
        type_i.db.type_name = name
        type_i.save()

        self.templatetypes.append(type_i)

        return type_i

    def add_type(self, name):#
        return self.add_templatetype(name)

    def delete(self):
        for templatetype in self.templatetypes:
            templatetype.delete()

        super(Template, self).delete()

    def get_as_dict(self, **kwargs):
        obj_dict = super(Template, self).get_as_dict(**kwargs)
        return obj_dict

class ResourceType(IfaceBase):
    """
        Records whether a node, link or network has been
        created based on a particulare type.
    """
    def __init__(self, ref_key=None, ref_id=None, type_id=None):
        IfaceBase.__init__(self, None, self.__class__.__name__)
        self.db.ref_key = ref_key
        self.db.ref_id = ref_id
        self.db.type_id = type_id

        if None not in (ref_key, ref_id, type_id):
            self.load()

    def get_type(self):
        """Return the corresponding TemplateType object.
        """
        return TemplateType(type_id=self.db.type_id)

class ResourceScenario(IfaceBase):
    """
        A resource scenario is what links the actual piece of data
        with a resource -- the data per resource will change per scenario.
    """
    def __init__(self, scenario = None, scenario_id = None, resource_attr_id=None):
        IfaceBase.__init__(self, scenario, self.__class__.__name__)

        self.db.scenario_id = scenario_id
        self.db.resource_attr_id = resource_attr_id
        self.resourceattr = None
        self.dataset = None

        if scenario_id is not None and resource_attr_id is not None:
            self.load()

    def load(self):
        """
            Override the base load function to also load sibling
            objects -- resource attribute and scenario data.
        """
        super(ResourceScenario, self).load()
        self.get_resource_attr()
        self.get_dataset()

    def get_resource_attr(self):
        ra = ResourceAttr(resource_attr_id = self.db.resource_attr_id)
        self.resourceattr = ra
        return ra

    def get_attr_id(self):
        if self.resourceattr is not None:
            return self.resourceattr.db.attr_id

        sql = """
            select
                attr_id
            from
                tResourceAttr
            where
                resource_attr_id=%s
        """%self.db.resource_attr_id

        rs = execute(sql)
        if len(rs) != 1:
            raise HydraError("Error retrieving attr_id for resource attr %s"
                             %(self.db.resource_attr_id))
        return rs[0].attr_id

    def get_dataset(self):
        ds = None
        if self.db.dataset_id is not None:
            ds = Dataset(dataset_id = self.db.dataset_id)
            self.dataset = ds
        return ds

    def get_as_dict(self, **kwargs):
        """
            This method overrides the base method as it hides
            some of the DB complexities
        """

        if kwargs.get('include_data', 'Y') == 'N':
            return None

        #first create the appropriate soap complex model
        obj_dict = dict(
            object_type      = self.name,
            resource_attr_id = self.db.resource_attr_id,
            attr_id          = self.get_attr_id(),
            value            = None,
        )

        if self.dataset is None:
            self.get_dataset()

        obj_dict['value'] = self.dataset.get_as_dict(**kwargs)

        return obj_dict

class Dataset(IfaceBase):
    """
        A table recording all pieces of data, including the
        type, units, name and dimension. The actual data value is stored
        in another table, which is identified based on the type.
    """
    def __init__(self, dataset_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.dataset_id = dataset_id
        self.datum = None

        if dataset_id is not None:
            self.load()

    def set_ownership(self, user_id, read='Y', write='Y', share='Y'):
        """
            Add 'exceptions' to a locked dataset.
            If a dataset is locked nobody except the owner can get it
            from the server. Exceptions to this rule are added by 'ownership.'
        """

        if self.db.locked == 'N':
            raise HydraError("Cannot set ownership. Dataset not locked")

        owner = Owner()
        owner.db.ref_key = 'DATASET'

        owner.db.ref_id  = self.db.dataset_id
        owner.db.user_id = int(user_id)
        owner.load()
        owner.db.view    = read
        owner.db.edit    = write
        owner.db.share   = share

        owner.save()

        return owner

    def check_read_permission(self, user_id):
        """
            Check whether this user can read this network
        """

        sql = """
            select
                user_id
            from
                tOwner
            where
                ref_key = 'DATASET'
            and ref_id  = %s
            and view    = 'Y'
            and user_id = %s
        """%(self.db.dataset_id, user_id)

        rs = execute(sql)

        if len(rs) == 0:
            raise HydraError("Permission denied. User %s does not have read"
                             " access on dataset %s" %
                             (user_id, self.db.dataset_id))

    def get_val(self, timestamp=None):
        datum = self.get_datum()
        if self.db.data_type in ('timeseries', 'eqtimeseries'):
            return datum.get_val(timestamp=timestamp)
        else:
            return datum.get_val()

    def get_datum(self):
        if self.datum is not None:
            return self.datum
        if self.db.data_type == 'descriptor':
            datum = Descriptor(data_id = self.db.data_id)
        elif self.db.data_type == 'timeseries':
            datum = TimeSeries(data_id=self.db.data_id)
            datum.load_all()
        elif self.db.data_type == 'eqtimeseries':
            datum = EqTimeSeries(data_id = self.db.data_id)
        elif self.db.data_type == 'scalar':
            datum = Scalar(data_id = self.db.data_id)
        elif self.db.data_type == 'array':
            datum = Array(data_id = self.db.data_id)
        else:
            raise HydraError("Unrecognised data type: %s"%self.db.data_type)

        self.datum = datum
        return datum

    def set_val(self, data_type, val):
        data = None
        if data_type == 'descriptor':
            data = Descriptor()
            data.db.desc_val = val
        elif data_type == 'timeseries':
            data = TimeSeries()
            data.set_ts_values(val)
        elif data_type == 'eqtimeseries':
            data = EqTimeSeries()
            data.db.start_time = val[0]
            data.db.frequency  = val[1]
            data.db.arr_data = val[2]
        elif data_type == 'scalar':
            data = Scalar()
            data.db.param_value = val
        elif data_type == 'array':
            data = Array()
            data.db.arr_data = val
        data.save()
        data.commit()
        data.load()
        self.db.data_id = data.db.data_id
        return data

    def delete_val(self):
        if self.db.data_type == 'descriptor':
            d = Descriptor(data_id = self.db.data_id)
        elif self.db.data_type == 'timeseries':
            d = TimeSeries(data_id=self.db.data_id)
        elif self.db.data_type == 'eqtimeseries':
            d = EqTimeSeries(data_id = self.db.data_id)
        elif self.db.data_type == 'scalar':
            d = Scalar(data_id = self.db.data_id)
        elif self.db.data_type == 'array':
            d = Array(data_id = self.db.data_id)

        logging.info("Deleting %s with data id %s", self.db.data_type, self.db.data_id)
        d.delete()

    def get_as_dict(self, **kwargs):
        """
            This method overrides the base method as it hides
            some of the DB complexities.

            If the user_id parameter is None, no value is returned
            If the user_id is not the creator of the dataset and
            does not have read permission in tOwner, no value is returned.
        """

        user_id = kwargs.get('user_id')
        #Create the dict but with no 'value' attribute.
        obj_dict = super(Dataset, self).get_as_dict(**kwargs)
        obj_dict['value'] = None
        if user_id is None:
            return obj_dict

        if self.db.locked == 'Y':
            try:
                self.check_read_permission(user_id)
            except HydraError:
                return obj_dict

        datum = self.get_datum()
        obj_dict['value'] = datum.get_as_dict(**kwargs)

        return obj_dict

    def get_groups(self):
        """
            Get the dataset groups that this dataset is in
        """

        sql = """
            select
                group_id
            from
                tDatasetGroupItem
            where
                dataset_id = %s
        """ % self.db.dataset_id

        rs = execute(sql)

        groups = []
        for r in rs:
            g = DatasetGroup(group_id=r.group_id)
            groups.append(g)

        return groups

    def set_hash(self, val):
        hash_string = "%s %s %s %s %s"%(self.db.data_name,
                                       self.db.data_units,
                                       self.db.data_dimen,
                                       self.db.data_type,
                                       str(val))

        data_hash  = hash(hash_string)

        self.db.data_hash = data_hash
        return data_hash


class DatasetGroup(IfaceBase):
    """
        Groups data together to make it easier to find
    """
    def __init__(self, group_id=None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.group_id = group_id

        if group_id is not None:
            self.load()

class DatasetGroupItem(IfaceBase):
    """
        Each data item in a dataset group
    """
    def __init__(self, datasetgroup=None, group_id=None, dataset_id=None):
        IfaceBase.__init__(self, datasetgroup, self.__class__.__name__)

        self.db.group_id = group_id
        self.db.dataset_id = dataset_id

        if None not in (group_id, dataset_id):
            self.load()

class DataAttr(IfaceBase):
    """
        Holds additional information on data.
    """
    def __init__(self, d_attr_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.d_attr_id = d_attr_id

        if d_attr_id is not None:
            self.load()

class Descriptor(IfaceBase):
    """
        A non numeric data value
    """
    def __init__(self, data_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.data_id = data_id
        if data_id is not None:
            self.load()

    def get_val(self):
        """
            Get the value of a descriptor. A string value.
        """
        return self.db.desc_val

class TimeSeries(IfaceBase):
    """
        Non-equally spaced time series data
        Links to multiple entries in time series data, which
        actually stores the info.
    """
    def __init__(self, data_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.data_id = data_id
        if data_id is not None:
            self.load()

    def set_ts_value(self, time, value):
        """
            Adds a single timeseries value to the timeseries.
            This consists of a timestamp and a value
        """

        for ts in self.timeseriesdatas:
            if ts.db.data_id == self.db.data_id and ts.db.ts_time == time:
                ts.db.ts_value = value
                return
        #else:
        ts_val = TimeSeriesData()
        ts_val.db.data_id = self.db.data_id
        ts_val.db.ts_time = time
        ts_val.db.ts_value = value

        self.timeseriesdatas.append(ts_val)

    def set_ts_values(self, values):
        """
            Adds multiple timeseries values to a timeseries.
            This is takes a list of tuples as an argument, as follows:
            [(time_1, value_1), (time_2, value_2), ...(time_n, value_n)]
        """

        for time, value in values:
            self.set_ts_value(time, value)

    def get_val(self, timestamp=None):
        """
            Given a timestamp (or list of timestamps) and some timeseries data,
            return the values appropriate to the requested times.

            If the timestamp is *before* the start of the timeseries data, return None
            If the timestamp is *after* the end of the timeseries data, return the last
            value.
        """
        self.load_all()
        ts_datas = self.timeseriesdatas
        #Set the return value to be a list of tuples -- the default
        #return value.
        val = []
        for ts in ts_datas:
            val.append((ts.db.ts_time,ts.db.ts_value))

        if timestamp is not None:
            #get the ts_value most appropriate for the given timestamp
            ts_val_dict = dict(val)
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

    def get_ts_value(self, time):
        """
            returns the value at a given time for a timeseries
        """
        for ts_data in self.timeseriesdatas:
            logging.debug("%s vs %s", ts_data.db.ts_time, time)
            if ts_data.db.ts_time == time:
                return ts_data.db.ts_value
        logging.info("No value found at %s for data_id %s", time, self.db.data_id)
        return None

    def delete(self):
        for ts_data in self.timeseriesdatas:
            ts_data.delete()
        super(TimeSeries, self).delete()

    def save(self):
        super(TimeSeries, self).save()

        for ts_data in self.timeseriesdatas:
            ts_data.db.data_id = self.db.data_id
            ts_data.save()

    def commit(self):
        super(TimeSeries, self).commit()
        for ts_data in self.timeseriesdatas:
            ts_data.commit()

class TimeSeriesData(IfaceBase):
    """
        Non-equally spaced time series data
        In other words: a value and a timestamp.
    """
    def __init__(self, timeseries=None, data_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.data_id = data_id
        if data_id is not None:
            self.load()

    def get_as_dict(self, **kwargs):
        obj_dict = dict(
            data_id = self.db.data_id,
            ts_time = self.get_timestamp(),
            ts_value = self.db.ts_value,
        )
        return obj_dict

    def get_timestamp(self):
        timestamp = convert_ordinal_to_datetime(self.db.ts_time)
        timestamp = self.time_format.format(timestamp.year,
                                            timestamp.month,
                                            timestamp.day,
                                            timestamp.hour,
                                            timestamp.minute,
                                            timestamp.second,
                                            timestamp.microsecond)
        return timestamp

class EqTimeSeries(IfaceBase):
    """
        Equally spaced time series data
        -- a start time, frequency and an associated array.
    """
    def __init__(self, data_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.data_id = data_id

        if data_id is not None:
            self.load()

    def get_val(self, timestamp=None):
        val  = self.db.arr_data

        if timestamp is not None:
            #easiest thing to do is build up a dictionary of timestamp / values
            val = eval(val) #this should be a multi-dimensional list
            start_date = self.db.start_time
            val_dict = dict()
            time_interval = start_date
            time_delta    = datetime.timedelta(seconds=self.db.frequency)
            for v in val:
                val_dict[time_interval] = v
                time_interval = time_interval + time_delta

            for time in val_dict.keys().sort().reverse():
                if timestamp >= time:
                    val = val_dict[time]
                    break
            else:
                val = None
        else:
            """
                Get the value of an equally spaced TimeSeries
                returns a dictionary containing start time, frequency and value.
            """
            starttime = convert_ordinal_to_datetime(self.db.start_time)
            starttime = self.time_format.format(starttime.year,
                                            starttime.month,
                                            starttime.day,
                                            starttime.hour,
                                            starttime.minute,
                                            starttime.second,
                                            starttime.microsecond)
            val = {
                'start_time' : starttime,
                'frequency'  : self.db.frequency,
                'arr_data'   : self.db.arr_data,
            }
        return val

class Scalar(IfaceBase):
    """
        The values contained in an equally spaced time series.
    """
    def __init__(self, data_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.data_id = data_id
        if data_id is not None:
            self.load()

    def get_val(self):
        """
            Get the value of this scalar. A number basically.
        """
        return self.db.param_value

class Array(IfaceBase):
    """
        List of values, stored as a BLOB.
    """
    def __init__(self, data_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.data_id = data_id
        if data_id is not None:
            self.load()

    def get_val(self):
        """
            Get the value of this array.
        """
        return self.db.arr_data

class Constraint(IfaceBase):
    """
        A constraint or rule placed on a network, perhaps
        to ensure mutual exclusion of certain resources..
    """
    def __init__(self, scenario=None, constraint_id = None):
        IfaceBase.__init__(self, scenario, self.__class__.__name__)

        self.scenario=scenario
        self.db.constraint_id = constraint_id

        if constraint_id is not None:
            self.load()

    def eval_condition(self):
        grp_1 = ConstraintGroup(constraint=self, group_id = self.db.group_id)

        condition_string = "%s %s %s"%(grp_1.eval_group(), self.db.op, self.db.constant)

        return condition_string

    def get_as_dict(self, **kwargs):
        self.load_all()
        obj_dict = super(Constraint, self).get_as_dict(**kwargs)

        return obj_dict

class ConstraintGroup(IfaceBase):
    """
        a connector class for constraints. Used for grouping constraints
        into logical sections, not unlike parentheses in a mathematical equation.
    """
    def __init__(self, constraint=None, group_id = None):
        IfaceBase.__init__(self, constraint, self.__class__.__name__)

        self.constraint = constraint
        self.db.group_id = group_id

        self.groups = []
        self.items  = []

        if group_id is not None:
            self.load()

    def load(self):
        super(ConstraintGroup, self).load()
        self.get_groups()
        self.get_items()

    def save(self):

        for group in self.get_groups():
            group.save()

        for item in self.get_items():
            item.save()

        super(ConstraintGroup, self).save()

    def get_groups(self):
        if len(self.groups) > 0:
            return self.groups

        if self.db.ref_key_1 == 'GRP':
            group = ConstraintGroup(self.constraint, group_id=self.db.ref_id_1)
            self.groups.append(group)

        if self.db.ref_key_2 == 'GRP':
            group = ConstraintGroup(self.constraint, group_id=self.db.ref_id_2)
            self.groups.append(group)

        return self.groups

    def get_items(self):

        if len(self.items) > 0:
            return self.items

        if self.db.ref_key_1 == 'ITEM':
            item = ConstraintItem(constraint=self.constraint, item_id=self.db.ref_id_1)
            self.items.append(item)

        if self.db.ref_key_2 == 'ITEM':
            item = ConstraintItem(constraint=self.constraint, item_id=self.db.ref_id_2)
            self.items.append(item)

        return self.items

    def eval_group(self):

        str_1 = None
        str_2 = None

        if self.db.ref_key_1 == 'GRP':
            group = ConstraintGroup(self.constraint, group_id=self.db.ref_id_1)
            str_1 = group.eval_group()
        elif self.db.ref_key_1 == 'ITEM':
            item = ConstraintItem(self.constraint, item_id=self.db.ref_id_1)

            if item.db.constant is None:

                r = ResourceScenario(
                        scenario_id      = self.constraint.db.scenario_id,
                        resource_attr_id = item.db.resource_attr_id
                )

                d = Dataset(dataset_id=r.db.dataset_id)
                str_1 = d.get_val()
            else:
                str_1 = item.db.constant

        if self.db.ref_key_2 == 'GRP':
            group = ConstraintGroup(self.constraint, group_id=self.db.ref_id_2)
            str_2 = group.eval_group()
        elif self.db.ref_key_2 == 'ITEM':
            item = ConstraintItem(item_id=self.db.ref_id_2)

            if item.db.constant is None:
                r = ResourceScenario(
                        scenario_id      = self.constraint.db.scenario_id,
                        resource_attr_id = item.db.resource_attr_id
                )

                d = Dataset(dataset_id=r.db.dataset_id)
                str_2 = d.get_val()
            else:
                str_2 = item.db.constant

        return "(%s %s %s)"%(str_1, self.db.op, str_2)

    def get_as_dict(self, **kwargs):

        obj_dict = super(ConstraintGroup, self).get_as_dict(**kwargs)

        obj_dict.update(dict(
            groups     = [],
            items      = [],
        ))
        if self.db.ref_key_1 == 'GRP':
            group = ConstraintGroup(self.constraint, group_id=self.db.ref_id_1)
            obj_dict['groups'].append(group.get_as_dict(**kwargs))
        elif self.db.ref_key_1 == 'ITEM':
            item = ConstraintItem(item_id=self.db.ref_id_1)
            obj_dict['items'].append(item.get_as_dict(**kwargs))

        if self.db.ref_key_2 == 'GRP':
            group = ConstraintGroup(self.constraint, group_id=self.db.ref_id_2)
            obj_dict['groups'].append(group.get_as_dict(**kwargs))
        elif self.db.ref_key_2 == 'ITEM':
            item = ConstraintItem(item_id=self.db.ref_id_2)
            obj_dict['items'].append(item.get_as_dict(**kwargs))

        return obj_dict

class ConstraintItem(IfaceBase):
    """
        The link to the resource, upon which the constraint is being applied.
    """
    def __init__(self, constraint=None, item_id = None):
        IfaceBase.__init__(self, constraint, self.__class__.__name__)

        self.constraint = constraint
        self.db.item_id = item_id
        if item_id is not None:
            self.load()

    def get_item_details(self):
        """
            Get the resource name, id and attribute to which
            this resource attribute belongs.
        """

        if self.db.constant is not None:
            return self.db.constant

        sql = """
            select
                attr.attr_name,
                ra.ref_key,
                ra.ref_id,
                case when node.node_name is not null then node.node_name
                    when link.link_name is not null then link.link_name
                    when network.network_name is not null then network.network_name
                    when project.project_name is not null then project.project_name
                    when grp.group_name is not null then grp.group_name
                    else null
                end as resource_name
                from
                    tResourceAttr ra
                    left join tNode node on (
                        ra.ref_key = 'NODE'
                        and ra.ref_id = node.node_id
                    )
                    left join tLink link on (
                        ra.ref_key = 'LINK'
                        and ra.ref_id = link.link_id
                    )
                    left join tNetwork network on (
                        ra.ref_key = 'NETWORK'
                        and ra.ref_id = network.network_id
                    )
                    left join tProject project on (
                        ra.ref_key = 'PROJECT'
                        and ra.ref_id = project.project_id
                    )
                    left join tResourceGroup grp on (
                        ra.ref_key = 'GROUP'
                        and ra.ref_id = grp.group_id
                    ),
                    tAttr attr
                where
                    ra.resource_attr_id = %s
                    and attr.attr_id = ra.attr_id
        """ % self.db.resource_attr_id

        rs = execute(sql)

        if len(rs) == 0:
            raise HydraError("Could not find resource for"
                " resource attribute(%s) in the contraint item!",
                self.db.resource_attr_id)

        return (rs[0].attr_name, rs[0].ref_key, rs[0].ref_id, rs[0].resource_name)

    def get_as_dict(self, **kwargs):
        attr_name     = None
        ref_key       = None
        ref_id        = None
        resource_name = None
        if self.db.constant is None:
            details       = self.get_item_details()
            attr_name, ref_key, ref_id, resource_name = details

        obj_dict = dict(
            attr_name        = attr_name,
            ref_key          = ref_key,
            ref_id           = ref_id,
            resource_name    = resource_name,
            constant         = self.db.constant,
            resource_attr_id = self.db.resource_attr_id,
            constraint_id    = self.db.constraint_id,
            item_id          = self.db.item_id,
        )

        return obj_dict

class User(IfaceBase):
    """
        Users of the hydra
    """
    def __init__(self, user_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.user_id = user_id

        if user_id is not None:
            self.load()

    def get_user_id(self):
        """
            Returns the user_id for a given username. NB: The username must
            be set by this point!
            If the user exists, the password and user id will be updated and
            the user id returnd.
            If not, None is returned.
        """

        if self.db.user_id is not None:
            return self.db.user_id

        if self.db.username is None:
            raise HydraError("Cannot find a user's id without a username.")

        sql = """
            select
                user_id,
                password
            from
                tUser
            where
                username = '%s'
        """ % self.db.username

        user_rs = execute(sql)

        if len(user_rs) > 0:
            self.db.user_id = user_rs[0].user_id
            self.db.password = user_rs[0].password
            return self.db.user_id
        else:
            logging.info("User %s does not exist."%self.db.username)
            return None

    def update_alter_time(self):
        self.db.last_edit = datetime.datetime.now()
        self.db.save()

class Role(IfaceBase):
    """
        Roles for hydra users
    """
    def __init__(self, role_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.role_id = role_id
        if role_id is not None:
            self.load()


class Perm(IfaceBase):
    """
        Hydra Permissions
    """
    def __init__(self, perm_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.perm_id = perm_id
        if perm_id is not None:
            self.load()

class RoleUser(IfaceBase):
    """
        Roles for hydra users
    """
    def __init__(self, role=None, user_id = None, role_id = None):
        IfaceBase.__init__(self, role, self.__class__.__name__)

        self.db.user_id = user_id
        self.db.role_id = role_id
        if user_id is not None and role_id is not None:
            self.load()

class RolePerm(IfaceBase):
    """
        Permissions for hydra Roles
    """
    def __init__(self, role=None, perm_id = None, role_id = None):
        IfaceBase.__init__(self, role, self.__class__.__name__)

        self.db.perm_id = perm_id
        self.db.role_id = role_id
        if perm_id is not None and role_id is not None:
            self.load()

class Owner(IfaceBase):
    """
       Ownership for a project.
    """
    def __init__(self, user_id = None, ref_key = None, ref_id = None):
        IfaceBase.__init__(self, None, self.__class__.__name__)

        self.db.user_id = user_id
        self.db.ref_key = ref_key
        self.db.ref_id  = ref_id
        if user_id is not None and ref_key is not None and ref_id is not None:
            self.load()

db_hierarchy = dict(
    project  = dict(
        obj   = Project,
        parent = None,
        table_name = 'tProject',
        pk     = ['project_id']
    ),
    network  = dict(
        obj   = Network,
        parent = 'project',
        table_name = 'tNetwork',
        pk     = ['network_id']
    ),
    node  = dict(
        obj   = Node,
        parent = 'network',
        table_name = 'tNode',
        pk     = ['node_id']
   ),
    link  = dict(
        obj   = Link,
        parent = 'network',
        table_name = 'tLink',
        pk     = ['link_id']
    ),
    resourcegroup  = dict(
        obj        = ResourceGroup,
        parent     = 'network',
        table_name = 'tResourceGroup',
        pk         = ['group_id'],
    ),
    scenario  = dict(
        obj    = Scenario,
        parent = 'network',
        table_name = 'tScenario',
        pk     = ['scenario_id'],
        attr_funcs = {
            'resourcegroupitems' : Scenario.get_resource_items
        }
    ),
    attr  = dict(
        obj   = Attr,
        parent = None,
        table_name = 'tAttr',
        pk     = ['attr_id']
    ),
    attrmap  = dict(
        obj   = AttrMap,
        parent = None,
        table_name = 'tAttrMap',
        pk     = ['attr_id_a', 'attr_id_b']
    ),
    resourceattr  = dict(
        obj   = ResourceAttr,
        parent = 'attr',
        table_name = 'tResourceAttr',
        pk     = ['resource_attr_id'],
        attr_funcs = {
            'constraintitems'   : ResourceAttr.get_constraint_items,
            'resourcescenarios' : ResourceAttr.get_resource_scenarios
        }
    ),
    templatetype  = dict(
        obj   = TemplateType,
        parent = 'template',
        table_name = 'tTemplateType',
        pk     = ['type_id']
    ),
    typeattr  = dict(
        obj   = TypeAttr,
        parent = 'templatetype',
        table_name = 'tTypeAttr',
        pk     = ['attr_id', 'type_id'],
    ),
    template  = dict(
        obj   = Template,
        parent = None,
        table_name = 'tTemplate',
        pk     = ['template_id']
    ),
    resourcetype = dict(
        obj   = ResourceType,
        parent = None,
        table_name = 'tResourceType',
        pk  = ['ref_key', 'ref_id', 'type_id'],
    ),
    resourcescenario  = dict(
        obj   = ResourceScenario,
        parent = 'scenario',
        table_name = 'tResourceScenario',
        pk     = ['resource_attr_id', 'scenario_id']
    ),
    resourcegroupitem = dict(
        obj  = ResourceGroupItem,
        parent = 'resourcegroup',
        table_name = 'tResourceGroupItem',
        pk         = ['item_id']
    ),
    dataset  = dict(
        obj   = Dataset,
        parent = None,
        table_name = 'tDataset',
        pk     = ['dataset_id']
    ),
    datasetgroup = dict(
        obj        = DatasetGroup,
        parent     = None,
        table_name = 'tDatasetGroup',
        pk         = ['group_id']
    ),
    datasetgroupitem = dict(
        obj        = DatasetGroupItem,
        parent     = 'datasetgroup',
        table_name = 'tDatasetGroupItem',
        pk         = ['group_id', 'dataset_id'],
    ),
    dataattr  = dict(
        obj   = DataAttr,
        parent = None,
        table_name = 'tDataAttr',
        pk     = ['d_attr_id'],
    ),
    descriptor  = dict(
        obj   = Descriptor,
        parent = None,
        table_name = 'tDescriptor',
        pk     = ['data_id']
    ),
    timeseries  = dict(
        obj   = TimeSeries,
        parent = None,
        table_name = 'tTimeSeries',
        pk     = ['data_id']
    ),
    timeseriesdata  = dict(
        obj   = TimeSeriesData,
        parent = 'timeseries',
        table_name = 'tTimeSeriesData',
        pk     = ['data_id', 'ts_time']
    ),
    eqtimeseries  = dict(
        obj   = EqTimeSeries,
        parent = None,
        table_name = 'tEqTimeSeries',
        pk     = ['data_id']
    ),
    scalar  = dict(
        obj   = Scalar,
        parent = None,
        table_name = 'tScalar',
        pk     = ['data_id']
    ),
    array  = dict(
        obj   = Array,
        parent = None,
        table_name = 'tArray',
        pk     = ['data_id']
    ),
    constraint  = dict(
        obj   = Constraint,
        parent = 'scenario',
        table_name = 'tConstraint',
        pk     = ['constraint_id']
    ),
    constraintgroup  = dict(
        obj   = ConstraintGroup,
        parent = 'constraint',
        table_name = 'tConstraintGroup',
        pk     = ['group_id']
    ),
    constraintitem  = dict(
        obj   = ConstraintItem,
        parent = 'constraint',
        table_name = 'tConstraintItem',
        pk     = ['item_id']
    ),
    user  = dict(
        obj   = User,
        parent = None,
        table_name = 'tUser',
        pk     = ['user_id']
    ),
    role  = dict(
        obj   = Role,
        parent = None,
        table_name = 'tRole',
        pk     = ['role_id']
    ),
    perm  = dict(
        obj   = Perm,
        parent = None,
        table_name = 'tPerm',
        pk     = ['perm_id']
    ),
    roleuser  = dict(
        obj   = RoleUser,
        parent = 'role',
        table_name = 'tRoleUser',
        pk     = ['user_id', 'role_id']
    ),
    roleperm  = dict(
        obj   = RolePerm,
        parent = 'role',
        table_name = 'tRolePerm',
        pk     = ['perm_id', 'role_id']
    ),
    owner  = dict(
        obj   = Owner,
        parent = None,
        table_name = 'tOwner',
        pk     = ['user_id', 'project_id']
    ),
)
