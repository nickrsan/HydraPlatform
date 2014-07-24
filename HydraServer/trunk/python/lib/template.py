#d (c) Copyright 2013, 2014, University of Manchester
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
from db import DBSession
from db.model import Template, TemplateType, TypeAttr, Attr, Network, Node, Link, ResourceGroup, ResourceType, ResourceAttr, ResourceScenario
from data import add_dataset

from HydraLib.HydraException import HydraError, ResourceNotFoundError
from HydraLib import config, util
from lxml import etree
from decimal import Decimal
import logging
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import joinedload_all
import re
log = logging.getLogger(__name__)

def get_types_by_attr(resource, template_id=None):
    """
        Using the attributes of the resource, get all the
        types that this resource matches.
        @returns a dictionary, keyed on the template name, with the
        value being the list of type names which match the resources
        attributes.
    """

    resource_type_templates = []

    #Create a list of all of this resources attributes.
    attr_ids = []
    for attr in resource.attributes:
        attr_ids.append(attr.attr_id)
    all_resource_attr_ids = set(attr_ids)

    all_types = DBSession.query(TemplateType).options(joinedload_all('typeattrs')).filter(TemplateType.resource_type==resource.ref_key)
    if template_id is not None:
        all_types = all_types.filter(TemplateType.template_id==template_id)

    all_types = all_types.all()

    #tmpl type attrs must be a subset of the resource's attrs
    for ttype in all_types:
        type_attr_ids = []
        for typeattr in ttype.typeattrs:
            type_attr_ids.append(typeattr.attr_id)
        if set(type_attr_ids).issubset(all_resource_attr_ids):
            resource_type_templates.append(ttype)

    return resource_type_templates


def parse_attribute(attribute):

    dimension = attribute.find('dimension').text
    name      = attribute.find('name').text

    try:
        attr = DBSession.query(Attr).filter(Attr.attr_name==name, Attr.attr_dimen==dimension).one()
    except NoResultFound: 
        attr         = Attr()
        attr.attr_dimen = attribute.find('dimension').text
        attr.attr_name  = attribute.find('name').text

        log.info("Attribute not found, creating new attribute: name:%s, dimen:%s",
                    attr.attr_name, attr.attr_dimen)

        DBSession.add(attr)
    if attr.attr_dimen != dimension:
        raise HydraError(
            "An attribute with name "
            "%s already exists but with a different"
            " dimension (%s). Please rename your attribute." %
            (name, attr.attr_dimen))
    DBSession.flush()
    return attr

def parse_typeattr(type_i, attribute):

    attr = parse_attribute(attribute)

    dimension = attribute.find('dimension').text

    try:
        typeattr_i = DBSession.query(TypeAttr).filter(TypeAttr.type_id==type_i.type_id,
                                                      TypeAttr.attr_id==attr.attr_id).one()
    except NoResultFound:
        typeattr_i = TypeAttr()
        log.info("Creating type attr: type_id=%s, attr_id=%s", type_i.type_id, attr.attr_id)
        typeattr_i.type_id=type_i.type_id
        typeattr_i.attr_id=attr.attr_id
        type_i.typeattrs.append(typeattr_i)
        DBSession.add(typeattr_i)
    
    typeattr_i.dimension=dimension

    if attribute.find('is_var') is not None:
        typeattr_i.attr_is_var = attribute.find('is_var').text

    if attribute.find('default') is not None:
        default = attribute.find('default')
        unit = default.find('unit').text
        val  = default.find('value').text
        try:
            Decimal(val)
            data_type = 'scalar'
        except:
            data_type = 'descriptor'

        dataset = add_dataset(data_type,
                               val,
                               unit,
                               dimension,
                               name="%s Default"%attr.attr_name,)
        typeattr_i.default_dataset_id = dataset.dataset_id
   
    if attribute.find('restrictions') is not None:
        typeattr_i.data_restriction = str(util.get_restriction_as_dict(attribute.find('restrictions')))
    else:
        typeattr_i.data_restriction = None

    return typeattr_i

def upload_template_xml(template_xml,**kwargs):
    """
        Add the template, type and typeattrs described
        in an XML file.

        Delete type, typeattr entries in the DB that are not in the XML file
        The assumption is that they have been deleted and are no longer required.
    """

    template_xsd_path = config.get('templates', 'template_xsd_path')
    xmlschema_doc = etree.parse(template_xsd_path)

    xmlschema = etree.XMLSchema(xmlschema_doc)

    xml_tree = etree.fromstring(template_xml)

    xmlschema.assertValid(xml_tree)

    template_name = xml_tree.find('template_name').text

    template_layout = None
    if xml_tree.find('layout') is not None and \
               xml_tree.find('layout').text is not None:
        layout = xml_tree.find('layout')
        layout_string = get_layout_as_dict(layout)
        template_layout = str(layout_string)

    try:

        tmpl_i = DBSession.query(Template).filter(Template.template_name==template_name).one()
        tmpl_i.layout = template_layout
        log.info("Existing template found. name=%s", template_name)
    except NoResultFound:
        log.info("Template not found. Creating new one. name=%s", template_name)
        tmpl_i = Template(template_name=template_name, layout=template_layout)
        DBSession.add(tmpl_i)

    types = xml_tree.find('resources')

    #Delete any types which are in the DB but no longer in the XML file
    type_name_map = {r.type_name:r.type_id for r in tmpl_i.templatetypes}
    attr_name_map = {}
    for type_i in tmpl_i.templatetypes:
        for attr in type_i.typeattrs:
            attr_name_map[attr.attr.attr_name] = (attr.attr_id, attr.type_id)

    existing_types = set([r.type_name for r in tmpl_i.templatetypes])

    new_types = set([r.find('name').text for r in types.findall('resource')])

    types_to_delete = existing_types - new_types

    for type_to_delete in types_to_delete:
        type_id = type_name_map[type_to_delete]
        try:
            type_i = DBSession.query(TemplateType).filter(TemplateType.type_id==type_id).one()
            log.info("Deleting type %s", type_i.type_name)
            DBSession.delete(type_i)
        except NoResultFound:
            pass

    #Add or update types.
    for resource in types.findall('resource'):
        type_name = resource.find('name').text
        #check if the type is already in the DB. If not, create a new one.
        type_is_new = False
        if type_name in existing_types:
            type_id = type_name_map[type_name]
            type_i = DBSession.query(TemplateType).filter(TemplateType.type_id==type_id).one()
            
        else:
            log.info("Type %s not found, creating new one.", type_name)
            type_i = TemplateType()
            type_i.type_name = type_name 
            tmpl_i.templatetypes.append(type_i)
            type_is_new = True

        if resource.find('alias') is not None:
            type_i.alias = resource.find('alias').text

        if resource.find('type') is not None:
            type_i.resource_type = resource.find('type').text

        if resource.find('layout') is not None and \
            resource.find('layout').text is not None:
            layout = resource.find('layout')
            layout_string = get_layout_as_dict(layout)
            type_i.layout = str(layout_string)
       
        if resource.find('type') is not None and \
           resource.find('type').text is not None:
            type_i.resource_type = resource.find('type').text
        #delete any TypeAttrs which are in the DB but not in the XML file
        existing_attrs = []
        if not type_is_new:
            for r in tmpl_i.templatetypes:
                if r.type_name == type_name:
                    for typeattr in r.typeattrs:
                        existing_attrs.append(typeattr.attr.attr_name)

        existing_attrs = set(existing_attrs)

        template_attrs = set([r.find('name').text for r in resource.findall('attribute')])

        attrs_to_delete = existing_attrs - template_attrs
        for attr_to_delete in attrs_to_delete:
            attr_id, type_id = attr_name_map[attr_to_delete]
            try:
                attr_i = DBSession.query(TypeAttr).filter(TypeAttr.attr_id==attr_id, TypeAttr.type_id==type_id).one()
                DBSession.delete(attr_i)
                log.info("Attr %s in type %s deleted",attr_i.attr.attr_name, attr_i.templatetype.type_name)
            except NoResultFound:
                log.debug("Attr %s not found in type %s"%(attr_id, type_id))
                continue

        #Add or update type typeattrs
        for attribute in resource.findall('attribute'):
            parse_typeattr(type_i, attribute)

    return tmpl_i

def apply_template_to_network(template_id, network_id, **kwargs):
    """
        For each node and link in a network, check whether it matches
        a type in a given template. If so, assign the type to the node / link.
    """

    net_i = DBSession.query(Network).filter(Network.network_id==network_id).one()
    #There should only ever be one matching type, but if there are more,
    #all we can do is pick the first one.
    try: 
        network_type_id = DBSession.query(TemplateType.type_id).filter(TemplateType.template_id==template_id,
                                                                       TemplateType.resource_type=='NETWORK').one()
        assign_type_to_resource(network_type_id.type_id, 'NETWORK', network_id,**kwargs)
    except NoResultFound:
        log.info("No network type to set.")
        pass

    for node_i in net_i.nodes:
        templates = get_types_by_attr(node_i, template_id)
        if len(templates) > 0:
            assign_type_to_resource(templates[0].type_id, 'NODE', node_i.node_id,**kwargs)
    for link_i in net_i.links:
        templates = get_types_by_attr(link_i, template_id)
        if len(templates) > 0:
            assign_type_to_resource(templates[0].type_id, 'LINK', link_i.link_id,**kwargs)

    for group_i in net_i.resourcegroups:
        templates = get_types_by_attr(group_i, template_id)
        if len(templates) > 0:
            assign_type_to_resource(templates[0].type_id, 'GROUP', group_i.group_id,**kwargs)

def get_matching_resource_types(resource_type, resource_id,**kwargs):
    """
        Get the possible types of a resource by checking its attributes
        against all available types.

        @returns A list of TypeSummary objects.
    """
    resource_i = None
    if resource_type == 'NETWORK':
        resource_i = DBSession.query(Network).filter(Network.network_id==resource_id).one()
    elif resource_type == 'NODE':
        resource_i = DBSession.query(Node).filter(Node.node_id==resource_id).one()
    elif resource_type == 'LINK':
        resource_i = DBSession.query(Link).filter(Link.link_id==resource_id).one()
    elif resource_type == 'GROUP':
        resource_i = DBSession.query(ResourceGroup).filter(ResourceGroup.resourcegroup_id==resource_id).one()

    matching_types = get_types_by_attr(resource_i)
    return matching_types

def assign_types_to_resources(resource_types,**kwargs):
    """
        Assign new types to list of resources.
        This function checks if the necessary
        attributes are present and adds them if needed. Non existing attributes
        are also added when the type is already assigned. This means that this
        function can also be used to update resources, when a resource type has
        changed.
    """
    types = {}

    for resource_type in resource_types:
        ref_id = resource_type.ref_id
        type_id = resource_type.type_id
        if types.get(resource_type.type_id) is None:
            t = DBSession.query(TemplateType).filter(TemplateType.type_id==type_id).one()
            types[resource_type.type_id] = t
    res_types = []
    res_attrs = []
    for resource_type in resource_types:
        ref_id  = resource_type.ref_id
        ref_key = resource_type.ref_key
        type_id = resource_type.type_id
        if resource_type.ref_key == 'NETWORK':
            resource = DBSession.query(Network).filter(Network.network_id==ref_id).one()
        elif resource_type.ref_key == 'NODE':
            resource = DBSession.query(Node).filter(Node.node_id==ref_id).one()
        elif resource_type.ref_key == 'LINK':
            resource = DBSession.query(Link).filter(Link.link_id==ref_id).one()
        elif resource_type.ref_key == 'GROUP':
            resource = DBSession.query(ResourceGroup).filter(ResourceGroup.group_id==ref_id).one()
        resource.ref_key = ref_key
        resource.ref_id  = ref_id
        

        ra, rt = set_resource_type(resource, type_id, types)
        res_types.append(rt)
        res_attrs.extend(ra)
    DBSession.execute(ResourceType.__table__.insert(), res_types)
    DBSession.execute(ResourceAttr.__table__.insert(), res_attrs)
    resource.attributes

    #Make DBsession 'dirty' to pick up the inserts by doing a fake delete. 
    DBSession.query(Attr).filter(Attr.attr_id==None).delete()

    ret_val = [t for t in types.values()]
    return ret_val


def assign_type_to_resource(type_id, resource_type, resource_id,**kwargs):
    """Assign new type to a resource. This function checks if the necessary
    attributes are present and adds them if needed. Non existing attributes
    are also added when the type is already assigned. This means that this
    function can also be used to update resources, when a resource type has
    changed.
    """
    if resource_type == 'NETWORK':
        resource = DBSession.query(Network).filter(Network.network_id==resource_id).one()
    elif resource_type == 'NODE':
        resource = DBSession.query(Node).filter(Node.node_id==resource_id).one()
    elif resource_type == 'LINK':
        resource = DBSession.query(Link).filter(Link.link_id==resource_id).one()
    elif resource_type == 'GROUP':
        resource = DBSession.query(ResourceGroup).filter(ResourceGroup.group_id==resource_id).one()
    res_attrs, res_type = set_resource_type(resource, type_id, **kwargs)

    type_i = DBSession.query(TemplateType).filter(TemplateType.type_id==type_id).one()
    if resource_type != type_i.resource_type:
        raise HydraError("Cannot assign a %s type to a %s"%
                         (type_i.resource_type,resource_type))

    DBSession.execute(ResourceType.__table__.insert(), [res_type])
    if len(res_attrs) > 0:
        DBSession.execute(ResourceAttr.__table__.insert(), res_attrs)

    #Make DBsession 'dirty' to pick up the inserts by doing a fake delete. 
    DBSession.query(Attr).filter(Attr.attr_id==None).delete()

    return DBSession.query(TemplateType).filter(TemplateType.type_id==type_id).one()

def set_resource_type(resource, type_id, types={}, **kwargs):
    """
        Set this resource to be a certain type.
        Type objects (a dictionary keyed on type_id) may be
        passed in to save on loading.
        This function does not call save. It must be done afterwards.
        New resource attributes are added to the resource if the template
        requires them. Resource attributes on the resource but not used by
        the template are not removed.
        @returns list of new resource attributes
        ,new resource type object
    """

    ref_key = resource.ref_key

    existing_attr_ids = []
    for attr in resource.attributes:
        existing_attr_ids.append(attr.attr_id)

    if type_id in types.keys():
        type_i = types[type_id]
    else:
        type_i = DBSession.query(TemplateType).filter(TemplateType.type_id==type_id).one()

    type_attrs = dict()
    for typeattr in type_i.typeattrs:
        type_attrs.update({typeattr.attr_id:
                           typeattr.attr_is_var})

    # check if attributes exist
    missing_attr_ids = set(type_attrs.keys()) - set(existing_attr_ids)

    # add attributes if necessary
    new_res_attrs = []
    for attr_id in missing_attr_ids:
        ra_dict = dict(
            ref_key = ref_key,
            attr_id = attr_id,
            attr_is_var = type_attrs[attr_id],
            node_id    = resource.node_id    if ref_key == 'NODE' else None,
            link_id    = resource.link_id    if ref_key == 'LINK' else None,
            group_id   = resource.group_id   if ref_key == 'GROUP' else None,
            network_id = resource.network_id if ref_key == 'NETWORK' else None,

        )
        new_res_attrs.append(ra_dict)

    # add type to tResourceType if it doesn't exist already
    resource_type = dict(
        node_id    = resource.node_id    if ref_key == 'NODE' else None,
        link_id    = resource.link_id    if ref_key == 'LINK' else None,
        group_id   = resource.group_id   if ref_key == 'GROUP' else None,
        network_id = resource.network_id if ref_key == 'NETWORK' else None,
        ref_key    = ref_key,
        type_id    = type_id,
    )

    return new_res_attrs, resource_type

def remove_type_from_resource( type_id, resource_type, resource_id,**kwargs): 
    """ 
        Remove a resource type trom a resource 
    """ 
    node_id = resource_id if resource_type == 'NODE' else None
    link_id = resource_id if resource_type == 'LINK' else None
    group_id = resource_id if resource_type == 'GROUP' else None

    resourcetype = DBSession.query(ResourceType).filter(
                                        ResourceType.type_id==type_id,
                                        ResourceType.ref_key==resource_type,
                                        ResourceType.node_id == node_id,
    ResourceType.link_id == link_id,
    ResourceType.group_id == group_id).one() 
    DBSession.delete(resourcetype) 

def _parse_data_restriction(restriction_dict):
#    {{soap_server.hydra_complexmodels}LESSTHAN}

    if restriction_dict is None or restriction_dict == '':
        return None

    dict_str = re.sub('{[a-zA-Z\.\_]*}', '', str(restriction_dict))

    new_dict = eval(dict_str)

    ret_dict = {}
    for k, v in new_dict.items():
        if len(v) == 1:
            ret_dict[k] = v[0]
        else:
            ret_dict[k] = v

    return str(ret_dict)

def add_template(template,**kwargs):
    """
        Add template and a type and typeattrs.
    """
    tmpl = Template()
    tmpl.template_name = template.name
    tmpl.layout        = str(template.layout)

    DBSession.add(tmpl)

    for templatetype in template.types:
        ttype = TemplateType()
        ttype.type_name = templatetype.name
        ttype.layout    = templatetype.layout
        ttype.resource_type = templatetype.resource_type
        ttype.alias         = templatetype.alias
        DBSession.add(ttype)

        for typeattr in templatetype.typeattrs:
            ta = TypeAttr(attr_id=typeattr.attr_id)
            ta.data_restriction = _parse_data_restriction(typeattr.data_restriction)
            ta.data_type        = typeattr.data_type
            ta.dimension        = typeattr.dimension
            ta.attr_is_var      = typeattr.is_var
            ttype.typeattrs.append(ta)
            DBSession.add(ta)
        tmpl.templatetypes.append(ttype)
    DBSession.flush()
    return tmpl

def update_template(template,**kwargs):
    """
        Update template and a type and typeattrs.
    """
    tmpl = DBSession.query(Template).filter(Template.template_id==template.id).one()
    tmpl.template_name = template.name
    tmpl.layout        = str(template.layout)

    for templatetype in template.types:
        if templatetype.id is not None:
            for type_i in tmpl.templatetypes:
                if type_i.template_id == templatetype.id:
                    type_i.type_name = templatetype.name
                    type_i.alias     = templatetype.alias
                    type_i.layout    = templatetype.layout
                    break
        else:
            type_i = TemplateType()
            type_i.type_name = templatetype.name
            type_i.layout    = templatetype.layout
            type_i.resource_type = templatetype.resource_type
            type_i.alias         = templatetype.alias
            DBSession.add(type_i)

        for typeattr in templatetype.typeattrs:
            for typeattr_i in type_i.typeattrs:
                if typeattr_i.attr_id == typeattr.attr_id:
                    typeattr_i.data_restriction = _parse_data_restriction(typeattr.data_restriction)
                    typeattr_i.data_type        = typeattr.data_type
                    typeattr_i.dimension        = typeattr.dimension
                    typeattr_i.attr_is_var      = typeattr.is_var

                    break
            else:
                ta = TypeAttr(attr_id=typeattr.attr_id)
                ta.data_restriction = _parse_data_restriction(typeattr.data_restriction)
                ta.data_type        = typeattr.data_type
                ta.dimension        = typeattr.dimension
                ta.attr_is_var      = typeattr.is_var
                type_i.typeattrs.append(ta)
                DBSession.add(ta)

    DBSession.flush()
 
    return tmpl

def get_templates(**kwargs):
    """
        Get all resource template templates.
    """

    templates = DBSession.query(Template).options(joinedload_all('templatetypes.typeattrs')).all()

    return templates 

def remove_attr_from_type(type_id, attr_id,**kwargs):
    """

        Remove an attribute from a type
    """
    typeattr_i = DBSession.query(TypeAttr).filter(TypeAttr.type_id==type_id,
                                                  TypeAttr.attr_id==attr_id).one()
    DBSession.delete(typeattr_i)

def get_template(template_id,**kwargs):
    """
        Get a specific resource template template, either by ID or name.
    """

    tmpl = DBSession.query(Template).filter(Template.template_id==template_id).one()

    return tmpl

def get_template_by_name(name,**kwargs):
    """
        Get a specific resource template, either by ID or name.
    """
    try:
        tmpl = DBSession.query(Template).filter(Template.template_name == name).one()
        return tmpl
    except NoResultFound:
        log.info("%s is not a valid identifier for a template",name)
        return None

def add_templatetype(templatetype,**kwargs):
    """
        Add a template type with typeattrs.
    """
    tmpltype = TemplateType()
    tmpltype.type_name  = templatetype.name
    tmpltype.resource_type = templatetype.resource_type
    tmpltype.alias      = templatetype.alias
    tmpltype.layout     = templatetype.layout

    for typeattr in templatetype.typeattrs:
        ta = TypeAttr(attr_id=typeattr.attr_id)
        tmpltype.typeattrs.append(ta)
        DBSession.add(ta)
    
    DBSession.add(tmpltype)
    DBSession.flush()

    return tmpltype

def update_templatetype(templatetype,**kwargs):
    """
        Update a resource type and its typeattrs.
        New typeattrs will be added. typeattrs not sent will be ignored.
        To delete typeattrs, call delete_typeattr
    """
    tmpltype = DBSession.query(TemplateType).filter(TemplateType.type_id == templatetype.id).one()
    tmpltype.type_name  = templatetype.name
    tmpltype.alias      = templatetype.alias
    tmpltype.layout     = templatetype.layout

    for typeattr in templatetype.typeattrs:
        for typeattr_i in tmpltype.typeattrs:
            if typeattr_i.attr_id == typeattr.attr_id:
                break
        else:
            ta = TypeAttr(attr_id=typeattr.attr_id)
            tmpltype.typeattrs.append(ta)
            DBSession.add(ta)

    DBSession.flush()

    return tmpltype

def get_templatetype(type_id,**kwargs):
    """
        Get a specific resource type by ID.
    """
    templatetype = DBSession.query(TemplateType).filter(TemplateType.type_id==type_id).one()

    return templatetype

def get_templatetype_by_name(template_id, type_name,**kwargs):
    """
        Get a specific resource type by name.
    """

    try:
        templatetype = DBSession.query(TemplateType).filter(TemplateType.template_id==template_id, TemplateType.type_name==type_name).one()
    except NoResultFound:
        raise HydraError("%s is not a valid identifier for a type"%(type_name))

    return templatetype

def add_typeattr(typeattr,**kwargs):
    """
        Add an typeattr to an existing type.
    """

    ta = TypeAttr()
    ta.type_id=typeattr.type_id
    ta.attr_id=typeattr.attr_id
    ta.attr_name = typeattr.attr_name
    ta.data_type = typeattr.data_type
    ta.dimension = typeattr.dimension
    ta.attr_is_var = typeattr.is_var
    ta.default_dataset_id = typeattr.default_dataset_id
    DBSession.add(ta)
    DBSession.flush()

    updated_template_type = DBSession.query(TemplateType).filter(TemplateType.type_id==ta.type_id).one()

    return updated_template_type


def delete_typeattr(typeattr,**kwargs):
    """
        Remove an typeattr from an existing type
    """
    ta = DBSession.query(TypeAttr).filter(TypeAttr.type_id == typeattr.type_id,
                                          TypeAttr.attr_id == typeattr.attr_id).one()
    DBSession.delete(ta)

    return 'OK'

def validate_attr(resource_attr_id, scenario_id, type_id=None):
    try:
        rs = DBSession.query(ResourceScenario).filter(ResourceScenario.resource_attr_id==resource_attr_id, ResourceScenario.scenario_id==scenario_id).options(joinedload_all("resourceattr")).options(joinedload_all("dataset")).one()

        _do_validate_resourcescenario(rs)
                    
    except NoResultFound:
        raise ResourceNotFoundError("Resource Scenario %s not found"%resource_attr_id)

def validate_attrs(resource_attr_ids, scenario_id, type_id=None):
    try:
        multi_rs = DBSession.query(ResourceScenario).filter(ResourceScenario.resource_attr_id.in_(resource_attr_ids), ResourceScenario.scenario_id==scenario_id).options(joinedload_all("resourceattr")).options(joinedload_all("dataset")).all()
        
        for rs in multi_rs:
            _do_validate_resourcescenario(rs, type_id)
                    
    except NoResultFound:
        raise ResourceNotFoundError("Resource Scenarios %s not found"%resource_attr_ids)


def _do_validate_resourcescenario(resourcescenario, type_id=None):
    res = resourcescenario.resourceattr.get_resource()

    types = res.types

    dataset = resourcescenario.dataset

    if len(types) == 0:
        return

    #Validate against all the types for the resource
    for resourcetype in types:
        #If a specific type has been specified, then only validate
        #against that type and ignore all the others
        if type_id is not None:
            if resourcetype.type_id != type_id:
                continue
        #Identify the template types for the template
        tmpltype = resourcetype.templatetype
        for ta in tmpltype.typeattrs:
            #If we find a template type which mactches the current attribute.
            #we can do some validation.
            if ta.attr_id == resourcescenario.resourceattr.attr_id:
                if ta.data_restriction:
                    validation_dict = eval(ta.data_restriction)
                    util.validate_value(validation_dict, dataset.get_val(raw=False))

def get_network_as_xml_template(network_id,**kwargs):
    """
        Turn an existing network into an xml template
        using its attributes.
        If an optional scenario ID is passed in, default
        values will be populated from that scenario.
    """
    template_xml = etree.Element("template_definition")

    net_i = DBSession.query(Network).filter(Network.network_id==network_id).one()

    template_name = etree.SubElement(template_xml, "template_name")
    template_name.text = "TemplateType from Network %s"%(net_i.network_name)
    layout = _get_layout_as_etree(net_i.network_layout)

    resources = etree.SubElement(template_xml, "resources")
    if net_i.attributes:
        net_resource    = etree.SubElement(resources, "resource")

        resource_type   = etree.SubElement(net_resource, "type")
        resource_type.text   = "NETWORK"

        resource_name   = etree.SubElement(net_resource, "name")
        resource_name.text   = net_i.network_name

        if net_i.network_layout is not None:
            resource_layout = etree.SubElement(net_resource, "layout")
            resource_layout.text = net_i.network_layout

        for net_attr in net_i.attributes:
            _make_attr_element(net_resource, net_attr)

    existing_types = {'NODE': [], 'LINK': []}
    for node_i in net_i.nodes:
        node_attributes = node_i.attributes
        attr_ids = [attr.attr_id for attr in node_attributes]
        if attr_ids>0 and attr_ids not in existing_types['NODE']:

            node_resource    = etree.Element("resource")

            resource_type   = etree.SubElement(node_resource, "type")
            resource_type.text   = "NODE"

            resource_name   = etree.SubElement(node_resource, "name")
            resource_name.text   = node_i.node_name

            layout = _get_layout_as_etree(node_i.node_layout)

            if layout is not None:
                node_resource.append(layout)

            for node_attr in node_attributes:
                _make_attr_element(node_resource, node_attr)

            existing_types['NODE'].append(attr_ids)
            resources.append(node_resource)

    for link_i in net_i.links:
        link_attributes = link_i.attributes
        attr_ids = [attr.attr_id for attr in link_attributes]
        if attr_ids>0 and attr_ids not in existing_types['LINK']:
            link_resource    = etree.Element("resource")

            resource_type   = etree.SubElement(link_resource, "type")
            resource_type.text   = "LINK"

            resource_name   = etree.SubElement(link_resource, "name")
            resource_name.text   = link_i.link_name

            layout = _get_layout_as_etree(link_i.link_layout)

            if layout is not None:
                link_resource.append(layout)

            for link_attr in link_attributes:
                _make_attr_element(link_resource, link_attr)

            existing_types['LINK'].append(attr_ids)
            resources.append(link_resource)

    xml_string = etree.tostring(template_xml)

    return xml_string

def _make_attr_element(parent, resource_attr_i):
    """
        General function to add an attribute element to a resource element.
    """
    attr = etree.SubElement(parent, "attribute")
    attr_i = resource_attr_i.attr

    attr_name      = etree.SubElement(attr, 'name')
    attr_name.text = attr_i.attr_name

    attr_dimension = etree.SubElement(attr, 'dimension')
    attr_dimension.text = attr_i.attr_dimen

    attr_is_var    = etree.SubElement(attr, 'is_var')
    attr_is_var.text = resource_attr_i.attr_is_var

    # if scenario_id is not None:
    #     for rs in resource_attr_i.get_resource_scenarios():
    #         if rs.scenario_id == scenario_id
    #             attr_default   = etree.SubElement(attr, 'default')
    #             default_val = etree.SubElement(attr_default, 'value')
    #             default_val.text = rs.get_dataset().get_val()
    #             default_unit = etree.SubElement(attr_default, 'unit')
    #             default_unit.text = rs.get_dataset().unit

    return attr

def get_layout_as_dict(layout_tree):
    """
    Convert something that looks like this:
    <layout>
        <item>
            <name>color</name>
            <value>red</value>
        </item>
        <item>
            <name>shapefile</name>
            <value>blah.shp</value>
        </item>
    </layout>
    Into something that looks like this:
    {
        'color' : ['red'],
        'shapefile' : ['blah.shp']
    }
    """
    layout_dict = dict()

    for item in layout_tree.findall('item'):
        name  = item.find('name').text
        val_element = item.find('value')
        value = val_element.text.strip()
        if value == '':
            children = val_element.getchildren()
            value = etree.tostring(children[0], pretty_print=True)
        layout_dict[name] = [value]
    return layout_dict

def _get_layout_as_etree(layout_dict):
    """
    Convert something that looks like this:
    {
        'color' : ['red'],
        'shapefile' : ['blah.shp']
    }

    Into something that looks like this:
    <layout>
        <item>
            <name>color</name>
            <value>red</value>
        </item>
        <item>
            <name>shapefile</name>
            <value>blah.shp</value>
        </item>
    </layout>
    """
    if layout_dict is None:
        return None

    layout = etree.Element("layout")
    layout_dict = eval(layout_dict)
    for k, v in layout_dict.items():
        item = etree.SubElement(layout, "item")
        name = etree.SubElement(item, "name")
        name.text = k
        value = etree.SubElement(item, "value")
        value.text = str(v)

    return layout


