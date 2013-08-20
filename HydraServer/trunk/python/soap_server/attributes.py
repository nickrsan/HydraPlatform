from spyne.service import ServiceBase
import logging
from HydraLib.HydraException import HydraError
from spyne.model.primitive import Integer, Boolean
from spyne.decorator import rpc
from hydra_complexmodels import Attr
from db import HydraIface
from hydra_complexmodels import Network,\
        Node,\
        Link,\
        Project,\
        Scenario,\
        ResourceTemplateGroup,\
        ResourceTemplate

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

class AttributeService(ServiceBase):
    """
        The attribute SOAP service
    """

    @rpc(Attr, _returns=Attr)
    def add_attribute(ctx, attr):
        """
            Add a generic attribute, which can then be used in creating
            a resource attribute, and put into a template.
            (Attr){
                attr_id = 1020
                attr_name = "Test Attr"
                attr_dimen = "very big"
            }

        """

        x = HydraIface.Attr()
        x.db.attr_name = attr.attr_name
        x.db.attr_dimen = attr.attr_dimen
        x.save()
        return x.get_as_complexmodel()

    @rpc(Integer, _returns=Boolean)
    def delete_attribute(ctx, attr_id):
        """
            Set the status of an attribute to 'X'
        """
        success = True
        try:
            x = HydraIface.Attr(attr_id = attr_id)
            x.db.status = 'X'
            x.save()
        except HydraError, e:
            logging.critical(e)
            success = False
        return success
        

    @rpc(Integer, Integer, _returns=Node)
    def add_node_attribute(ctx, node_id, attr_id):
        """
            Add an attribute to a node.
        """

    @rpc(Integer, Integer, _returns=Link)
    def add_link_attribute(ctx, link_id, attr_id):
        """
            Add an attribute to a link.
        """
    @rpc(Integer, Integer, _returns=Network)
    def add_network_attribute(ctx, network_id, attr_id):
        """
            Add an attribute to a network.
        """
    @rpc(Integer, Integer, _returns=Scenario)
    def add_scenaro_attribute(ctx, scenario_id, attr_id):
        """
            Add an attribute to a scenario.
        """
    @rpc(Integer, Integer,_returns=Project)
    def add_project_attribute(ctx, project_id, attr_id):
        """
            Add an attribute to a project.
        """

    @rpc(ResourceTemplateGroup, _returns=ResourceTemplateGroup)
    def add_template_group(ctx, group):
        """
            Add a template group
        """
        pass

    @rpc(ResourceTemplateGroup, _returns=ResourceTemplateGroup)
    def update_template_group(ctx, group):
        """
            Add a template group
        """
        pass

    @rpc(ResourceTemplate, _returns=ResourceTemplate)
    def add_resource_template(ctx, resourcetemplate):
        """
            Add a resource template
        """
        pass

    @rpc(ResourceTemplate, _returns=ResourceTemplate)
    def update_resource_template(ctx, resourcetemplate):
        """
            Add a resource template
        """
        pass

    @rpc(Integer, ResourceTemplate, _returns=ResourceTemplateGroup)
    def add_resource_template_to_group(ctx, group_id, template):
        """
            Add a template to a group
        """
        pass

    @rpc(Integer, Attr, _returns=ResourceTemplate)
    def add_attr_to_template(ctx, template_id, attr):
        """

            Add an attribute to a resourcetemplate, creating a resourcetemplateitem.
        """
        pass

    @rpc(Integer, Node, _returns=Node)
    def add_node_attrs_from_template(ctx, template_id, node):
        """
            adds all the attributes defined by a template to a node.
        """
        pass

    @rpc(Integer, Link, _returns=Node)
    def add_link_attrs_from_template(ctx, template_id, link):
        """
            adds all the attributes defined by a template to a link.
        """
        pass

    @rpc(Integer, Network, _returns=Network)
    def add_network_attrs_from_template(ctx, template_id, network):
        """
            adds all the attributes defined by a template to a link.
        """
        pass


    @rpc(Integer, Scenario, _returns=Scenario)
    def add_scenario_attrs_from_template(ctx, template_id, Scenario):
        """
            adds all the attributes defined by a template to a scenario.
        """
        pass

    @rpc(Integer, Project, _returns=Project)
    def add_project_attrs_from_template(ctx, template_id, project):
        """
            adds all the attributes defined by a template to a project.
        """
        pass
