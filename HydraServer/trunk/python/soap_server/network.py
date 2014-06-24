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
from spyne.model.primitive import Unicode, Integer, Boolean
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_complexmodels import Network,\
    Node,\
    Link,\
    Scenario,\
    ResourceGroup,\
    NetworkExtents
from lib import network
from hydra_base import HydraService

class NetworkService(HydraService):
    """
        The network SOAP service.
    """

    @rpc(Network, _returns=Network)
    def add_network(ctx, net):
        """
        Takes an entire network complex model and saves it to the DB.  This
        complex model includes links & scenarios (with resource data).  Returns
        the network's complex model.

        As links connect two nodes using the node_ids, if the nodes are new
        they will not yet have node_ids. In this case, use negative ids as
        temporary IDS until the node has been given an permanent ID.

        All inter-object referencing of new objects should be done using
        negative IDs in the client.

        The returned object will have positive IDS

        """
        net = network.add_network(net, **ctx.in_header.__dict__)
        ret_net = Network(net, summary=True)
        return ret_net

    @rpc(Integer,
         Unicode(pattern="[YN]", default='Y'),
         Integer(),
         Integer(min_occurs="0", max_occurs="unbounded"),
         _returns=Network)
    def get_network(ctx, network_id, include_data, template_id, scenario_ids):
        """
            Return a whole network as a complex model.
        """
        net  = network.get_network(network_id,
                                   include_data,
                                   scenario_ids,
                                   template_id,
                                   **ctx.in_header.__dict__)
        ret_net = Network(net)
        return ret_net

    @rpc(Integer, Unicode, _returns=Network)
    def get_network_by_name(ctx, project_id, network_name):
        """
        Return a whole network as a complex model.
        """

        net = network.get_network_by_name(project_id, network_name, **ctx.in_header.__dict__)

        return Network(net)

    @rpc(Integer, Unicode, _returns=Unicode)
    def network_exists(ctx, project_id, network_name):
        """
        Return a whole network as a complex model.
        """

        net_exists = network.network_exists(project_id, network_name, **ctx.in_header.__dict__)

        return net_exists

    @rpc(Network, _returns=Network)
    def update_network(ctx, net):
        """
            Update an entire network
        """
        net = network.update_network(net, **ctx.in_header.__dict__)
        return Network(net, summary=True)

    @rpc(Integer, _returns=Node)
    def get_node(ctx, node_id):
        node = network.get_node(node_id, **ctx.in_header.__dict__)
        return Node(node)

    @rpc(Integer, _returns=Link)
    def get_link(ctx, link_id):
        link = network.get_link(link_id, **ctx.in_header.__dict__)
        return Link(link)

    @rpc(Integer, Boolean, _returns=Unicode)
    def delete_network(ctx, network_id, purge_data):
        """
        Deletes a network. This does not remove the network from the DB. It
        just sets the status to 'X', meaning it can no longer be seen by the
        user.
        """
        #check_perm('delete_network')
        network.delete_network(network_id, purge_data, **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, _returns=NetworkExtents)
    def get_network_extents(ctx, network_id):
        """
        Given a network, return its maximum extents.
        This would be the minimum x value of all nodes,
        the minimum y value of all nodes,
        the maximum x value of all nodes and
        maximum y value of all nodes.

        @returns NetworkExtents object
        """
        extents = network.get_network_extents(network_id, **ctx.in_header.__dict__)
        
        ne = NetworkExtents()
        ne.network_id = extents['network_id']
        ne.min_x = extents['min_x']
        ne.max_x = extents['max_x']
        ne.min_y = extents['min_y']
        ne.max_y = extents['max_y']

        return ne

    @rpc(Integer, Node, _returns=Node)
    def add_node(ctx, network_id, node):

        """
        Add a node to a network:

        .. code-block:: python

            (Node){
               id = 1027
               name = "Node 1"
               description = "Node Description"
               x = 0.0
               y = 0.0
               attributes =
                  (ResourceAttrArray){
                     ResourceAttr[] =
                        (ResourceAttr){
                           attr_id = 1234
                        },
                        (ResourceAttr){
                           attr_id = 4321
                        },
                  }
             }
        """

        node_dict = network.add_node(network_id, node, **ctx.in_header.__dict__)

        new_node = Node(node_dict)

        return new_node

    @rpc(Node, _returns=Node)
    def update_node(ctx, node):
        """
        Update a node.
        If new attributes are present, they will be added to the node.
        The non-presence of attributes does not remove them.

        .. code-block:: python

            (Node){
               id = 1039
               name = "Node 1"
               description = "Node Description"
               x = 0.0
               y = 0.0
               status = "A"
               attributes =
                  (ResourceAttrArray){
                     ResourceAttr[] =
                        (ResourceAttr){
                           id = 850
                           attr_id = 1038
                           ref_id = 1039
                           ref_key = "NODE"
                           attr_is_var = True
                        },
                        (ResourceAttr){
                           id = 852
                           attr_id = 1040
                           ref_id = 1039
                           ref_key = "NODE"
                           attr_is_var = True
                        },
                  }
             }

        """
            
        node_dict = network.update_node(node, **ctx.in_header.__dict__)
        updated_node = Node(node_dict)

        return updated_node

    @rpc(Integer, Boolean,  _returns=Unicode)
    def delete_resourceattr(ctx, resource_attr_id, purge_data):
        """
            Deletes a resource attribute and all associated data.
        """
        network.delete_resourceattr(resource_attr_id, purge_data, **ctx.in_header.__dict__)
        return 'OK'


    @rpc(Integer, _returns=Unicode)
    def delete_node(ctx, node_id):
        """
            Set the status of a node to 'X'
        """
        #check_perm('edit_topology')
        network.delete_node(node_id, **ctx.in_header.__dict__)
        return 'OK' 

    @rpc(Integer, Boolean, _returns=Unicode)
    def purge_node(ctx, node_id, purge_data):
        """
            Remove node from DB completely
            If there are attributes on the node, use purge_data to try to
            delete the data. If no other resources link to this data, it
            will be deleted.

        """
        network.purge_node(node_id, purge_data, **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, Link, _returns=Link)
    def add_link(ctx, network_id, link):
        """
            Add a link to a network
        """

        link_dict = network.add_link(network_id, link, **ctx.in_header.__dict__)
        new_link = Link(link_dict)

        return new_link

    @rpc(Link, _returns=Link)
    def update_link(ctx, link):
        """
            Update a link.
        """
        link_dict = network.update_link(link, **ctx.in_header.__dict__)
        updated_link = Link(link_dict)

        return updated_link 

    @rpc(Integer, _returns=Unicode)
    def delete_link(ctx, link_id):
        """
            Set the status of a link to 'X'
        """
        network.update_link(link_id, **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, Boolean, _returns=Unicode)
    def purge_link(ctx, link_id, purge_data):
        """
            Remove link from DB completely
            If there are attributes on the link, use purge_data to try to
            delete the data. If no other resources link to this data, it
            will be deleted.
        """
        network.purge_link(link_id, **ctx.in_header.__dict__)
        return 'OK'

    @rpc(Integer, ResourceGroup, _returns=ResourceGroup)
    def add_group(ctx, network_id, group):
        """
            Add a resourcegroup to a network
        """

        group_i = network.add_group(network_id, group, **ctx.in_header.__dict__)
        new_group = ResourceGroup(group_i)

        return new_group

    @rpc(Integer, _returns=SpyneArray(Scenario))
    def get_scenarios(ctx, network_id):
        """
            Get all the scenarios in a given network.
        """
        scenarios_i = network.get_scenarios(network_id, **ctx.in_header.__dict__)

        scenarios = []
        for scen in scenarios_i:
            scen.load()
            s_complex = Scenario(scen)
            scenarios.append(s_complex)

        return scenarios

    @rpc(Integer, _returns=SpyneArray(Integer))
    def validate_network_topology(ctx, network_id):
        """
            Check for the presence of orphan nodes in a network.
        """
        return network.validate_network_topology(network_id, **ctx.in_header.__dict__)
