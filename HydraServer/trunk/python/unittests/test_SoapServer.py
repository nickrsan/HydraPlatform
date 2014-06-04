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
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import unittest
import logging
import shutil
import os

from HydraLib import config
from HydraLib.util import get_datetime

from suds.client import Client
from suds.plugin import MessagePlugin
from tempfile import gettempdir as tmp
import datetime

global CLIENT
CLIENT = None

log = logging.getLogger(__name__)

class FixNamespace(MessagePlugin):
    def marshalled(self, context):
        self.fix_ns(context.envelope)

    def fix_ns(self, element):
        if element.prefix == 'xs':
            element.prefix = 'ns0'

        for e in element.getChildren():
            self.fix_ns(e)

def connect():
    port = config.getint('hydra_server', 'port', 12345)
    url = 'http://localhost:%s?wsdl' % port
    client = Client(url, plugins=[FixNamespace()])

    client.add_prefix('hyd', 'soap_server.hydra_complexmodels')
    global CLIENT
    CLIENT = client
    return client

class SoapServerTest(unittest.TestCase):

    def setUp(self):
        logging.getLogger('suds').setLevel(logging.ERROR)
        logging.getLogger('suds.client').setLevel(logging.CRITICAL)
        logging.getLogger('suds.metrics').setLevel(logging.CRITICAL)
        # Clear SUDS cache:
        shutil.rmtree(os.path.join(tmp(), 'suds'), True)
        global CLIENT
        if CLIENT is None:
            connect()

        self.client = CLIENT

        self.login('root', '')


    def tearDown(self):
        log.debug("Tearing down")
        self.logout('root')

    def login(self, username, password):
        login_response = self.client.service.login(username, password)

        session_id = login_response.session_id
        user_id    = login_response.user_id

        token = self.client.factory.create('RequestHeader')
        token.session_id = session_id
        token.username = username
        token.user_id  = user_id

        self.client.set_options(cache=None, soapheaders=token)

    def logout(self, username):
        msg = self.client.service.logout(username)
        return msg

    def create_user(self, name):

        existing_user = self.client.service.get_user_by_name(name)
        if existing_user is not None:
            return existing_user

        user = self.client.factory.create('hyd:User')
        user.username = name
        user.password = "password"
        user.display_name = "test useer"

        new_user = self.client.service.add_user(user)

        #make the user an admin user by default
        role =  self.client.service.get_role_by_code('admin')

        self.client.service.set_user_role(new_user.id, role.id)

        return new_user

    def create_template(self):
        template = self.client.service.get_template_by_name('Test Template')

        if template is not None:
            return template

        link_attr_1 = self.create_attr("tmpl_attr_1", dimension='Pressure')
        link_attr_2 = self.create_attr("tmpl_attr_2", dimension='Speed')
        node_attr_1 = self.create_attr("tmpl_attr_1", dimension='Volume')
        node_attr_2 = self.create_attr("tmpl_attr_2", dimension='Speed')

        template = self.client.factory.create('hyd:Template')
        template.name = 'Test Template'


        types = self.client.factory.create('hyd:TemplateTypeArray')
        #**********************
        #type 1           #
        #**********************
        type1 = self.client.factory.create('hyd:TemplateType')
        type1.name = "Test type 1"
        type1.alias = "Test type alias"
        type1.resource_type='NODE'

        typeattrs = self.client.factory.create('hyd:TypeAttrArray')

        typeattr_1 = self.client.factory.create('hyd:TypeAttr')
        typeattr_1.attr_id = node_attr_1.id
        typeattrs.TypeAttr.append(typeattr_1)

        typeattr_2 = self.client.factory.create('hyd:TypeAttr')
        typeattr_2.attr_id = node_attr_2.id
        typeattrs.TypeAttr.append(typeattr_2)

        type1.typeattrs = typeattrs

        types.TemplateType.append(type1)
        #**********************
        #type 2           #
        #**********************
        type2 = self.client.factory.create('hyd:TemplateType')
        type2.name = "Test type 2"
        type2.resource_type='LINK'

        typeattrs = self.client.factory.create('hyd:TypeAttrArray')

        typeattr_1 = self.client.factory.create('hyd:TypeAttr')
        typeattr_1.attr_id = link_attr_1.id
        typeattrs.TypeAttr.append(typeattr_1)

        typeattr_2 = self.client.factory.create('hyd:TypeAttr')
        typeattr_2.attr_id = link_attr_2.id
        typeattrs.TypeAttr.append(typeattr_2)

        type2.typeattrs = typeattrs

        types.TemplateType.append(type2)

        template.types = types

        new_template = self.client.service.add_template(template)

        assert new_template.name == template.name, "Names are not the same!"
        assert new_template.id is not None, "New Template has no ID!"
        assert new_template.id > 0, "New Template has incorrect ID!"

        assert len(new_template.types) == 1, "Resource types did not add correctly"
        for t in new_template.types.TemplateType[0].typeattrs.TypeAttr:
            assert t.attr_id in (node_attr_1.id, node_attr_2.id);
            "Node types were not added correctly!"

        for t in new_template.types.TemplateType[1].typeattrs.TypeAttr:
            assert t.attr_id in (link_attr_1.id, link_attr_2.id);
            "Node types were not added correctly!"

        return new_template

    def create_project(self, name):
        project = self.client.factory.create('hyd:Project')
        project.name = 'SOAP test %s'%(datetime.datetime.now())
        project = self.client.service.add_project(project)
        return project

    def create_network(self, project):
        network = self.client.factory.create('hyd:Network')
        network.name = 'Test network @ %s'% datetime.datetime.now()
        network.description = 'A test network.'
        network.project_id = project.id
        network.nodes = []
        network.links = []
        network.scenarios = []
        network.layout = None
        network = self.client.service.add_network(network)
        return network

    def create_link(self, node_1_name, node_2_name, node_1_id, node_2_id):

        ra_array = self.client.factory.create('hyd:ResourceAttrArray')

        link = {
            'id'          : None,
            'name'        : "%s_to_%s"%(node_1_name, node_2_name),
            'description' : 'A test link between two nodes.',
            'layout'      : None,
            'node_1_id'   : node_1_id,
            'node_2_id'   : node_2_id,
            'attributes'  : ra_array,
        }

        return link

    def create_node(self,node_id, attributes=None, node_name="Test Node Name"):

        if attributes is None:
            attributes = self.client.factory.create('hyd:ResourceAttrArray')
        #turn 0 into 1, -1 into 2, -2 into 3 etc..
        coord = (node_id * -1) + 1
        node = {
            'id' : node_id,
            'name' : node_name,
            'description' : "A node representing a water resource",
            'layout'      : None,
            'x' : 10 * coord,
            'y' : 10 * coord -1,
            'attributes' : attributes,
        }

        return node

    def create_attr(self, name="Test attribute", dimension="dimensionless"):
        attr = self.client.service.get_attribute(name, dimension)
        if attr is None:
            attr = {'name'  : name,
                    'dimen' : dimension
                   }
            attr = self.client.service.add_attribute(attr)
        return attr

    def test_create_network(self):
        self.create_network_with_data()

    def create_network_with_data(self, project_id=None, num_nodes=10):
        """
            Test adding data to a network through a scenario.
            This test adds attributes to one node and then assignes data to them.
            It assigns a descriptor, array and timeseries to the
            attributes node.
        """
        start = datetime.datetime.now()
        if project_id is None:
            (project) = {
                'name'        : 'New Project %s'%(datetime.datetime.now()),
                'description' : 'New Project Description',
            }
            p =  self.client.service.add_project(project)
            project_id = p.id

        log.debug("Project creation took: %s"%(datetime.datetime.now()-start))
        start = datetime.datetime.now()

        link_attr1 = self.create_attr("link_attr_1", dimension='Pressure')
        link_attr2 = self.create_attr("link_attr_2", dimension='Speed')
        node_attr1 = self.create_attr("node_attr_1", dimension='Volume')
        node_attr2 = self.create_attr("node_attr_2", dimension='Speed')
        node_attr3 = self.create_attr("node_attr_3", dimension='Current')
        group_attr = self.create_attr("group_attr", dimension='Volume')

        template = self.create_template()

        log.debug("Attribute creation took: %s"%(datetime.datetime.now()-start))
        start = datetime.datetime.now()

       
        #Put an attribute on a group
        group_ra = dict(
            ref_id  = None,
            ref_key = 'GROUP',
            attr_is_var = 'N',
            attr_id = group_attr.id,
            id      = -1
        )
        group_attrs = self.client.factory.create('hyd:ResourceAttrArray')
        group_attrs.ResourceAttr = [group_ra]

        nodes = []
        links = []

        prev_node = None
        ra_index = 2
        for n in range(num_nodes):
            node = self.create_node(n*-1, node_name="Node %s"%(n))

            #From our attributes, create a resource attr for our node
            #We don't assign data directly to these resource attributes. This
            #is done when creating the scenario -- a scenario is just a set of
            #data for a given list of resource attributes.
            node_ra1         = dict(
                ref_key = 'NODE',
                ref_id  = None,
                attr_id = node_attr1.id,
                id      = ra_index * -1,
                attr_is_var = 'N',
            )
            ra_index = ra_index + 1
            node_ra2         = dict(
                ref_key = 'NODE',
                ref_id  = None,
                attr_id = node_attr2.id,
                id      = ra_index * -1,
                attr_is_var = 'Y',
            )
            ra_index = ra_index + 1
            node_ra3         = dict(
                ref_key = 'NODE',
                ref_id  = None,
                attr_id = node_attr3.id,
                id      = ra_index * -1,
                attr_is_var = 'N',
            )
            ra_index = ra_index + 1

            node['attributes'].ResourceAttr = [node_ra1, node_ra2, node_ra3] 

            type_summary_arr = self.client.factory.create('hyd:TypeSummaryArray')


            type_summary = self.client.factory.create('hyd:TypeSummary')
            type_summary.id = template.id
            type_summary.name = template.name
            type_summary.id = template.types.TemplateType[0].id
            type_summary.name = template.types.TemplateType[0].name

            type_summary_arr.TypeSummary.append(type_summary)

            node['types'] = type_summary_arr

            nodes.append(node)

            if prev_node is not None:
                #Connect the two nodes with a link
                link = self.create_link(
                    node['name'],
                    prev_node['name'],
                    node['id'],
                    prev_node['id'])

                link_ra1         = dict(
                    ref_id  = None,
                    ref_key = 'LINK',
                    id     = ra_index * -1,
                    attr_id = link_attr1.id,
                    attr_is_var = 'N',
                )
                ra_index = ra_index + 1
                link_ra2         = dict(
                    ref_id  = None,
                    ref_key = 'LINK',
                    attr_id = link_attr2.id,
                    id      = ra_index * -1,
                    attr_is_var = 'N',
                )
                ra_index = ra_index + 1
                
                link['attributes'].ResourceAttr = [link_ra1, link_ra2] 
                
                links.append(link)

            prev_node = node 

        #A network must contain an array of links. In this case, the array

        log.debug("Making nodes & links took: %s"%(datetime.datetime.now()-start))
        start = datetime.datetime.now()

        #Create the scenario
        scenario = self.client.factory.create('hyd:Scenario')
        scenario.id = -1
        scenario.name        = 'Scenario 1'
        scenario.description = 'Scenario Description'
        scenario.start_time  = datetime.datetime.now()
        scenario.end_time    = scenario.start_time + datetime.timedelta(hours=1)
        scenario.time_step   = 1 # one second intervals.

        #Multiple data (Called ResourceScenario) means an array.
        scenario_data = self.client.factory.create('hyd:ResourceScenarioArray')

        group_array       = self.client.factory.create('hyd:ResourceGroupArray')
        group             = self.client.factory.create('hyd:ResourceGroup')
        group.id          = -1
        group.name        = "Test Group"
        group.description = "Test group description"

        group.attributes = group_attrs

        group_array.ResourceGroup.append(group)

        group_item_array      = self.client.factory.create('hyd:ResourceGroupItemArray')
        group_item_1 = dict(
            ref_key  = 'NODE',
            ref_id   = nodes[0]['id'],
            group_id = group['id'],
        )
        group_item_2  = dict(
            ref_key  = 'NODE',
            ref_id   = nodes[1]['id'],
            group_id = group['id'],
        )
        group_item_array.ResourceGroupItem = [group_item_1, group_item_2]

        scenario.resourcegroupitems = group_item_array

        #This is an example of 3 diffent kinds of data
        
        #For Links use the following:
        #A simple string (Descriptor)
        #A multi-dimensional array.

        #For nodes, use the following:
        #A time series, where the value may be a 1-D array

    
        for n in nodes:
            for na in n['attributes'].ResourceAttr:
                if na.get('attr_is_var', 'N') == 'N':
                    if na['attr_id'] == node_attr1['id']:
                        timeseries = self.create_timeseries(na)
                        scenario_data.ResourceScenario.append(timeseries)
                    elif na['attr_id'] == node_attr3['id']:
                        eqtimeseries = self.create_eqtimeseries(na)
                        scenario_data.ResourceScenario.append(eqtimeseries)

        for l in links:
            for na in l['attributes'].ResourceAttr:
                if na['attr_id'] == link_attr1['id']:
                    array      = self.create_array(na)
                    scenario_data.ResourceScenario.append(array)
                elif na['attr_id'] == link_attr2['id']:
                    descriptor = self.create_descriptor(na)
                    scenario_data.ResourceScenario.append(descriptor)


        grp_timeseries = self.create_timeseries(group_attrs.ResourceAttr[0])

        scenario_data.ResourceScenario.append(grp_timeseries)

        #Set the scenario's data to the array we have just populated
        scenario.resourcescenarios = scenario_data

        #A network can have multiple scenarios, so they are contained in
        #a scenario array
        scenario_array = self.client.factory.create('hyd:ScenarioArray')
        scenario_array.Scenario.append(scenario)

        log.debug("Scenario definition took: %s"%(datetime.datetime.now()-start))
        #This can also be defined as a simple dictionary, but I do it this
        #way so I can check the value is correct after the network is created.
        layout = self.client.factory.create("xs:anyType")
        layout.color = 'red'
        layout.shapefile = 'blah.shp'

        node_array = self.client.factory.create("hyd:NodeArray")
        node_array.Node = nodes
        link_array = self.client.factory.create("hyd:LinkArray")
        link_array.Link = links

        (network) = {
            'name'        : 'Network @ %s'%datetime.datetime.now(),
            'description' : 'Test network with 2 nodes and 1 link',
            'project_id'  : project_id,
            'links'       : link_array,
            'nodes'       : node_array,
            'layout'      : layout,
            'scenarios'   : scenario_array,
            'resourcegroups' : group_array,
        }

        #log.debug(network)
        start = datetime.datetime.now()
        log.info("Creating network...")
        response_network = self.client.service.add_network(network)

        self.check_network(network, response_network)
     
        log.info("Network Creation took: %s"%(datetime.datetime.now()-start))

        return response_network

    def check_network(self, request_net, response_net):

        assert repr(response_net.layout) == repr(request_net['layout'])


        before_times = [] 

        s = request_net['scenarios'].Scenario[0]
        for rs0 in s['resourcescenarios'].ResourceScenario:
            if rs0['value']['type'] == 'timeseries':
                val = rs0['value']['value']
                for v in val['ts_values']:
                    before_times.append(v['ts_time'])

        after_times = []
        s = response_net.scenarios.Scenario[0]
        for rs0 in s.resourcescenarios.ResourceScenario:
            if rs0.value.type == 'timeseries':
                val = rs0.value.value
                for v in val['ts_values']:
                    after_times.append(v['ts_time'])
        for d in after_times:
            assert get_datetime(d) in before_times, "%s is incorrect"%(d)


    def create_descriptor(self, ResourceAttr, val="test"):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.

        dataset = dict(
            id=None,
            type = 'descriptor',
            name = 'Flow speed',
            unit = 'm s^-1',
            dimension = 'Speed',
            locked = 'N',
            value = {'desc_val':val},
        )

        scenario_attr = dict(
            attr_id = ResourceAttr['attr_id'],
            resource_attr_id = ResourceAttr['id'],
            value = dataset,
        )

        return scenario_attr


    def create_timeseries(self, ResourceAttr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        #[[[1, 2, 3], [5, 4, 6]], [[10, 20, 30], [40, 50, 60]]]

        test_val_1 = {'array':[
                        {'array':[
                            {'array':[
                                {'item':[1.0, 2.0, "hello"]},
                            ]},
                            {'array' : [
                                {'item':[5.0, 4.0, 6.0]},
                            ]},
                        ]},
                        {'array':[
                            {'array' : [
                                {'item':[10.0, 20.0, 30.0]},
                            ]},
                            {'array' : [
                                {'item':[40.0, 50.0, 60.0]},
                            ]},
                        ]}
                    ]}

        test_val_2 = {'array' : [
                        {'item':[1.0, 2.0, 3.0]},
                    ]}

        metadata_array = self.client.factory.create("hyd:MetadataArray")
        metadata = self.client.factory.create("hyd:Metadata")
        metadata.name = 'created_by'
        metadata.value = 'Test user'
        metadata_array.Metadata.append(metadata)

        dataset = dict(
            id=None,
            type = 'timeseries',
            name = 'my time series',
            unit = 'cm^3',
            dimension = 'Volume',
            locked = 'N',
            value = {'ts_values' : 
            [
                {'ts_time' : datetime.datetime.now(),
                'ts_value' : test_val_1},
                {'ts_time' : datetime.datetime.now()+datetime.timedelta(hours=1),
                'ts_value' : test_val_2},
                {'ts_time' : datetime.datetime.now()+datetime.timedelta(hours=2),
                'ts_value' : 3.0},

            ]
        },
            metadata = metadata_array, 
        )

        scenario_attr = dict(
            attr_id = ResourceAttr['attr_id'],
            resource_attr_id = ResourceAttr['id'],
            value = dataset,
        )

        return scenario_attr

    def create_array(self, ResourceAttr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        #[[1, 2, 3], [4, 5, 6], [7, 8, 9]]
        arr= {'arr_data' :
            {'array':[
                {'array':[
                        {'item':[1.0, 2.0, 3.0]},
                ]},
                {'array':[
                        {'item':[4.0, 5.0, 6.0]},
                ]},
                {'array' : [
                        {'item':[7.0, 8.0, 9.0]},
                ]},
            ]} 
        }
        
        metadata_array = self.client.factory.create("hyd:MetadataArray")
        metadata = self.client.factory.create("hyd:Metadata")
        metadata.name = 'created_by'
        metadata.value = 'Test user'
        metadata_array.Metadata.append(metadata)

        dataset = dict(
            id=None,
            type = 'array',
            name = 'my array',
            unit = 'bar',
            dimension = 'Pressure',
            locked = 'N',
            value = arr,
            metadata = metadata_array, 
        )

        scenario_attr = dict(
            attr_id = ResourceAttr['attr_id'],
            resource_attr_id = ResourceAttr['id'],
            value = dataset,
        )


        return scenario_attr

    def create_eqtimeseries(self, ResourceAttr):
        #A scenario attribute is a piece of data associated
        #with a resource attribute.
        #[[1, 2, 3], [4, 5, 6], [7, 8, 9]]
        ts_val = {
            'start_time' : datetime.datetime.now(),
            'frequency'  : 3600.0,
            'arr_data': 
                    {'array':[
                            {'item':[9, 210, 11]},
                    ]},
        }

        dataset = dict(
            id=None,
            type = 'eqtimeseries',
            name = 'my equally spaced timeseries',
            unit = 'amps',
            dimension = 'Current',
            locked = 'N',
            value = ts_val,
        )

        scenario_attr = dict(
            attr_id = ResourceAttr['attr_id'],
            resource_attr_id = ResourceAttr['id'],
            value = dataset,
        )


        return scenario_attr




    def create_constraint(self, net, constant=5):
        #We are trying to achieve a structure that looks like:
        #(((A + 5) * B) - C) == 0
        #3 groups
        #4 items
        #group 1 contains group 2 & item C
        #group 2 contains group 3 & item B
        #group 3 contains item A and item 5
        #The best way to do this is in reverse order. Start with the inner
        #group and work your way out.

        #create all the groups & items
        #Innermost group first (A + 5)
        group_3 = self.client.factory.create('hyd:ConstraintGroup')
        group_3.op = '+'

        item_1 = self.client.factory.create('hyd:ConstraintItem')
        item_1.resource_attr_id = net.nodes.Node[0].attributes.ResourceAttr[0].id
        item_2 = self.client.factory.create('hyd:ConstraintItem')
        item_2.constant = constant

        #set the items in group 3 (aka 'A' and 5 from example above)
        group_3_items = self.client.factory.create('hyd:ConstraintItemArray')
        group_3_items.ConstraintItem.append(item_1)
        group_3_items.ConstraintItem.append(item_2)

        group_3.constraintitems = group_3_items

        #Then the next group out (group_1 * B)
        #Group 2 (which has both an item and a group)
        group_2 = self.client.factory.create('hyd:ConstraintGroup')
        group_2.op = '*'

        item_3 = self.client.factory.create('hyd:ConstraintItem')
        item_3.resource_attr_id = net.nodes.Node[0].attributes.ResourceAttr[1].id

        group_2_items = self.client.factory.create('hyd:ConstraintItemArray')
        group_2_items.ConstraintItem.append(item_3)

        group_2_groups = self.client.factory.create('hyd:ConstraintGroupArray')
        group_2_groups.ConstraintGroup.append(group_3)

        group_2.constraintgroups = group_2_groups
        group_2.constraintitems  = group_2_items

        #Then the outermost group: (group_2 - C)
        #Group 1 has  also has an item and a group
        group_1 = self.client.factory.create('hyd:ConstraintGroup')
        group_1.op = '-'

        item_4 = self.client.factory.create('hyd:ConstraintItem')
        item_4.resource_attr_id = net.links.Link[0].attributes.ResourceAttr[0].id


        group_1_items = self.client.factory.create('hyd:ConstraintItemArray')
        group_1_items.ConstraintItem.append(item_4)

        group_1_groups = self.client.factory.create('hyd:ConstraintGroupArray')
        group_1_groups.ConstraintGroup.append(group_2)
        group_1.constraintgroups = group_1_groups
        group_1.constraintitems = group_1_items


        constraint = self.client.factory.create('hyd:Constraint')
        constraint.scenario_id = net.scenarios[0][0].id
        constraint.op = "=="
        constraint.constant = 0

        constraint.constraintgroup = group_1

        test_constraint = self.client.service.add_constraint(net.scenarios[0][0].id, constraint)
        return test_constraint

def run():
    unittest.main()

if __name__ == '__main__':
    run()  # all tests
