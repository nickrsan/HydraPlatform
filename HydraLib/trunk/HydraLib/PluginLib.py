# (c) Copyright 2013, 2014, University of Manchester
#
# HydraPlatform is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# HydraPlatform is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with HydraPlatform.  If not, see <http://www.gnu.org/licenses/>
#
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import config
import hydra_logging
import logging
from suds.client import Client
from suds.plugin import MessagePlugin

from datetime import datetime
import os


class FixNamespace(MessagePlugin):
    """Hopefully a temporary fix for an unresolved namespace issue.
    """
    def marshalled(self, context):
        self.fix_ns(context.envelope)

    def fix_ns(self, element):
        if element.prefix == 'xs':
            element.prefix = 'ns0'

        for e in element.getChildren():
            self.fix_ns(e)


class HydraResource(object):
    """A prototype for Hydra resources. It supports attributes and groups
    object types by template. This allows to export group nodes by object
    type based on the template used.
    """
    def __init__(self):
        self.name = None
        self.ID = None
        self.attributes = []
        self.groups = []
        self.template = dict()
        self.template[None] = []

    def add_attribute(self, attr, res_attr, res_scen):
        attribute = HydraAttribute(attr, res_attr, res_scen)

        self.attributes.append(attribute)

    def delete_attribute(self, attribute):
        idx = self.attributes.index(attribute)
        del self.attributes[idx]

    def get_attribute(self, attr_name=None, attr_id=None):

        if attr_name is not None:
            return self._get_attr_by_name(attr_name)
        elif attr_id is not None:
            return self._get_attr_by_id(attr_id)

    def set_type(self, types):
        for obj_type in types.TypeSummary:
            # Add resource type to template dictionary
            if obj_type.template_id not in self.template.keys():
                self.template[obj_type.template_id] = []
            self.template[obj_type.template_id].append(obj_type.name)
            # Add resource type to default entry holding all resource types
            if obj_type.name not in self.template[None]:
                self.template[None].append(obj_type.name)

    def group(self, group_attr):
        attr = self._get_attr_by_name(group_attr)
        if attr is not None:
            group = attr.value.__getitem__(0)
            self.groups.append(group)
            # The attribute is used for grouping and will not be exported
            self.delete_attribute(attr)
            return group

    def _get_attr_by_name(self, attr_name):
        for attr in self.attributes:
            if attr.name == attr_name:
                return attr

    def _get_attr_by_id(self, attr_id):
        for attr in self.attributes:
            if attr.attr_id == attr_id:
                return attr


class HydraNetwork(HydraResource):
    """
    """

    description = None
    scenario_id = None
    nodes = []
    links = []
    node_groups = []
    link_groups = []

    def load(self, soap_net, soap_attrs):

        # load network
        resource_scenarios = dict()
        for res_scen in \
                soap_net.scenarios.Scenario[0].resourcescenarios.ResourceScenario:
            resource_scenarios.update({res_scen.resource_attr_id: res_scen})
        attributes = dict()
        for attr in soap_attrs.Attr:
            attributes.update({attr.id: attr})

        self.name = soap_net.name
        self.ID = soap_net.id
        self.description = soap_net.description
        self.scenario_id = soap_net.scenarios.Scenario[0].id
        self.set_type(soap_net.types)

        if soap_net.attributes is not None:
            for res_attr in soap_net.attributes.Attr:
                if res_attr.id in resource_scenarios.keys():
                    self.add_attribute(attributes[res_attr.attr_id],
                                    res_attr,
                                    resource_scenarios[res_attr.id])
                else:
                    self.add_attribute(attributes[res_attr.attr_id],
                                    res_attr,
                                    None)

        # load nodes
        for node in soap_net.nodes.Node:
            new_node = HydraResource()
            new_node.ID = node.id
            new_node.name = node.name
            if node.attributes is not None:
                for res_attr in node.attributes.ResourceAttr:
                    if res_attr.id in resource_scenarios.keys():
                        new_node.add_attribute(attributes[res_attr.attr_id],
                                               res_attr,
                                               resource_scenarios[res_attr.id])
                    else:
                        new_node.add_attribute(attributes[res_attr.attr_id],
                                               res_attr,
                                               None)

            new_node.set_type(node.types)
            self.add_node(new_node)
            del new_node

        # load links
        for link in soap_net.links.Link:
            new_link = HydraResource()
            new_link.ID = link.id
            new_link.name = link.name
            new_link.from_node = self.get_node(node_id=link.node_1_id).name
            new_link.to_node = self.get_node(node_id=link.node_2_id).name
            if link.attributes is not None:
                for res_attr in link.attributes.ResourceAttr:
                    if res_attr.id in resource_scenarios.keys():
                        new_link.add_attribute(attributes[res_attr.attr_id],
                                            res_attr,
                                            resource_scenarios[res_attr.id])
                    else:
                        new_link.add_attribute(attributes[res_attr.attr_id],
                                               res_attr,
                                               None)
            new_link.set_type(link.types)

            self.add_link(new_link)
            del new_link

    def add_node(self, node):
        self.nodes.append(node)

    def delete_node(self, node):
        pass

    def get_node(self, node_name=None, node_id=None, node_type=None, group=None):
        if node_name is not None:
            return self._get_node_by_name(node_name)
        elif node_id is not None:
            return self._get_node_by_id(node_id)
        elif node_type is not None:
            return self._get_nodes_by_type(node_type)
        elif group is not None:
            return self._get_nodes_by_group(group)

    def add_link(self, link):
        self.links.append(link)

    def delete_link(self, link):
        pass

    def get_link(self, link_name=None, link_id=None, link_type=None, group=None):
        if link_name is not None:
            return self._get_link_by_name(link_name)
        elif link_id is not None:
            return self._get_link_by_id(link_id)
        elif link_type is not None:
            return self._get_links_by_type(link_type)
        elif group is not None:
            return self._get_links_by_group(group)

    def get_node_types(self, template_id=None):
        node_types = []
        for node in self.nodes:
            for n_type in node.template[template_id]:
                if n_type not in node_types:
                    node_types.append(n_type)
        return node_types

    def get_link_types(self, template_id=None):
        link_types = []
        for link in self.links:
            for l_type in link.template[template_id]:
                if l_type not in link_types:
                    link_types.append(l_type)
        return link_types

    def create_node_groups(self, group_attr):
        for i, node in enumerate(self.nodes):
            group = self.nodes[i].group(group_attr)
            if group is not None and group not in self.node_groups:
                self.node_groups.append(group)

    def create_link_groups(self, group_attr):
        for i, link in enumerate(self.links):
            group = self.links[i].group(group_attr)
            if group is not None and group not in self.link_groups:
                self.link_groups.append(group)

    def _get_node_by_name(self, name):
        for node in self.nodes:
            if node.name == name:
                return node

    def _get_node_by_id(self, ID):
        for node in self.nodes:
            if node.ID == ID:
                return node

    def _get_nodes_by_type(self, node_type):
        nodes = []
        for node in self.nodes:
            if node_type in node.template[None]:
                nodes.append(node)
        return nodes

    def _get_nodes_by_group(self, node_group):
        nodes = []
        for node in self.nodes:
            if node_group not in node.groups:
                nodes.append(node)
        return nodes

    def _get_link_by_name(self, name):
        for link in self.links:
            if link.name == name:
                return link

    def _get_link_by_id(self, ID):
        for link in self.links:
            if link.ID == ID:
                return link

    def _get_links_by_type(self, link_type):
        links = []
        for link in self.links:
            if link_type in link.template[None]:
                links.append(link)
        return links

    def _get_links_by_group(self, link_group):
        links = []
        for link in self.links:
            if link_group not in link.groups:
                links.append(link)
        return links


class HydraAttribute(object):

    name = None

    attr_id = None
    resource_attr_id = None
    is_var = False

    dataset_id = None
    dataset_type = ''

    value = None

    def __init__(self, attr, res_attr, res_scen):
        self.name = attr.name
        self.attr_id = attr.id
        self.resource_attr_id = res_attr.id
        if res_attr.attr_is_var == 'Y':
            self.is_var = True
        if res_scen is not None:
            self.dataset_id = res_scen.value.id
            self.dataset_type = res_scen.value.type
            self.value = res_scen.value.value


def connect():
    hydra_logging.init(level='INFO')
    logging.getLogger('suds').setLevel(logging.ERROR)
    logging.getLogger('suds.client').setLevel(logging.CRITICAL)
    #logging.getLogger('suds.metrics').setLevel(logging.INFO)
    url = config.get('hydra_client', 'url')
    user = config.get('hydra_client', 'user')
    passwd = config.get('hydra_client', 'password')
    cli = Client(url, timeout=3600, plugins=[FixNamespace()])
    login_response = cli.service.login(user, passwd)
    user_id = login_response.user_id
    session_id = login_response.session_id
    token = cli.factory.create('RequestHeader')
    token.session_id = session_id
    token.username = user
    token.user_id = user_id
    cli.set_options(soapheaders=token)
    cli.add_prefix('hyd', 'soap_server.hydra_complexmodels')

    return cli


def temp_ids(n=-1):
    """
    Create an iterator for temporary IDs for nodes, links and other entities
    that need them. You need to initialise the temporary id first and call the
    next element using the ``.next()`` function::

        temp_node_id = PluginLib.temp_ids()

        # Create a node
        # ...

        Node.id = temp_node_id.next()
    """
    while True:
        yield n
        n -= 1


def date_to_string(date, seasonal=False):
    """Convert a date to a standard string used by Hydra. The resulting string
    looks like this::

        '2013-10-03 00:49:17.568-0400'

    Hydra also accepts seasonal time series (yearly recurring). If the flag
    ``seasonal`` is set to ``True``, this function will generate a string
    recognised by Hydra as seasonal time stamp.
    """
    if seasonal:
        FORMAT = 'XXXX-%m-%d %H:%M:%S.%f%z'
    else:
        FORMAT = '%Y-%m-%d %H:%M:%S.%f%z'
    return date.strftime(FORMAT)


def guess_timefmt(datestr):
    """
    Try to guess the format a date is written in.

    The following formats are supported:

    ================= ============== ===============
    Format            Example        Python format
    ----------------- -------------- ---------------
    ``YYYY-MM-DD``    2002-04-21     %Y-%m-%d
    ``YYYY.MM.DD``    2002.04.21     %Y.%m.%d
    ``YYYY MM DD``    2002 04 21     %Y %m %d
    ``DD-MM-YYYY``    21-04-2002     %d-%m-%Y
    ``DD.MM.YYYY``    21.04.2002     %d.%m.%Y
    ``DD MM YYYY``    21 04 2002     %d %m %Y
    ``MM/DD/YYYY``    04/21/2002     %m/%d/%Y
    ================= ============== ===============

    These formats can also be used for seasonal (yearly recurring) time series.
    The year needs to be replaced by ``XXXX``.

    The following formats are recognised depending on your locale setting.
    There is no guarantee that this will work.

    ================= ============== ===============
    Format            Example        Python format
    ----------------- -------------- ---------------
    ``DD-mmm-YYYY``   21-Apr-2002    %d-%b-%Y
    ``DD.mmm.YYYY``   21.Apr.2002    %d.%b.%Y
    ``DD mmm YYYY``   21 Apr 2002    %d %b %Y
    ``mmm DD YYYY``   Apr 21 2002    %b %d %Y
    ``Mmmmm DD YYYY`` April 21 2002  %B %d %Y
    ================= ============== ===============

    .. note::
        - The time needs to follow this definition without exception:
            `%H:%M:%S.%f`. A complete date and time should therefore look like
            this::

                2002-04-21 15:29:37.522

        - Be aware that in a file with comma separated values you should not
          use a date format that contains commas.
    """

    delimiters = ['-', '.', ' ']
    formatstrings = [['%Y', '%m', '%d'],
                     ['%d', '%m', '%Y'],
                     ['%d', '%b', '%Y'],
                     ['XXXX', '%m', '%d'],
                     ['%d', '%m', 'XXXX'],
                     ['%d', '%b', 'XXXX']]

    timeformats = ['%H:%M:%S.%f', '%H:%M:%S', '%H:%M']

    # Check if a time is indicated or not
    for timefmt in timeformats:
        try:
            datetime.strptime(datestr.split(' ')[-1].strip(), timefmt)
            usetime = True
        except ValueError:
            usetime = False

    # Check the simple ones:
    for fmt in formatstrings:
        for delim in delimiters:
            datefmt = fmt[0] + delim + fmt[1] + delim + fmt[2]
            if usetime:
                for timefmt in timeformats:
                    complfmt = datefmt + ' ' + timefmt
                    try:
                        datetime.strptime(datestr, complfmt)
                        return complfmt
                    except ValueError:
                        pass
            else:
                try:
                    datetime.strptime(datestr, datefmt)
                    return datefmt
                except ValueError:
                    pass

    # Check for other formats:
    custom_formats = ['%m/%d/%Y', '%b %d %Y', '%B %d %Y', '%m/%d/XXXX']

    for fmt in custom_formats:
        if usetime:
            for timefmt in timeformats:
                complfmt = fmt + ' ' + timefmt
                try:
                    datetime.strptime(datestr, complfmt)
                    return complfmt
                except ValueError:
                    pass

        else:
            try:
                datetime.strptime(datestr, fmt)
                return fmt
            except ValueError:
                pass

    return None


def create_xml_response(plugin_name, network_id, scenaro_ids,
                        errors=[], warnings=[], message=None, files=[]):
    xml_string = """<plugin_result>
    <message>%(message)s</message>
    <plugin_name>%(plugin_name)s</plugin_name>
    <network_id>%(network_id)s</network_id>
    %(scenario_list)s
    <errors>
        %(error_list)s
    </errors>
    <warnings>
        %(warning_list)s
    </warnings>
    <files>
        %(file_list)s
    </files>
</plugin_result>"""

    scenario_string = "<scenario>%s</scenario>"
    error_string = "<error>%s</error>"
    warning_string = "<warning>%s</warning>"
    file_string = "<file>%s<file>"

    xml_string = xml_string % dict(
        plugin_name  = plugin_name,
        network_id   = network_id,
        scenario_list = "\n".join([scenario_string % scen_id
                                   for scen_id in scenaro_ids]),
        message      = message if message is not None else "",
        error_list   = "\n".join([error_string%error for error in errors]),
        warning_list = "\n".join([warning_string%warning for warning in warnings]),
        file_list = "\n".join([file_string % f for f in files]),
    )

    return xml_string


def write_xml_result(plugin_name, xml_string, file_path=None):
    if file_path is None:
        file_path = config.get('plugin', 'result_file')

    home = os.path.expanduser('~')

    output_file = os.path.join(home, file_path, plugin_name)

    f = open(output_file, 'a')

    output_string = "%%%s%%%s%%%s%%" % (os.getpid(), xml_string, os.getpid())

    f.write(output_string)

    f.close()


def set_resource_types(client, xml_template, network,
                       nodetype_dict, linktype_dict, networktype):
    logging.info("Setting resource types")

    template = client.service.upload_template_xml(xml_template)

    type_ids = dict()

    for type_name in nodetype_dict.keys():
        for tmpltype in template.types.TemplateType:
            if tmpltype.name == type_name:
                type_ids.update({tmpltype.name: tmpltype.id})
                break

    for type_name in linktype_dict.keys():
        for tmpltype in template.types.TemplateType:
            if tmpltype.name == type_name:
                type_ids.update({tmpltype.name: tmpltype.id})
                break

    for tmpltype in template.types.TemplateType:
        if tmpltype.name == networktype:
            type_ids.update({tmpltype.name: tmpltype.id})
            break

    args = client.factory.create('hyd:ResourceTypeDefArray')
    if type_ids[networktype]:
        args.ResourceTypeDef.append(dict(
            ref_key = 'NETWORK',
            ref_id  = network.id,
            type_id = type_ids[networktype],
        ))

    for node in network.nodes.Node:
        for typename, node_name_list in nodetype_dict.items():
            if type_ids[typename] and node.name in node_name_list:
                args.ResourceTypeDef.append(dict(
                    ref_key = 'NODE',
                    ref_id  = node.id,
                    type_id = type_ids[typename],
                ))

    for link in network.links.Link:
        for typename, link_name_list in linktype_dict.items():
            if type_ids[typename] and link.name in link_name_list:
                args.ResourceTypeDef.append(dict(
                    ref_key = 'LINK',
                    ref_id  = link.id,
                    type_id = type_ids[typename],
                ))

    client.service.assign_types_to_resources(args)

def parse_suds_array(arr):
    """
        Take a list of nested suds any types and return a python list containing
        a single value, a string or sub lists.
    """
    ret_arr = []
    if hasattr(arr, 'array'):
        ret_arr = []
        sub_arr = arr.array
        if type(sub_arr) is list:
            for s in sub_arr:
                ret_arr.append(parse_suds_array(s))
        else:
            return parse_suds_array(sub_arr)
    elif hasattr(arr, 'item'):
        for x in arr.item:
            try:
                val = float(x)
            except:
                val = str(x)
            ret_arr.append(val)
        return ret_arr
    else:
        raise ValueError("Something has gone wrong parsing an array.")
    return ret_arr

def parse_array(arr):
    """
        Take a dictionary and turn it into an array as follows:
        {'array': ['item' : [1, 2, 3]}]} -> [1, 2, 3]
        {'array' :
            {'array': [
                'item' : [1, 2, 3]}
            ]} 
            {'array': [
                'item' : [1, 2, 3]}
            ]}
         ]} -> [[1, 2, 3], [4, 5, 6]]
    """
    if arr.get('array'):
        ret_arr = []
        sub_arr = arr['array']
        if type(sub_arr) is list:
            for s in sub_arr:
                ret_arr.append(parse_array(s))
        else:
            return parse_array(sub_arr)
    elif arr.get('item'):
        return list(arr['item'])
    else:
        raise ValueError("Something has gone wrong parsing an array.")
    return ret_arr

def create_dict(arr_data):
    """
        Take a numpy array and turn it into the correct xml structure
        of array tags and item tags.
    """
    arr = {'array': []}
    if arr_data.ndim == 1:
        arr['array'].append({'item': list(arr_data)})
    
    if arr_data.ndim > 1:
        for a in arr_data:
            if a.ndim == 1:
                arr['array'].append(create_dict(a))
            else:
                val = create_dict(a)
                arr['array'].append(val)
    return arr

