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

import test_SoapServer
from lxml import etree

class PluginsTest(test_SoapServer.SoapServerTest):

    def get_plugins(self):
        plugins = self.client.service.get_plugins()

        assert len(plugins[0]) > 0, "Plugins not retrieved correctly."

    def run_plugin(self):
        plugins = self.client.service.get_plugins()


        plugin_etrees = []
        for plugin in plugins[0]:
            plugin_etrees.append(etree.XML(plugin))

        for ptree in plugin_etrees:
            plugin_name = ptree.find('plugin_name').text
            print plugin_name
            if plugin_name == 'Test Plugin':
                #call the plugin
                plugin = self.client.factory.create('ns1:Plugin')

                plugin.name = plugin_name
                plugin.location = ptree.find('plugin_dir').text

                mandatory_args = ptree.find('mandatory_args')
                args = mandatory_args.findall('arg')

                for arg in args:
                    plugin_param = self.client.factory.create('ns1:PluginParam')
                    plugin_param.name = arg.find('name').text
                    plugin_param.value = 1
                    plugin.params.PluginParam.append(plugin_param)

                PID = self.client.service.run_plugin(plugin)
                print PID

                #Give the plugin a chance to execute.
                import time
                time.sleep(1)

                result = self.client.service.check_plugin_status(plugin_name, PID)

                print result

                break
        else:
            self.fail("Test plugin not found!")

if __name__ == '__main__':
    test_SoapServer.run()
