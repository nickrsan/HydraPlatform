
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
from lxml import etree

class PluginsTest(test_SoapServer.SoapServerTest):

    def test_get_plugins(self):
        plugins = self.client.service.get_plugins()

        assert len(plugins[0]) > 0, "Plugins not retrieved correctly."

    def test_run_plugin(self):
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
