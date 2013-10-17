from HydraLib import util
from hydra_complexmodels import Plugin
from spyne.model.complex import Array as SpyneArray
from spyne.decorator import rpc
from hydra_base import HydraService

import os

import logging

from lxml import etree

class PluginService(HydraService):
    """
        Plugin SOAP service
    """

    @rpc(_returns=SpyneArray(Plugin))
    def get_plugins(ctx):
        """
            Get all available plugins
        """

        config = util.load_config()
        
        plugins = []
        plugin_paths = []
        plugin_details = []
        
        #Look in directory or set of directories for
        #plugins
        
        base_plugin_dir = config.get('plugin', 'default_directory')

        base_plugin_dir_contents = os.listdir(base_plugin_dir)
        for directory in base_plugin_dir_contents:
            #ignore hidden files
            if directory[0] == '.' or directory == 'xml':
                continue

            #Is this a file or a directory? If it's a directory, it's a plugin.
            path = os.path.join(base_plugin_dir, directory)
            if os.path.isdir(path):
                plugin_paths.append(path)
        
        #For each plugin, get its details (an XML string)
        
        #Get the xml description file from the plugin directory. If there
        #is no xml file, the plugin in unusable.
        for plugin_dir in plugin_paths:
            full_plugin_path = os.path.join(plugin_dir, 'trunk')
            
            dir_contents = os.listdir(full_plugin_path)
            
            for file_name in dir_contents:
                file_path = os.path.join(full_plugin_path, file_name)
                if file_name == 'plugin.xsd':
                    f = open(file_path, 'r')
                    schema_text = f.read()
                    try:
                        xmlschema_doc = etree.parse(file_path)
                        xmlschema = etree.XMLSchema(xmlschema_doc)
                        p = Plugin()
                        plugins.append(schema_text)
                    except Exception, e:
                        logging.critical("Schema %s did not validate! (error was %s)"%(file_name, e))

                    break
            else:
                logging.warning("No xml plugin details found for %s. Ignoring", plugin_dir)

        #Return the list of XML strings, detailing each plugin's 
        #information and requirements

        
        return plugins

    @rpc(Plugin, _returns=Plugin)
    def run_plugin(ctx, plugin):
        """
            Run a plugin
        """

    @rpc(Plugin, _returns=Plugin)
    def register_plugin(ctx, plugin):
        """
            Add a plugin to the list of available plugins.
        """
