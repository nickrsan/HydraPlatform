from HydraLib import config
from spyne.model.complex import Array as SpyneArray
from spyne.model.primitive import String, Integer
from hydra_complexmodels import Plugin
from spyne.decorator import rpc
from hydra_base import HydraService

import os
import subprocess 
import sys

import logging

from lxml import etree

class PluginService(HydraService):
    """
        Plugin SOAP service
    """

    @rpc(_returns=SpyneArray(String))
    def get_plugins(ctx):
        """
            Get all available plugins
        """
        
        plugins = []
        plugin_paths = []
        
        #Look in directory or set of directories for
        #plugins
        
        base_plugin_dir = config.get('plugin', 'default_directory')
        plugin_xsd_path = config.get('plugin', 'plugin_xsd_path')

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
        
        #Retrieve the xml schema for validating the XML to make sure
        #what is being provided  to the IU is correct.
        xmlschema_doc = etree.parse(plugin_xsd_path)
                        
        xmlschema = etree.XMLSchema(xmlschema_doc)
        
        #Get the xml description file from the plugin directory. If there
        #is no xml file, the plugin in unusable.
        for plugin_dir in plugin_paths:
            full_plugin_path = os.path.join(plugin_dir, 'trunk')
            
            dir_contents = os.listdir(full_plugin_path)
            #look for a plugin.xml file in the plugin directory 
            for file_name in dir_contents:
                file_path = os.path.join(full_plugin_path, file_name)
                if file_name == 'plugin.xml':
                    
                    f = open(file_path, 'r')
                    
                    #validate the xml using the xml schema for defining
                    #plugin details
                    try:
                        y = open(file_path, 'r')
                        
                        xml_tree = etree.parse(y)

                        xmlschema.assertValid(xml_tree)
                       
                        plugins.append(etree.tostring(xml_tree))
                    except Exception, e:
                        logging.critical("Schema %s did not validate! (error was %s)"%(file_name, e))

                    break
            else:
                logging.warning("No xml plugin details found for %s. Ignoring", plugin_dir)

        return plugins

    @rpc(Plugin, _returns=String)
    def run_plugin(ctx, plugin):
        """
            Run a plugin
        """
       
        args = [sys.executable]

        #Get plugin executable
        home = os.path.expanduser('~')
        path_to_plugin = os.path.join(home, 'svn/HYDRA/HydraPlugins', plugin.location)
        args.append(path_to_plugin)
        
        #Parse plugin arguments into a string
        plugin_params = " "
        for p in plugin.params:
            param = "--%s=%s "%(p.name, p.value)
            args.append("--%s"%p.name)
            args.append(p.value)
            plugin_params = plugin_params + param

        log_dir = config.get('plugin', 'result_file')
        log_file = os.path.join(home, log_dir, plugin.name)

        #this reads all the logs so far. We're not interested in them
        #Everything after this is new content to the file
        try:
            f = open(log_file, 'r')
            f.read()
        except:
            f = open(log_file, 'w')
            f.close()
            f = open(log_file, 'r')

        pid = subprocess.Popen(args).pid
        #run plugin
        #os.system("%s %s"%(path_to_plugin, plugin_params))
        
        logging.info("Process started! PID: %s", pid)

        return str(pid)

       
        #monitor plugin

        #when plugin is done, read its output from 
        #the output file and return to the client


    @rpc(String, Integer, _returns=String)
    def check_plugin_status(ctx, plugin_name, pid):
        home = os.path.expanduser('~')
        log_dir = config.get('plugin', 'result_file')
        log_file = os.path.join(home, log_dir, plugin_name)
        try:
            f = open(log_file, 'r')
            file_text = f.read()
            pid_index = file_text.find("%%%s%%"%(pid))

            if pid_index < 0:
                return "No log found for PID %s in %s"%(pid, plugin_name)

            split_string = file_text.split("%%%s%%"%(pid))

            return split_string[1]

        except IOError, e:
            return "No log file found for %s in plugin %s Error was: %s"%(pid, plugin_name, e)

