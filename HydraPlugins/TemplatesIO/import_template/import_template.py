#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A Hydra app for importing a template file to Hydra.

Basics
~~~~~~

A Hydra app for running the jordan model
Basic usage::

       import_template.py [-h] -t template

Options
~~~~~~~

====================== ====== ============ =======================================
Option                 Short  Parameter    Description
====================== ====== ============ =======================================
``--help``             ``-h``              Show help message and exit.
``--template''         ``-t'' TEMPLATE     The template file
``--server-url``       ``-u`` SERVER-URL   Url of the server the plugin will
                                           connect to.
                                           Defaults to localhost.
``--session-id``       ``-c`` SESSION-ID   Session ID used by the calling software.
                                           If left empty, the plugin will attempt
                                           to log in itself.
====================== ====== ============ =======================================

"""


import argparse as ap
import logging

from HydraLib.HydraException import HydraPluginError
from HydraLib.PluginLib import JsonConnection,\
                               create_xml_response,\
                               write_progress,\
                               write_output,\
                               validate_plugin_xml
import os, sys

log = logging.getLogger(__name__)

global __location__
__location__ = os.path.split(sys.argv[0])[0]

class TemplateImporter(object):
    """
       Import a template file into Hydra
    """

    def __init__(self, url=None, session_id=None):

        self.connection = JsonConnection(url)
        write_output("Connecting...")
        if session_id is not None:
            write_output("Using existing session %s"% session_id)
            self.connection.session_id=session_id
        else:
            self.connection.login()

        self.num_steps = 3

    def import_template(self, template):

        if template is not None:
            template_file = open(template).readlines()
            template_text = "".join(x for x in template_file)

            template = self.connection.call('upload_template_xml', 
                                            {'template_xml':template_text})
        else:
            raise HydraPluginError("No template file specified!")

        return template

def commandline_parser():
    parser = ap.ArgumentParser(
        description="""Import a template to
                    Written by Stephen Knox <stephen.knox@manchester.ac.uk>
                    (c) Copyright 2015, University of Manchester.
        """, epilog="For more information visit www.hydraplatform.org")
    parser.add_argument('-t', '--template',
                        help='''The template file''')
    parser.add_argument('-u', '--server-url',
                        help='''Specify the URL of the server to which this
                        plug-in connects.''')
    parser.add_argument('-c', '--session-id',
                        help='''Session ID. If this does not exist, a login will be
                        attempted based on details in config.''')
    return parser


if __name__ == '__main__':
    parser = commandline_parser()
    args = parser.parse_args()
    template_importer = TemplateImporter(url=args.server_url, session_id=args.session_id)
    errors = []
    try:
        write_output("Starting Import")
        write_progress(1, template_importer.num_steps) 

        validate_plugin_xml(os.path.join(__location__, 'plugin.xml'))
        
        template = template_importer.import_template(args.template)
        message = "Import Complete for template %s. ID is %s" %(template.name, template.id)
    except HydraPluginError as e:
        message="An error has occurred"
        errors = [e.message]
        log.exception(e)
    except Exception, e:
        message="An error has occurred"
        log.exception(e)
        errors = [e]

    xml_response = create_xml_response('Import Template',
                                                 None,
                                                 [],
                                                 errors,
                                                 [],
                                                 message,
                                                 [args.template])
    print xml_response
