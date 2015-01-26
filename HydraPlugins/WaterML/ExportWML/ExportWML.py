#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A Hydra plug-in for importing WML timeseries files.

Basics
~~~~~~
This app allows one or multiple WaterML timeseries files to be imported into Hydra.

The app assumes that you have downloaded timeseries files
from the cuahsi web service. A single file can be passed to the app or a directory.
If a directory is passed, the app will find the appropriate timeseries files and import
all the data it can. 

Basic usage::

       ExportWML.py [-h] [-g Group ID] [-d Dataset ID] [-n Filename]

Options
~~~~~~~

================ ====== =========== ==============================================
Option           Short  Parameter   Description
================ ====== =========== ==============================================
``--help``       ``-h``             show help message and exit.
``--group``      ``-g`` Group ID    ID of the dataset group from which to retrieve 
                                    the datasets. (Not required if dataset ID 
                                    is specified. Dataset ID takes precedence 
                                    if both are specified.)'
``--dataset``    ``-d`` Dataset ID  ID of the dataset to be exported. 
                                    (Not required if groupID is specified. 
                                    This takes precedence if both are specified.)
``--filename``   ``-n`` File Name   Name of the export file.
================ ====== =========== ==============================================


File structure
~~~~~~~~~~~~~~

Building a windows executable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 - Use pyinstaller (pip install pyisntaller) to build a windows executable.
 - cd to the $PATH_TO_HYDRA/HydraPlugins/WaterMLPlugin/trunk
 - pyinstaller -F ExportWML.py
 or if you want more compression (a smaller exe), install upx
 - pyinstaller -F --upx-dir=/path/to/upx/dir ExportWML.py
 - An executable file will appear in the dist folder

API docs
~~~~~~~~
"""

import argparse as ap
import logging
import os, sys
import pytz

from HydraLib import PluginLib
from HydraLib.PluginLib import write_progress, write_output, validate_plugin_xml
from HydraLib import config

from HydraLib.HydraException import HydraPluginError

from owslib.waterml.wml11 import WaterML_1_1 as wml

import requests

import json
log = logging.getLogger(__name__)

__location__ = os.path.split(sys.argv[0])[0]

class ExportWML(object):
    """
    """

    def __init__(self, url=None, session_id=None):

        self.url = url

        self.timezone = pytz.utc
        self.basepath = ''

        self.session_id=None

        self.warnings = []
        self.message = ''
        self.files = []

    def make_timestep(self, time, value, metadata, metadata_ts):
        pass

    def make_wml_timeseries(self, dataset):
        

        xml_timesteps = []
        ts = dataset['ts_values']
        for val in ts:
            tstep = self.make_timestep(ts['ts_time'], ts['ts_val'], dataset['metadata'], None)
            xml_timesteps.append(tstep)


    def get_datasets(self,  dataset_id, dataset_group_id):
        datasets = []
        if dataset_id is not None:
            """
                Check for datasets first.
            """
            try:
                dataset = self.call('get_dataset', {'dataset_id':dataset_id})
            except:
                dataset = None
            if dataset is None:
                """
                    No datasets? OK, maybe user put in a group?
                """
                if dataset_group_id is not None:
                    try:
                        group = self.call('get_dataset_group',{'dataset_group_id':dataset_group_id})
                    except:
                        group = None

                    if group is None:
                        raise HydraPluginError("Dataset %s does not exist", dataset_id)
                    else:
                        write_output("Couldn't find dataset. Using group instead.")
            else:
                if dataset['type'] != 'timeseries':
                    raise HydraPluginError("Only timeseries can be exported to WML.")
                datasets.append(dataset)
        else:
            """
                Fall back to group. Get each dataset in turn.
            """
            if dataset_group_id is not None:
                group = self.call('get_dataset_group',{'dataset_group_id':dataset_group_id})
                if group is None:
                    raise HydraPluginError("Dataset group %s does not exist", dataset_id)
                for ds_id in group['dataset_ids']:
                    dataset = self.call('get_dataset', {'dataset_id':ds_id})
                    if datset['type'] != 'timeseries':
                        continue
                    datasets.append(dataset)

        if len(datasets) == 0:
            raise HydraPluginError("Could not find any datasets to export. Please check IDS.")
        return datasets


    def login(self):
        user = config.get('hydra_client', 'user')
        passwd = config.get('hydra_client', 'password')
        login_params = {'username':user, 'password':passwd}

        resp = self.call('login', login_params)
        #set variables for use in request headers
        self.session_id = resp['session_id']
        log.info("Session ID=%s", self.session_id)

    def call(self, func, args):
        log.info("Calling: %s"%(func))
        if self.url is None:
            port = config.getint('hydra_server', 'port', 8080)
            domain = config.get('hydra_server', 'domain', '127.0.0.1')
            self.url = "http://%s:%s/json"%(domain, port)
            log.info("Setting URL %s", self.url)
        call = {func:args}
        headers = {
                    'Content-Type': 'application/json',       
                    'session_id':self.session_id,
                    'app_name' : 'Export WML'
                  }
        r = requests.post(self.url, data=json.dumps(call), headers=headers)
        if not r.ok:
            try:
                resp = json.loads(r.content)
                err = "%s:%s"%(resp['faultcode'], resp['faultstring'])
            except:
                err = r.content
            raise HydraPluginError(err)
        return json.loads(r.content) 

def commandline_parser():
    parser = ap.ArgumentParser(
        description="""Export a single dataset or group of datasets into a Water ML file.

        Written by Stephen Knox <stephen.knox@manchester.ac.uk>
        (c) Copyright 2013, University of Manchester.

        """, epilog="For more information visit www.hydra-network.com",
        formatter_class=ap.RawDescriptionHelpFormatter)
    parser.add_argument('-g', '--group',
                        help='''The ID of the dataset group to be exported. (Not required if dataset ID is specified. Dataset ID takes precedence if both are specified.)''')
    parser.add_argument('-d', '--dataset',
                        help='''The ID of the dataset to be exported. (Not required if groupID is specified. This takes precedence if both are specified.)''')
    parser.add_argument('-n', '--filename',
                        help='''The name of the output file.''')
    parser.add_argument('-u', '--server_url',
                        help='''Specify the URL of the server to which this
                        plug-in connects.''')
    parser.add_argument('-c', '--session_id',
                        help='''Session ID. If this does not exist, a login will be
                        attempted based on details in config.''')
    return parser


if __name__ == '__main__':
    parser = commandline_parser()
    args = parser.parse_args()
    importwml = ExportWML(url=args.server_url, session_id=args.session_id)

    try:
        if args.session_id is not None:
            log.info("Using existing session %s", args.session_id)
            importwml.session_id=args.session_id
        else:
            importwml.login()

        validate_plugin_xml(os.path.join(__location__, 'plugin.xml'))

        data_import_name = importwml.read_timeseries_data(args.timeseriesfile, args.uploadname)
        write_output("Saving data")
        importwml.message = 'Data import was successful. Timeseries imported into group named "%s"'%data_import_name

        errors = []
    except HydraPluginError as e:
        errors = [e.message]
    except requests.exceptions.ConnectionError:
        errors = ["Could not connect to server"]

    xml_response = PluginLib.create_xml_response('ExportWML',
                                                 None,
                                                 [],
                                                 errors,
                                                 importwml.warnings,
                                                 importwml.message,
                                                 importwml.files)

    print xml_response
