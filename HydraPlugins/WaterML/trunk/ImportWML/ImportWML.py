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

       ImportWML.py [-h] [-f Timeseries file]

Options
~~~~~~~

========== ====== ========== ==============================================
Option     Short  Parameter  Description
========== ====== ========== ==============================================
``--help`` ``-h``            show help message and exit.
``--file`` ``-t`` Timeseries File  XML file containing a WaterML timeseries
========== ====== ========== ==============================================


File structure
~~~~~~~~~~~~~~

Building a windows executable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 - Use pyinstaller (pip install pyisntaller) to build a windows executable.
 - cd to the $PATH_TO_HYDRA/HydraPlugins/WaterMLPlugin/trunk
 - pyinstaller -F ImportWML.py
 or if you want more compression (a smaller exe), install upx
 - pyinstaller -F --upx-dir=/path/to/upx/dir ExportWML.py
 - An executable file will appear in the dist folder

API docs
~~~~~~~~
"""

import argparse as ap
import logging
import os, sys
from datetime import datetime
import pytz

from HydraLib import PluginLib
from HydraLib.PluginLib import write_progress, write_output, validate_plugin_xml
from HydraLib import config, util

from HydraLib.HydraException import HydraPluginError,HydraError

from suds import WebFault
from numpy import array, reshape

from owslib.waterml.wml11 import WaterML_1_1 as wml

from lxml import etree
import requests

import json
log = logging.getLogger(__name__)

__location__ = os.path.split(sys.argv[0])[0]

class ImportWML(object):
    """
    """

    def __init__(self, url=None, session_id=None):

        self.url = url

        self.timezone = pytz.utc
        self.basepath = ''

        self.session_id=None
        if session_id is not None:
            log.info("Using existing session %s", session_id)
            self.session_id=session_id
        else:
            self.login()

        self.warnings = []
        self.message = ''
        self.files = []

        self.num_steps = 6

    def read_timeseries_data(self, target):
        """
            Read Water ML timeseries data.
            @target
                Can be either a directory containing multiple timeseries
                files or a single timeseries file.
        """
        self.read_timeseries_file(target)

    def read_timeseries_file(self, file):
        """
            Taking a wml file as an argument,
            return an array where each element is a line in the wml.
        """

        nodes = self.call('get_all_node_data', {'network_id':2,'scenario_id': 2})
        node_ids = []
        for ra in nodes:
            if ra['ref_id'] not in node_ids:
                node_ids.append(ra['ref_id'])
        nodes = self.call('get_all_node_data', {'network_id':2,'scenario_id': 2,
                                                'node_ids':node_ids})
      #  links = self.call('get_all_link_data', {'network_id':2,'scenario_id': 2})
      #  link_ids = []
      #  for ra in links:
      #      if ra['ref_id'] not in link_ids:
      #          link_ids.append(ra['ref_id'])
      #  links = self.call('get_all_link_data', {'network_id':2,'scenario_id': 2,
      #                                          'link_ids':link_ids})

       # nodes = self.call('get_all_node_data', {'network_id':3,'scenario_id': 3})
       # links = self.call('get_all_link_data', {'network_id':3,'scenario_id': 3})

        timeseries_xml_data=None
        if file == None:
            log.warn("No file specified")
            return None
        self.basepath = os.path.dirname(os.path.realpath(file))
        with open(file, mode='r') as timeseries_file:
            timeseries_xml_data = timeseries_file.read()
            try:
                resp = wml(timeseries_xml_data).response
            except:
                raise HydraPluginError("Invalid WaterML XML content.")

        query_info = dict(resp.query_info.criteria.parameters)
        start = query_info['startDate']
        end   = query_info['endDate']
        site  = query_info['site']
         
        all_timeseries = resp.time_series
        
        datasets = []
        for wml_timeseries in all_timeseries:
            datasets.append(self.create_timeseries(wml_timeseries))
            datasets.append(self.create_timeseries_meta(wml_timeseries))

        new_dataset_ids = self.call('bulk_insert_data', {'bulk_data':datasets})

        data_import_name = "%s between %s and %s"%(site, start, end)

        self.create_dataset_group(data_import_name, new_dataset_ids)

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
                    'app_name' : 'Import WML'
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

    def create_dataset_group(self, name, dataset_ids):

        new_group = {'group_name': name,
                        'dataset_ids' : dataset_ids}

        self.call('add_dataset_group', {'group':new_group})

    def create_timeseries(self, wml_timeseries):

        meta = self.get_metadata(wml_timeseries)
        
        values   = wml_timeseries.values[0]

        timesteps = values.get_date_values(utc=True)

        timeseries_val = []
        for timestep in timesteps:
            timestep_val = {'ts_time' : str(timestep[0]), 'ts_value' : timestep[1]}
            timeseries_val.append(timestep_val)


        metadata_list = []
        if meta:
            for k, v in meta.items():
                metadata_list.append(dict(name=k,value=v))

        dataset = dict(
            id=None,
            type='timeseries',
            unit=meta.get('unit_abbreviation'),
            dimension=meta.get('unit_unit_type'),
            name="%s %s %s"%(meta['data_type'],meta['variable_name'], meta['site_codes']),
            value={'ts_values':timeseries_val},
            locked='N',
            metadata=metadata_list,
        )

        return dataset

    def create_timeseries_meta(self, wml_timeseries):

        meta = self.get_metadata(wml_timeseries)
        
        values   = wml_timeseries.values[0]

        #Metadata about the metadata. Very Meta.
        meta_meta = {}
        for cc in values.censor_codes:
            meta_meta['censor_code_%s'%cc.id] = "%s:%s"%(cc.code, cc.description) 

        for method in values.methods:
            meta_meta['method_%s'%method.id] = "%s:%s(%s)"%(method.code,
                                                       method.description,
                                                       method.link)
        for s in values.sources:
            meta_meta['source_%s'%s.code] = str(s)

        for qc in values.qualit_control_levels:
            meta_meta['quality_control_%s'%qc.code] = "%s:%s"%(qc.definition,
                                                         qc.explanation)

        timestep_meta = []
        for wml_val in values:
            timestep_meta.append({'ts_time' : str(wml_val.date_time_utc),
                                  'ts_value' : [['test', ['testa', 'testb']],
                                                ['test1', ['test1a', 'test1b']],
                                                ['7', [wml_val.method_code, wml_val.source_code]]]})
        meta_dataset = dict(
            id=None,
            type='timeseries',
            unit=None,
            dimension=None,
            name="%s %s %s metadata"%(meta['data_type'],meta['variable_name'], meta['site_codes']),
            value={'ts_values':timestep_meta},
            locked='N',
            metadata=[],
        )
        logging.warn(meta_dataset)

        return meta_dataset

    def get_metadata(self, wml_timeseries):
        meta = {}
       
        blacklist = ('_ns', '_root', 'notes')

        def todict(waterml_obj, prefix=None):
            obj_as_dict = dict()
            for k, v in waterml_obj.__dict__.items():
                if not v:
                    continue
                if k in blacklist or k.startswith('_'):
                    continue
                if str(type(v)).find('waterml') > 0:
                    obj_as_dict.update(todict(v, "%s_"%k if prefix is None else "%s%s_"%(prefix,k)))
                else:
                    if type(v) is list:
                        if type(v[0]) == str:
                            mval = ",".join(v)
                        else:
                            mval = ",".join(v[0])
                    elif type(v) is dict:
                        mval = str(v)
                    else:
                        mval = v
                    obj_as_dict["%s%s"%('' if prefix is None else prefix, k)] = mval
            return obj_as_dict

        src_info = wml_timeseries.source_info
        src_dict = todict(src_info)
        meta.update(src_dict)

        variable = wml_timeseries.variable
        variable_dict = todict(variable)
        meta.update(variable_dict)

        return meta

def commandline_parser():
    parser = ap.ArgumentParser(
        description="""Import a single WML or directory of WML timeseries files into hydra as datsets into a new datset group.

Written by Philipp Meier <philipp@diemeiers.ch>
(c) Copyright 2013, University College London.

        """, epilog="For more information visit www.hydra-network.com",
        formatter_class=ap.RawDescriptionHelpFormatter)
    parser.add_argument('-t', '--timeseriesfile',
                        help='''The XML file containing a WaterML timeseries.''')
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
    importwml = ImportWML(url=args.server_url, session_id=args.session_id)

    try:
        
        validate_plugin_xml(os.path.join(__location__, 'plugin.xml'))

        importwml.read_timeseries_data(args.timeseriesfile)
        write_output("Saving data")
        importwml.message = 'Data import was successful.'
	errors = []
    except HydraPluginError as e:
        errors = [e.message]

    xml_response = PluginLib.create_xml_response('ImportWML',
                                                 None,
                                                 [],
                                                 errors,
                                                 importwml.warnings,
                                                 importwml.message,
                                                 importwml.files)

    print xml_response
