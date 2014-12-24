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

================ ====== =========== ==============================================
Option           Short  Parameter   Description
================ ====== =========== ==============================================
``--help``       ``-h``             show help message and exit.
``--file``       ``-t`` Timeseries  File  XML file containing a WaterML timeseries
``--uploadname`` ``-n`` Upload Name Name of the dataset grouping
================ ====== =========== ==============================================


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
import pytz

from HydraLib import PluginLib
from HydraLib.PluginLib import JsonConnection, write_progress, write_output, validate_plugin_xml
from HydraLib import config

from HydraLib.HydraException import HydraPluginError

from owslib.waterml.wml11 import WaterML_1_1 as wml

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

        self.connection = JsonConnection(url)
        if session_id is not None:
            log.info("Using existing session %s", session_id)
            self.connection.session_id=session_id
        else:
            self.connection.login()

        self.warnings = []
        self.message = ''
        self.files = []

    def read_timeseries_data(self, targets, dataset_group_name):
        """
            Read Water ML timeseries data.
            @target
                Can be either a directory containing multiple timeseries
                files or a single timeseries file.
        """
        write_progress(1, 2)
        
        all_dataset_ids = []
        for target in targets:
            if os.path.isdir(target):
                for i, f in enumerate(os.listdir(target)):
                    try:
                        write_output("Reading timeseries file %s"%f)
                        data_import_name, file_dataset_ids = self.read_timeseries_file(os.path.join(target, f))
                        all_dataset_ids.extend(file_dataset_ids)
                    except Exception, e:
                        log.critical(e)
                        self.warnings.append("Unable to read timeseries data from file %s"%f)
            
                data_import_name = "timeseries from folder %s"%target

            elif os.path.isfile(target):
                try:
                    write_output("Reading timeseries file %s"%target)
                    data_import_name, file_dataset_ids = self.read_timeseries_file(target)
                    all_dataset_ids.extend(file_dataset_ids)
                except Exception, e:
                    raise HydraPluginError("Unable to read timeseries data from file %s. Invalid content. "
                                       "Ensure the file contains the result of 'GetValues'."%target)
            else:
                raise HydraPluginError("Unable to recognise file %s. Please check inputs"%(target,))

        write_progress(2, 2)
        log.info(len(all_dataset_ids))
        log.info(len(set(all_dataset_ids)))
        if dataset_group_name is not None:
            data_import_name = dataset_group_name
        self.create_dataset_group(data_import_name, list(set(all_dataset_ids)))
        write_output("Dataset %s created"%data_import_name)
        return data_import_name 


    def read_timeseries_file(self, file):
        """
            Taking a wml file as an argument,
            return an array where each element is a line in the wml.
        """

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


        query_info = {} 
        for param, param_val in resp.query_info.criteria.parameters:
            if query_info.get(param):
                if type(query_info[param]) is list:
                    query_info[param].append(param_val)
                else:
                    new_param_vals = [query_info[param], param_val]
                    query_info[param] = new_param_vals
            else:
                query_info[param] = param_val

        start = query_info.get('startDate', 'unknownStart')
        end   = query_info.get('endDate', 'unknownEnd')
        site  = query_info.get('site', 'unknownSite')
        variable  = query_info.get('variable')
         
        all_timeseries = resp.time_series
        
        datasets = []
        for wml_timeseries in all_timeseries:
            datasets.append(self.create_timeseries(wml_timeseries))
            datasets.append(self.create_timeseries_meta(wml_timeseries))

        new_dataset_ids = self.connection.call('bulk_insert_data', {'bulk_data':datasets})
        data_import_name_base = "readings at site %s"
        params = [site]
        if start is not None and end is not None:
            data_import_name_base = data_import_name_base + " between %s and %s"
            params.append(start)
            params.append(end)
        if variable:
            data_import_name_base = data_import_name_base + " with variable %s"
            params.append(variable)

        data_import_name = data_import_name_base%tuple(params)

        return data_import_name, new_dataset_ids

    def login(self):
        user = config.get('hydra_client', 'user')
        passwd = config.get('hydra_client', 'password')
        login_params = {'username':user, 'password':passwd}

        resp = self.connection.call('login', login_params)
        #set variables for use in request headers
        self.session_id = resp['session_id']
        log.info("Session ID=%s", self.session_id)

    def create_dataset_group(self, name, dataset_ids):

        new_group = {'group_name': name,
                        'dataset_ids' : dataset_ids}

        self.connection.call('add_dataset_group', {'group':new_group})

    def create_timeseries(self, wml_timeseries):

        meta = self.get_metadata(wml_timeseries)
        
        values   = wml_timeseries.values[0]

        timesteps = values.get_date_values(utc=True)

        ts_dict = {}
        for ts in timesteps:
            ts_time = ts[0]
            ts_val  = ts[1]
            if ts_dict.get(ts_time):
                ts_dict[ts_time].append(ts_val)
            else:
                ts_dict[ts_time] = [ts_val]

        timeseries_val = []
        for ts_time, ts_val in ts_dict.items():
            if len(ts_val) == 1:
                ts_val = ts_val[0]
            timestep_val = {'ts_time' : str(ts_time), 'ts_value' : ts_val}
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
            hidden='N',
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
        
        meta_ts_dict = {}
        for wml_val in values:
            meta_ts_time = str(wml_val.date_time_utc)
            meta_ts_val  = [wml_val.lab_sample_code, wml_val.quality_control_level, wml_val.method_code, wml_val.source_code]
            if meta_ts_dict.get(meta_ts_time):
                meta_ts_dict[meta_ts_time].append(meta_ts_val)
            else:
                meta_ts_dict[meta_ts_time] = [meta_ts_val]
        timestep_meta = []
        for t, v in meta_ts_dict.items():
            if len(v) == 1:
                v = v[0]
            timestep_meta.append({'ts_time' : t,
                                  'ts_value' : v})
        meta_dataset = dict(
            id=None,
            type='timeseries',
            unit=None,
            dimension=None,
            name="%s %s %s metadata"%(meta['data_type'],meta['variable_name'], meta['site_codes']),
            value={'ts_values':timestep_meta},
            hidden='N',
            metadata=[],
        )

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
    parser.add_argument('-t', '--timeseriesfile', nargs='+',
                        help='''The XML file containing a WaterML timeseries.''')
    parser.add_argument('-n', '--uploadname',
                        help='''The name of the dataset group into which all the timeseries will be put.''')
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

    xml_response = PluginLib.create_xml_response('ImportWML',
                                                 None,
                                                 [],
                                                 errors,
                                                 importwml.warnings,
                                                 importwml.message,
                                                 importwml.files)

    print xml_response
