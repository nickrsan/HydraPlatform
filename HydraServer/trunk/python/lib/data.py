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
import datetime
import sys
from HydraLib.util import timestamp_to_ordinal
import logging
global FORMAT
FORMAT = "%Y-%m-%d %H:%M:%S.%f"
#"2013-08-13T15:55:43.468886Z"

current_module = sys.modules[__name__]


def parse_value(data):
    """
        Turn a complex model object into a hydraiface - friendly value.
    """

    data_type = data.type

    if data.value is None:
        logging.warn("Cannot parse dataset. No value specified.")
        return None

    #attr_data.value is a dictionary,
    #but the keys have namespaces which must be stripped.
    val_names = data.value.keys()
    value = []
    for name in val_names:
        value.append(data.value[name])

    if data_type == 'descriptor':
        return value[0][0]
    elif data_type == 'timeseries':
        # The brand new way to parse time series data:
        ts = []
        for ts_val in value[0]:
            for key in ts_val.keys():
                if key.find('ts_time') > 0:
                    #The value is a list, so must get index 0
                    timestamp = ts_val[key][0]
                    # Check if we have received a seasonal time series first
                    ordinal_ts_time = timestamp_to_ordinal(timestamp)
                elif key.find('ts_value') > 0:
                    series = []
                    for val in ts_val[key]:
                        series.append(eval(val))
                        ts.append((ordinal_ts_time, eval(val)))

        return ts
    elif data_type == 'eqtimeseries':
        start_time = datetime.strptime(value[0][0], FORMAT)
        frequency  = value[1][0]
        arr_data   = eval(value[2][0])
        return (start_time, frequency, arr_data)
    elif data_type == 'scalar':
        return value[0][0]
    elif data_type == 'array':
        val = eval(value[0][0])
        return val


def get_array(arr):

    if len(arr) == 0:
        return []

    #am I a dictionary? If so, i'm only iterested in the values
    if type(arr) is dict:
        arr = arr[0]

    if type(arr[0]) is str:
        return [float(val) for val in arr]

    #arr must therefore be a list.
    current_level = []
    for level in arr:
        current_level.append(get_array(level))

    return current_level
