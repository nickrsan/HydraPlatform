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
import datetime
import logging
from decimal import Decimal, ROUND_HALF_UP

from operator import mul


def get_datetime(timestamp):

    if isinstance(timestamp, datetime.datetime):
        return timestamp

    FORMAT = "%Y-%m-%d %H:%M:%S.%f"
    #"2013-08-13T15:55:43.468886Z"

    if timestamp[0:4] == 'XXXX':
        # Do seasonal time series stuff...
        timestamp = timestamp.replace('XXXX', '0001')
    # and proceed as usual
    try:
        ts_time = datetime.datetime.strptime(timestamp, FORMAT)
    except ValueError as e:
        if e.message.split(' ', 1)[0].strip() == 'unconverted':
            utcoffset = e.message.split()[3].strip()
            timestamp = timestamp.replace(utcoffset, '')
            ts_time = datetime.datetime.strptime(timestamp, FORMAT)
            # Apply offset
            tzoffset = datetime.timedelta(hours=int(utcoffset[0:3]),
                                          minutes=int(utcoffset[3:5]))
            ts_time -= tzoffset
        else:
            raise e

    return ts_time


def timestamp_to_ordinal(timestamp):
    """Convert a timestamp as defined in the soap interface to the time format
    stored in the database.
    """

    if timestamp is None:
        return None

    ts_time = get_datetime(timestamp)
    # Convert time to Gregorian ordinal (1 = January 1st, year 1)
    ordinal_ts_time = Decimal(ts_time.toordinal())
    total_seconds = (ts_time -
                     datetime.datetime(ts_time.year,
                                       ts_time.month,
                                       ts_time.day,
                                       0, 0, 0)).total_seconds()

    fraction = (Decimal(repr(total_seconds)) / Decimal(86400)).quantize(Decimal('.00000000000000000001'),rounding=ROUND_HALF_UP)
    ordinal_ts_time += fraction
    logging.debug("%s converted to %s", timestamp, ordinal_ts_time)

    return ordinal_ts_time


def ordinal_to_timestamp(date):
    if date is None:
        return None

    day = int(date)
    time = date - day
    time_in_secs_ms = (time * Decimal(86400)).quantize(Decimal('.000001'),
                                                       rounding=ROUND_HALF_UP)

    time_in_secs = int(time_in_secs_ms)
    time_in_ms = int((time_in_secs_ms - time_in_secs) * 1000000)

    td = datetime.timedelta(seconds=int(time_in_secs), microseconds=time_in_ms)
    d = datetime.datetime.fromordinal(day) + td
    logging.debug("%s converted to %s", date, d)

    return d


def array_dim(arr):
    """Return the size of a multidimansional array.
    """
    dim = []
    while True:
        try:
            dim.append(len(arr))
            arr = arr[0]
        except TypeError:
            return dim


def arr_to_vector(arr):
    """Reshape a multidimensional array to a vector.
    """
    dim = array_dim(arr)
    tmp_arr = []
    for n in range(len(dim) - 1):
        for inner in arr:
            for i in inner:
                tmp_arr.append(i)
        arr = tmp_arr
        tmp_arr = []
    return arr


def vector_to_arr(vec, dim):
    """Reshape a vector to a multidimensional array with dimensions 'dim'.
    """
    if len(dim) <= 1:
        return vec
    array = vec
    while len(dim) > 1:
        i = 0
        outer_array = []
        for m in range(reduce(mul, dim[0:-1])):
            inner_array = []
            for n in range(dim[-1]):
                inner_array.append(array[i])
                i += 1
            outer_array.append(inner_array)
        array = outer_array
        dim = dim[0:-1]

        return array


if __name__ == '__main__':
    pass
