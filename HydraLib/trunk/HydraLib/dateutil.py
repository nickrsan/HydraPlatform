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
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import logging
from decimal import Decimal, ROUND_HALF_UP

log = logging.getLogger(__name__)

#"2013-08-13 15:55:43.468886Z"
FORMAT = "%Y-%m-%d %H:%M:%S.%f"

def get_datetime(timestamp):

    if isinstance(timestamp, datetime):
        return timestamp

    fmt = guess_timefmt(timestamp)
    if fmt is None:
        fmt = FORMAT

    # and proceed as usual
    try:
        ts_time = datetime.strptime(timestamp, fmt)
    except ValueError as e:
        if e.message.split(' ', 1)[0].strip() == 'unconverted':
            utcoffset = e.message.split()[3].strip()
            timestamp = timestamp.replace(utcoffset, '')
            ts_time = datetime.strptime(timestamp, fmt)
            # Apply offset
            tzoffset = timedelta(hours=int(utcoffset[0:3]),
                                          minutes=int(utcoffset[3:5]))
            ts_time -= tzoffset
        else:
            raise e

    if timestamp[0:4] == 'XXXX':
        # Do seasonal time series stuff...
        timestamp = timestamp.replace('XXXX', '1900')

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
                     datetime(ts_time.year,
                                       ts_time.month,
                                       ts_time.day,
                                       0, 0, 0)).total_seconds()

    fraction = (Decimal(repr(total_seconds)) / Decimal(86400)).quantize(Decimal('.00000000000000000001'),rounding=ROUND_HALF_UP)
    ordinal_ts_time += fraction
    log.debug("%s converted to %s", timestamp, ordinal_ts_time)

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

    td = timedelta(seconds=int(time_in_secs), microseconds=time_in_ms)
    d = datetime.fromordinal(day) + td
    log.debug("%s converted to %s", date, d)

    return get_datetime(d)

def date_to_string(date, seasonal=False):
    """Convert a date to a standard string used by Hydra. The resulting string
    looks like this::

        '2013-10-03 00:49:17.568-0400'

    Hydra also accepts seasonal time series (yearly recurring). If the flag
    ``seasonal`` is set to ``True``, this function will generate a string
    recognised by Hydra as seasonal time stamp.
    """
    if seasonal:
        FORMAT = 'XXXX-%m-%d %H:%M:%S.%f'
    else:
        FORMAT = '%Y-%m-%d %H:%M:%S.%f'
    return date.strftime(FORMAT)


def guess_timefmt(datestr):
    """
    Try to guess the format a date is written in.

    The following formats are supported:

    ================= ============== ===============
    Format            Example        Python format
    ----------------- -------------- ---------------
    ``YYYY-MM-DD``    2002-04-21     %Y-%m-%d
    ``YYYY.MM.DD``    2002.04.21     %Y.%m.%d
    ``YYYY MM DD``    2002 04 21     %Y %m %d
    ``DD-MM-YYYY``    21-04-2002     %d-%m-%Y
    ``DD.MM.YYYY``    21.04.2002     %d.%m.%Y
    ``DD MM YYYY``    21 04 2002     %d %m %Y
    ``MM/DD/YYYY``    04/21/2002     %m/%d/%Y
    ================= ============== ===============

    These formats can also be used for seasonal (yearly recurring) time series.
    The year needs to be replaced by ``XXXX``.

    The following formats are recognised depending on your locale setting.
    There is no guarantee that this will work.

    ================= ============== ===============
    Format            Example        Python format
    ----------------- -------------- ---------------
    ``DD-mmm-YYYY``   21-Apr-2002    %d-%b-%Y
    ``DD.mmm.YYYY``   21.Apr.2002    %d.%b.%Y
    ``DD mmm YYYY``   21 Apr 2002    %d %b %Y
    ``mmm DD YYYY``   Apr 21 2002    %b %d %Y
    ``Mmmmm DD YYYY`` April 21 2002  %B %d %Y
    ================= ============== ===============

    .. note::
        - The time needs to follow this definition without exception:
            `%H:%M:%S.%f`. A complete date and time should therefore look like
            this::

                2002-04-21 15:29:37.522

        - Be aware that in a file with comma separated values you should not
          use a date format that contains commas.
    """

    #replace 'T' with space to handle ISO times.
    if datestr.find('T') > 0:
        dt_delim = 'T'
    else:
        dt_delim = ' '

    delimiters = ['-', '.', ' ']
    formatstrings = [['%Y', '%m', '%d'],
                     ['%d', '%m', '%Y'],
                     ['%d', '%b', '%Y'],
                     ['XXXX', '%m', '%d'],
                     ['%d', '%m', 'XXXX'],
                     ['%d', '%b', 'XXXX']]

    timeformats = ['%H:%M:%S.%f', '%H:%M:%S', '%H:%M']

    # Check if a time is indicated or not
    for timefmt in timeformats:
        try:
            datetime.strptime(datestr.split(dt_delim)[-1].strip(), timefmt)
            usetime = True
            break
        except ValueError:
            usetime = False

    # Check the simple ones:
    for fmt in formatstrings:
        for delim in delimiters:
            datefmt = fmt[0] + delim + fmt[1] + delim + fmt[2]
            if usetime:
                for timefmt in timeformats:
                    complfmt = datefmt + dt_delim + timefmt
                    try:
                        datetime.strptime(datestr, complfmt)
                        return complfmt
                    except ValueError:
                        pass
            else:
                try:
                    datetime.strptime(datestr, datefmt)
                    return datefmt
                except ValueError:
                    pass

    # Check for other formats:
    custom_formats = ['%m/%d/%Y', '%b %d %Y', '%B %d %Y', '%m/%d/XXXX']

    for fmt in custom_formats:
        if usetime:
            for timefmt in timeformats:
                complfmt = fmt + dt_delim + timefmt
                try:
                    datetime.strptime(datestr, complfmt)
                    return complfmt
                except ValueError:
                    pass

        else:
            try:
                datetime.strptime(datestr, fmt)
                return fmt
            except ValueError:
                pass

    return None


