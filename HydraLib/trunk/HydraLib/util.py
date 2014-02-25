import datetime
import logging
from decimal import Decimal, ROUND_HALF_UP

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

    fraction = Decimal(repr(total_seconds)) / Decimal(86400)
    ordinal_ts_time += fraction
    logging.debug("%s converted to %s", timestamp, ordinal_ts_time)

    return ordinal_ts_time


def convert_ordinal_to_datetime(date):
    if date is None:
        return None

    day = int(date)
    time = date - day
    time_in_secs_ms = (time * Decimal(86400)).quantize(Decimal('.000001'), rounding=ROUND_HALF_UP)

    time_in_secs = int(time_in_secs_ms)
    time_in_ms = int((time_in_secs_ms - time_in_secs) * 1000000)

    td = datetime.timedelta(seconds=int(time_in_secs), microseconds=time_in_ms)
    d = datetime.datetime.fromordinal(day) + td
    logging.debug("%s converted to %s", date, d)

    return d


if __name__ == '__main__':
    pass
