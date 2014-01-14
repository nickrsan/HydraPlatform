import datetime
import logging
import os
import glob
import ConfigParser


def load_csv(cnx, cursor, filepath):

    sql = "insert into %(table_name)s (%(col_names)s) values (%(values)s)"

    filename = filepath.split('/')[-1]

    with open(filepath, 'r') as f:
        col_names = f.readlines[0]
        for w in f.readlines()[1:]:
            w = w.strip()
            entry = w.split(',')
            params = dict(
                table_name=filename,
                col_names=col_names,
                values=entry,
            )

        dt = datetime.datetime.strptime("%s %s" % (entry[0], entry[1]),
                                        "%d/%m/%Y %H:%M:%S")

        params['time'] = dt

        cursor.execute(sql, params)
        cnx.commit()
        cursor.close()
        cnx.close()

def timestamp_to_server_time(timestamp):
    """Convert a timestamp as defined in the soap interface to the time format
    stored in the database.
    """

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

    # Convert time to Gregorian ordinal (1 = January 1st, year 1)
    ordinal_ts_time = ts_time.toordinal()
    fraction = (ts_time -
                datetime.datetime(ts_time.year,
                                  ts_time.month,
                                  ts_time.day,
                                  0, 0, 0)).total_seconds()

    fraction = fraction / (86400)

    ordinal_ts_time += fraction

    return ordinal_ts_time


def convert_ordinal_to_datetime(date):
    day = int(date)
    time = date - day

    time_in_secs_ms = time * 86400

    time_in_secs = int(time_in_secs_ms)
    time_in_ms = int((time_in_secs_ms - time_in_secs) * 100000)

    td = datetime.timedelta(seconds=int(time_in_secs), microseconds=time_in_ms)
    d = datetime.datetime.fromordinal(day) + td

    return d


if __name__ == '__main__':
    pass
