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


def load_config():
    """Load a config file. This function looks for a config (*.ini) file in the
    following order::

        (1) ./*.ini
        (2) ~/.config/hydra/
        (3) /etc/hydra
        (4) [...]/HYDRA/HydraLib/trunk/../../config/

    (1) will override (2) will override (3) will override (4). Parameters not
    defined in (1) will be taken from (2). Parameters not defined in (2) will
    be taken from (3).  (3) is the config folder that will be checked out from
    the svn repository.  (2) Will be be provided as soon as an installable
    distribution is available. (1) will usually be written individually by
    every user."""
    #TODO: Check for the operating system we are running, provide search paths
    #      for Windows machines.
    modulepath = os.path.dirname(os.path.abspath(__file__))

    localfiles = glob.glob('*.ini')
    userfiles = glob.glob(os.path.expanduser('~') + '/.config/hydra/*.ini')
    sysfiles = glob.glob('/etc/hydra/*.ini')
    repofiles = glob.glob(modulepath + '/../../../config/*.ini')

    config = ConfigParser.RawConfigParser(allow_no_value=True)

    logging.debug('LOADING CONFIG FILES: ' +
                  ', '.join([', '.join(repofiles),
                             ', '.join(sysfiles),
                             ', '.join(userfiles)]))

    for ini_file in repofiles:
        config.read(ini_file)
    for ini_file in sysfiles:
        config.read(ini_file)
    for ini_file in userfiles:
        config.read(ini_file)
    for ini_file in localfiles:
        config.read(ini_file)

    return config


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


if __name__ == '__main__':
    pass
