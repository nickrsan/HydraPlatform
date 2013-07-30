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

        (1) ~/.config/hydra/
        (2) /etc/hydra
        (3) [...]/HYDRA/HydraLib/trunk/../../config/

    (1) will override (2) will override (3). Parameter not defined in (1) will
    be taken from (2). Parameters not defined in (2) will be taken from (3).
    (3) is the config folder that will be checked out from the svn repository.
    (2) Will be be provided as soon as an installable distribution is
    available. (1) will usually be written individually by every user."""
    #TODO: Check for the operating system we are running, provide search paths
    #      for Windows machines.
    modulepath = os.path.dirname(os.path.abspath(__file__))

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

    return config


if __name__ == '__main__':
    config = load_config()
    print config.get('mysqld', 'user')
