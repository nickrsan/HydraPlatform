#!/usr/bin/env python
# -*- coding: utf-8 -*-

from HydraLib import util, hydra_logging
from suds.client import Client

from datetime import datetime


def connect():
    hydra_logging.init(level='INFO')
    config = util.load_config()
    url = config.get('hydra_client', 'url')
    user = config.get('hydra_client', 'user')
    passwd = config.get('hydra_client', 'password')
    cli = Client(url, timeout=2400)
    session_id = cli.service.login(user, passwd)
    token = cli.factory.create('RequestHeader')
    token.session_id = session_id
    token.username = user
    cli.set_options(soapheaders=token)
    #cli.add_prefix('hyd', 'hydra.base')
    cli.add_prefix('hyd', 'soap_server.hydra_complexmodels')

    return cli


def temp_ids(n=-1):
    """
    Create an iterator for temporary IDs for nodes, links and other entities
    that need them. You need to initialise the temporary id first and call the
    next element using the ``.next()`` function::

        temp_node_id = PluginLib.temp_ids()

        # Create a node
        # ...

        Node.id = temp_node_id.next()
    """
    while True:
        yield n
        n -= 1


def date_to_string(date):
    """Convert a date to a standard string used by Hydra. The resulting string
    looks like this::

        '2013-10-03 00:49:17.568960-0400'
    """
    FORMAT = '%Y-%m-%d %H:%M:%S.%f%z'
    return date.strftime(FORMAT)


def guess_timefmt(datestr):
    """
    Try to guess the format a date is written in.

    The following formats are supported:

    ================ ============== ===============
    Format           Example        Python format
    ---------------- -------------- ---------------
    `YYYY-MM-DD`     2002-04-21     %Y-%m-%d
    `YYYY.MM.DD`     2002.04.21     %Y.%m.%d
    `YYYY MM DD`     2002 04 21     %Y %m %d
    `DD-MM-YYYY`     21-04-2002     %d-%m-%Y
    `DD.MM.YYYY`     21.04.2002     %d.%m.%Y
    `DD MM YYYY`     21 04 2002     %d %m %Y
    `MM/DD/YYYY`     04/21/2002     %m/%d/%Y
    ================ ============== ===============

    The following formats are recognised depending on your locale setting.
    There is no guarantee that this will work.

    ================ ============== ===============
    Format           Example        Python format
    ---------------- -------------- ---------------
    `DD-mmm-YYYY`    21-Apr-2002    %d-%b-%Y
    `DD.mmm.YYYY`    21.Apr.2002    %d.%b.%Y
    `DD mmm YYYY`    21 Apr 2002    %d %b %Y
    `mmm DD YYYY`    Apr 21 2002    %b %d %Y
    `Mmmmm DD YYYY`  April 21 2002  %B %d %Y
    ================ ============== ===============

    .. note::
        - The time needs to follow this definition without exception:
            `%H:%M:%S`. A complete date and time should therefore look like
            this::

                2002-04-21 15:29:37

        - Be aware that in a file with comma separated values you should not
          use a date format that contains commas.
    """

    delimiters = ['-', '.', ' ']
    formatstrings = [['%Y', '%m', '%d'],
                     ['%d', '%m', '%Y'],
                     ['%d', '%b', '%Y']]

    timefmt = '%H:%M:%S'

    # Check if a time is indicated or not
    try:
        datetime.strptime(datestr.split(' ')[-1].strip(), timefmt)
        usetime = True
    except ValueError:
        usetime = False

    # Check the simple ones:
    for fmt in formatstrings:
        for delim in delimiters:
            datefmt = fmt[0] + delim + fmt[1] + delim + fmt[2]
            if usetime:
                datefmt = datefmt + ' ' + timefmt
            try:
                datetime.strptime(datestr, datefmt)
                return datefmt
            except ValueError:
                pass

    # Check for other formats:
    custom_formats = ['%m/%d/%Y', '%b %d %Y', '%B %d %Y']

    for fmt in custom_formats:
        if usetime:
            datefmt = fmt + ' ' + timefmt
        else:
            datefmt = fmt

        try:
            datetime.strptime(datestr, datefmt)
            return datefmt
        except ValueError:
            pass

    return None
