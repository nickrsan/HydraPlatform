#!/usr/bin/env python
# -*- coding: utf-8 -*-

from HydraLib import util, hydra_logging
from suds.client import Client


def connect():
    hydra_logging.init(level='INFO')
    config = util.load_config()
    url = config.get('hydra_client', 'url')
    user = config.get('hydra_client', 'user')
    passwd = config.get('hydra_client', 'password')
    cli = Client(url)
    session_id = cli.service.login(user, passwd)
    token = cli.factory.create('RequestHeader')
    token.session_id = session_id
    token.username = user
    cli.set_options(soapheaders=token)
    cli.add_prefix('hyd', 'soap_server.hydra_complexmodels')

    return cli
