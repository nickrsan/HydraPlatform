#!/usr/bin/env python

from server import HydraServer

server = HydraServer()
application = server.crate_application()
