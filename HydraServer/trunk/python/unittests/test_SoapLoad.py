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

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import test_SoapServer
import logging
import timeit

class NetworkTest(test_SoapServer.SoapServerTest):

    def create_large_network(self):
        self.create_network_with_data(num_nodes=1000)

    def test_add_large_network(self):
        time = timeit.Timer(self.create_large_network).timeit(number=1)
        logging.debug(time)
        assert time < 50


if __name__ == '__main__':
    #cProfile.run('test_SoapServer.run()', sort='cumulative')
    test_SoapServer.run()
