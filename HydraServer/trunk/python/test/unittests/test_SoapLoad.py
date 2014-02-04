
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
