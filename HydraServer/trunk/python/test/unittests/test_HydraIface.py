import unittest 
from HydraLib import util, hydra_logging
import logging

class HydraIfaceTest(unittest.TestCase):
    def setUp(self):
        hydra_logging.init(level='DEBUG')
        self.connection = util.connect()
 
    def tearDown(self):
        logging.debug("Tearing down")
        util.disconnect()
        hydra_logging.shutdown()

def run():
    unittest.main()

if __name__ == "__main__":
    run() # run all tests
