import unittest
from HydraLib import util, hydra_logging

class HydraIfaceTest(unittest.TestCase):
    def setUp(self):
        self.connection = util.connect()
        hydra_logging.init(level='DEBUG')
 
    def tearDown(self):
        hydra_logging.shutdown()
        util.disconnect(self.connection)

if __name__ == "__main__":
    unittest.main() # run all tests
