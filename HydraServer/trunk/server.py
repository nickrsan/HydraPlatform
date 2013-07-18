from db import HydraIface
from HydraLib import hydra_logging
import logging

if __name__ == '__main__':
	hydra_logging.init(level='DEBUG')
	x = HydraIface.Project()
	x.db.project_name = "test"
	x.db.project_description = "test description"
	x.save()
	x.commit()

	logging.shutdown()
	print "\033[0;mFinished\033[0;m"
