from db import HydraIface
from HydraLib import hydra_logging

if __name__ == '__main__':
	hydra_logging.init(level='DEBUG')
	x = HydraIface.Project()
	x.db.project_name = "test"
	x.db.project_description = "test description"
	x.save()
	x.commit()

	x.db.project_name = "test_new"
	x.save()
	x.commit()
	x.load()

	x.delete()
	x.save()
	x.commit()
	x.load()

	hydra_logging.shutdown()
