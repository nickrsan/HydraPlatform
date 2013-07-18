from db import HydraIface
from HydraLib import hydra_logging
from HydraLib.HydraException import DBException
import logging
 
def test_insert():
    x = HydraIface.Project()
    x.db.project_name = "test"
    x.db.project_description = "test description"
    x.save()
    x.commit()

    hydra_logging.shutdown()

def test_update():
    x = HydraIface.Project()
    x.db.project_name = "test"
    x.db.project_description = "test description"
    x.save()
    x.commit()

    x.db.project_name = "test_new"
    x.save()
    x.commit()
    x.load()
    logging.debug(x.db.project_name)

    hydra_logging.shutdown()

def test_delete():
    x = HydraIface.Project()
    x.db.project_name = "test"
    x.db.project_description = "test description"
    x.save()
    x.commit()

    x.db.project_name = "test_new"
    x.save()
    x.delete()
    x.load()
    logging.debug(x.db.project_name)

    hydra_logging.shutdown()

def test_load():
    x = HydraIface.Project(project_id=10)
    x.load()

    logging.debug(x.db.project_name)

    hydra_logging.shutdown()


if __name__ == '__main__':
    hydra_logging.init(level='DEBUG')
    test_load()
    hydra_logging.shutdown()

