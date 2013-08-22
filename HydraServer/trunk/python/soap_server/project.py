from spyne.service import ServiceBase
from spyne.decorator import rpc
from spyne.model.primitive import Integer, Boolean
from HydraLib.HydraException import HydraError
import logging
from db import HydraIface
from HydraLib import hdb
from hydra_complexmodels import Project

class ProjectService(ServiceBase):
    """
        The project SOAP service
    """
    

    @rpc(Project, _returns=Project) 
    def add_project(ctx, project):
        """
            Add a new project
            returns a project complexmodel
        """
        try:
            x = HydraIface.Project()
            x.db.project_name = project.project_name
            x.db.project_description = project.project_description
            x.save()
            x.commit()
            return x.get_as_complexmodel()
        except Exception, e:
            logging.critical(e)
            hdb.rollback()
            return None

    @rpc(Project, _returns=Project) 
    def update_project(ctx, project):
        """
            Update a project
            returns a project complexmodel
        """
        try:
            x = HydraIface.Project(project_id = project.project_id)
            x.db.project_name = project.project_name
            x.db.project_description = project.project_description
            x.save()
            x.commit()
            return x.get_as_complexmodel()

        except Exception, e:
            logging.critical(e)
            hdb.rollback()
            return None
 

    @rpc(Integer, _returns=Project)
    def get_project(ctx, project_id):
        """
            get a project complexmodel
        """
        x = HydraIface.Project(project_id = project_id)
        return x.get_as_complexmodel()

    @rpc(Integer, _returns=Boolean)
    def delete_project(ctx, project_id):
        """
            Set the status of a project to 'X'
        """
        success = True
        try:
            x = HydraIface.Project(project_id = project_id)
            x.db.status = 'X'
            x.save()
            x.commit()
        except HydraError, e:
            logging.critical(e)
            success = False
            hdb.rollback()

        return success

#    @rpc(Integer, _returns=SpyneArray(Network))
#    def get_networks(ctx, project_id):
#        """
#            Get all networks in a project
#        """
#        networks = []
#
#        x = HydraIface.Project(project_id = project_id)
#        
#        for n_i in x.get_networks():
#            networks.append(n_i.get_as_complexmodel())
#
#        return networks
        



