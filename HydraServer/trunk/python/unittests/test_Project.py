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
import test_HydraIface
from db import HydraIface
import datetime

class ProjectTest(test_HydraIface.HydraIfaceTest):
    def test_update(self):
        x = HydraIface.Project()
        x.db.project_name = "test @ %s"%datetime.datetime.now()
        x.db.project_description = "test description"
        x.save()
        x.commit()
        
        new_name = "test @ %s"%datetime.datetime.now()
        x.db.project_name = new_name 
        x.save()
        x.commit()
        x.load()
        assert x.db.project_name == new_name, "Project did not update correctly"

    def test_delete(self):
        x = HydraIface.Project()
        x.db.project_name = "test @ %s"%datetime.datetime.now()
        x.db.project_description = "test description"
        x.save()
        x.commit()

        assert x.load() == True, "Pre-delete save did not work correctly."

        x.delete()
        assert x.load() == False, "Delete did not work correctly."

    def test_load(self):
        x = HydraIface.Project()
        x.db.project_name = "test @ %s"%datetime.datetime.now()
        x.db.project_description = "test description"
        x.save()
        x.commit()
        x.load()
        y = HydraIface.Project(project_id=x.db.project_id)
        assert y.load() == True, "Load did not work correctly"


if __name__ == "__main__":
    test_HydraIface.run() # run all tests
