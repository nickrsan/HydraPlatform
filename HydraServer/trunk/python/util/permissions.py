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
from db import HydraIface
from HydraLib.HydraException import HydraError

def check_perm(user_id, permission_code):
    """
        Checks whether a user has permission to perform an action.
        The permission_code parameter should be a permission contained in tPerm.

        If the user does not have permission to perfom an action, a permission
        error is thrown.
    """
    sql = """
        select
            perm_id
        from
            tPerm
        where
            perm_code = '%s'
    """%(permission_code)
    rs = HydraIface.execute(sql)
    if len(rs) == 0:
        raise HydraError("No permission %s"%(permission_code))

    perm_id = rs[0].perm_id
    
    sql = """
        select
            r.role_id
        from
            tUser u,
            tRoleUser r,
            tPerm p,
            tRolePerm rp
        where
            u.user_id  = %s
        and r.user_id  = u.user_id
        and rp.role_id = r.role_id
        and rp.perm_id = p.perm_id
        and p.perm_id  = %s
    """ % (user_id, perm_id)

    rs = HydraIface.execute(sql)

    if len(rs) == 0:
        raise HydraError("Permission denied. User %s does not have permission %s"%
                        (user_id, permission_code))
