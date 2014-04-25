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
from spyne.model.primitive import Integer
from spyne.decorator import rpc
from db import HydraIface
from hydra_complexmodels import Constraint,\
        ConstraintGroup,\
        get_as_complexmodel

from hydra_base import HydraService
from hydra_base import RequestHeader 

def create_constraint_struct(constraint_id, group):
    group_i = HydraIface.ConstraintGroup()
    group_i.db.op = group.op
    group_i.db.constraint_id = constraint_id
   


    for ref_num, sub_element in enumerate(group.constraintgroups + group.constraintitems):
        if isinstance(sub_element, ConstraintGroup):
            setattr(group_i.db, "ref_key_%s"%(ref_num+1), 'GRP')
            sub_group_i = create_constraint_struct(constraint_id, sub_element)
            setattr(group_i.db, "ref_id_%s"%(ref_num+1), sub_group_i.db.group_id)
            group_i.groups.append(sub_group_i)
        else:
            item_i = HydraIface.ConstraintItem()
            
            if sub_element.constant is None:
                item_i.db.resource_attr_id = sub_element.resource_attr_id
            else:
                item_i.db.constant         = sub_element.constant

            item_i.db.constraint_id    = constraint_id
            item_i.save()
            setattr(group_i.db, "ref_key_%s"%(ref_num+1), 'ITEM')
            setattr(group_i.db, "ref_id_%s"%(ref_num+1), item_i.db.item_id)
            group_i.items.append(item_i) 

    group_i.save()

    return group_i

class ConstraintService(HydraService):
    """
        The user soap service
    """

    __tns__ = 'hydra.soap'
    __in_header__ = RequestHeader

    @rpc(Integer, Constraint, _returns=Constraint)
    def add_constraint(ctx, scenario_id, constraint):
        """
            Add a new constraint to a scenario.
        """
        constraint_i = HydraIface.Constraint()
        constraint_i.db.scenario_id = scenario_id

        constraint_i.db.op = constraint.op
        constraint_i.db.constant = constraint.constant
    
        constraint_i.save()

        group_i = create_constraint_struct(constraint_i.db.constraint_id, constraint.constraintgroup)

        constraint_i.db.group_id = group_i.db.group_id
        constraint_i.save()

        return get_as_complexmodel(ctx, constraint_i)

    @rpc(Integer, Constraint, _returns=Constraint)
    def delete_constraint(ctx, scenario_id, constraint):
        """
            Add a new constraint to a scenario.
        """
        constraint_i = HydraIface.Constraint()
        
        return get_as_complexmodel(ctx, constraint_i)
 
    @rpc(ConstraintGroup, _returns=ConstraintGroup)
    def echo_constraintgroup(ctx, constraintgroup):
        """
            Echo a constraint group. Needed for the server
            to publish the ConstraintGroup object correctly
        """
        return constraintgroup
