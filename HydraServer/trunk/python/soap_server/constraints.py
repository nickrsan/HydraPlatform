from spyne.model.primitive import Integer, String
from spyne.decorator import rpc
from db import HydraIface
from hydra_complexmodels import Constraint,\
        ConstraintItem,\
        ConstraintGroup

from hydra_base import HydraService
from hydra_base import RequestHeader 
from HydraLib.HydraException import HydraError
import logging

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
            item_i.db.resource_attr_id = sub_element.resource_attr_id
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

        return constraint_i.get_as_complexmodel()

    @rpc(Integer, Constraint, _returns=Constraint)
    def delete_constraint(ctx, scenario_id, constraint):
        """
            Add a new constraint to a scenario.
        """
        constraint_i = HydraIface.Constraint()
        
        return constraint_i.get_as_complexmodel()
 
    @rpc(ConstraintGroup, _returns=ConstraintGroup)
    def echo_constraintgroup(ctx, constraintgroup):
        """
            Echo a constraint group. Needed for the server
            to publish the ConstraintGroup object correctly
        """
        return constraintgroup

    @rpc(ConstraintItem, _returns=ConstraintItem)
    def echo_constraintgroup(ctx, constraintitem):
        """
            Echo a constraint item. Needed for the server
            to publish the ConstraintItem object correctly
        """
        return constraintitem
