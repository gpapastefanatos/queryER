package org.imis.calcite.rel.core;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

/**
 * 
 * @author bstam
 * This is the base Class of all MergeEntities relational operators physical or logical.
 * For calcite we need to first create a base class that extends a RelNode class and then
 * extend this class with whatever we want.
 */
public abstract class MergeEntities extends SingleRel  {

	protected MergeEntities(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
		super(cluster, traits, input);
		// TODO Auto-generated constructor stub
	}
	
	@Override public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return copy(traitSet, sole(inputs));
	}
	
	public abstract MergeEntities copy(RelTraitSet traitSet,  RelNode input);


}
