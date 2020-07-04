package org.imis.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.tools.RelBuilderFactory;
import org.imis.calcite.rel.core.Deduplicate;
import org.imis.calcite.rel.logical.LogicalDeduplicateJoin;

/**
 * 
 * @author bstam
 * An important rule that checks the join type and removes the deduplication from the corresponding
 * tablescan.
 * e.x if it is a dirtyRight join then we remove the duplication from the right
 * This rule can be used to statistically infer the best plan, deduplicate first and then join
 * or join and then deduplicate.
 */
public class MultiJoinDirtyLeftDeduplicateRemoveRule extends RelOptRule{


	public static final MultiJoinDirtyLeftDeduplicateRemoveRule INSTANCE =
			new MultiJoinDirtyLeftDeduplicateRemoveRule(LogicalDeduplicateJoin.class, 
					 Deduplicate.class, LogicalDeduplicateJoin.class,
					RelFactories.LOGICAL_BUILDER);

	/** Creates a ProjectJoinRemoveRule. */
	public MultiJoinDirtyLeftDeduplicateRemoveRule(
			Class<? extends Join> joinClass,
			Class<? extends Deduplicate> deduplicateClass,
			Class<? extends Join> joinClass2,
			RelBuilderFactory relBuilderFactory) {
		super(
				operandJ(joinClass, null,
					join -> join.getJoinType() == JoinRelType.DIRTY_LEFT,
					operand(deduplicateClass, any()),
					operand(joinClass, any())
					),
				relBuilderFactory, null);
	}
	protected MultiJoinDirtyLeftDeduplicateRemoveRule(RelOptRuleOperand operand,
			RelBuilderFactory relBuilderFactory) {
		super(operand, relBuilderFactory, null);
	}


	/** Creates a JoinDeduplicateRemoveRule. */
	public MultiJoinDirtyLeftDeduplicateRemoveRule(RelOptRuleOperand operand,
			String description, RelBuilderFactory relBuilderFactory) {
		super(operand, relBuilderFactory, description);
	}

	
	/** Creates a JoinDeduplicateRemoveRule with default factory. */
	public MultiJoinDirtyLeftDeduplicateRemoveRule(
			RelOptRuleOperand operand,
			String description) {
		this(operand, description, RelFactories.LOGICAL_BUILDER);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		// TODO Auto-generated method stub
		final Join join = call.rel(0);
		final Deduplicate deduplicateLeft = call.rel(1);
		
		RelNode newJoin = null;
	
		if(join.getJoinType() == JoinRelType.DIRTY_LEFT) {
			newJoin = LogicalDeduplicateJoin.create(deduplicateLeft.getInput(), join.getRight(),  join.getCondition(),
					join.getVariablesSet(), join.getJoinType(), join.getKeyLeft(), join.getKeyRight(),
					join.getTableNameLeft(), join.getTableNameRight(), join.getFieldLeft(), join.getFieldRight());
		}		
		
		if(newJoin != null) {
			call.transformTo(newJoin);
		}
	}

}
