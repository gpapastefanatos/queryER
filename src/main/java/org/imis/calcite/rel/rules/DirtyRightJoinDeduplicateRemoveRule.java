package org.imis.calcite.rel.rules;

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;
import org.imis.calcite.rel.core.Deduplicate;
import org.imis.calcite.rel.logical.LogicalDeduplicateJoin;


/**
 * 
 * @author bstam
 * An important rule that checks the join type and removes the deduplication from the corresponding
 * tablescan.
 */
public class DirtyRightJoinDeduplicateRemoveRule extends RelOptRule {

	public static final DirtyRightJoinDeduplicateRemoveRule INSTANCE =
			new DirtyRightJoinDeduplicateRemoveRule(LogicalDeduplicateJoin.class, 
					Filter.class, Deduplicate.class,
					RelFactories.LOGICAL_BUILDER);
	
	public DirtyRightJoinDeduplicateRemoveRule(Class<? extends LogicalDeduplicateJoin> joinClass,
			Class<? extends Filter> filterClass, Class<? extends Deduplicate> deduplicateClass,  RelBuilderFactory relBuilderFactory) {
		
			super(
				operandJ(joinClass, null,
						join -> join.getJoinType() == JoinRelType.DIRTY_RIGHT,
					operand(filterClass, any()),
					operand(deduplicateClass, any())),
				relBuilderFactory, null);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		// TODO Auto-generated method stub
		final Join join = call.rel(0);
		final Filter filterLeft = call.rel(1);
		final Deduplicate deduplicateRight = call.rel(2);
		
		RelNode newJoin = null;
	

		List<String> tableRight = deduplicateRight.getRelTable().getQualifiedName();
		String rightTableName = "";

		if(tableRight.size() > 0) {
			rightTableName = tableRight.get(1);
		}
		newJoin = LogicalDeduplicateJoin.create(join.getLeft(), deduplicateRight.getInput(), join.getCondition(),
				join.getVariablesSet(), join.getJoinType(), null, deduplicateRight.getKey(),
				null, rightTableName, null,  deduplicateRight.getFieldTypes().size(), true);	
		
		if(newJoin != null) {
			call.transformTo(newJoin);
		
		}
	}
}
