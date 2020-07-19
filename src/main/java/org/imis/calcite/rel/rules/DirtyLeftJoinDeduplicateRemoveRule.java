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
public class DirtyLeftJoinDeduplicateRemoveRule extends RelOptRule {

	public static final DirtyLeftJoinDeduplicateRemoveRule INSTANCE =
			new DirtyLeftJoinDeduplicateRemoveRule(LogicalDeduplicateJoin.class, 
					Deduplicate.class, Filter.class, 
					RelFactories.LOGICAL_BUILDER);
	
	public DirtyLeftJoinDeduplicateRemoveRule(Class<? extends LogicalDeduplicateJoin> joinClass,
			Class<? extends Deduplicate> deduplicateClass, Class<? extends Filter> filterClass, RelBuilderFactory relBuilderFactory) {
		
			super(
				operandJ(joinClass, null,
						join -> join.getJoinType() == JoinRelType.DIRTY_LEFT,
					operand(deduplicateClass, any()),
					operand(filterClass, any())),
				relBuilderFactory, null);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		// TODO Auto-generated method stub
		final Join join = call.rel(0);
		final Deduplicate deduplicateLeft = call.rel(1);
		final Filter filterRight = call.rel(2);
		
		RelNode newJoin = null;
	

		List<String> tableLeft = deduplicateLeft.getRelTable().getQualifiedName();
		String leftTableName = "";
		
		if(tableLeft.size() > 0) {
			leftTableName = tableLeft.get(1);
		}

		newJoin = LogicalDeduplicateJoin.create(deduplicateLeft.getInput(0), join.getRight(), join.getCondition(),
				join.getVariablesSet(), join.getJoinType(), deduplicateLeft.getKey(), null,
				leftTableName, null, deduplicateLeft.getFieldTypes().size(), null, true);	
		
		if(newJoin != null) {
			call.transformTo(newJoin);
		}
	}
}
