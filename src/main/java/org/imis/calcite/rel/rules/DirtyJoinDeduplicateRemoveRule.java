package org.imis.calcite.rel.rules;

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.tools.RelBuilderFactory;
import org.imis.calcite.rel.core.Deduplicate;
import org.imis.calcite.rel.logical.LogicalDeduplicateJoin;

/**
 * 
 * @author bstam
 * An important rule that checks the join type and removes the deduplication from the corresponding
 * tablescan.
 */
public class DirtyJoinDeduplicateRemoveRule extends RelOptRule{

	
	public static final DirtyJoinDeduplicateRemoveRule INSTANCE =
			new DirtyJoinDeduplicateRemoveRule(LogicalDeduplicateJoin.class,  Deduplicate.class,
					RelFactories.LOGICAL_BUILDER);

	/** Creates a ProjectJoinRemoveRule. */
	public DirtyJoinDeduplicateRemoveRule(
			Class<? extends LogicalDeduplicateJoin> joinClass,
			Class<? extends Deduplicate> deduplicateClass,
			RelBuilderFactory relBuilderFactory) {
		super(
				operandJ(joinClass, null,
						join -> join.getJoinType() == JoinRelType.DIRTY
							 || join.getJoinType() == JoinRelType.CLEAN,
					operand(deduplicateClass, any()),
					operand(deduplicateClass, any())),
				relBuilderFactory, null);
	}
	protected DirtyJoinDeduplicateRemoveRule(RelOptRuleOperand operand,
			RelBuilderFactory relBuilderFactory) {
		super(operand, relBuilderFactory, null);
	}


	/** Creates a JoinDeduplicateRemoveRule. */
	public DirtyJoinDeduplicateRemoveRule(RelOptRuleOperand operand,
			String description, RelBuilderFactory relBuilderFactory) {
		super(operand, relBuilderFactory, description);
	}

	
	/** Creates a JoinDeduplicateRemoveRule with default factory. */
	public DirtyJoinDeduplicateRemoveRule(
			RelOptRuleOperand operand,
			String description) {
		this(operand, description, RelFactories.LOGICAL_BUILDER);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		// TODO Auto-generated method stub
		final Join join = call.rel(0);
		final Deduplicate deduplicateLeft = call.rel(1);
		final Deduplicate deduplicateRight = call.rel(2);
		RelNode newJoin = null;
		List<String> tableLeft = deduplicateLeft.getRelTable().getQualifiedName();
		String leftTableName = "";
		
		List<String> tableRight = deduplicateRight.getRelTable().getQualifiedName();
		String rightTableName = "";
		if(tableLeft.size() > 0) {
			leftTableName = tableLeft.get(1);
		}
		if(tableRight.size() > 0) {
			rightTableName = tableRight.get(1);
		}
		
		//TODO: WRITE COMMENT FOR THIS
		if(!join.isDirtyJoin()) {
		
			newJoin = LogicalDeduplicateJoin.create(deduplicateLeft.getInput(0), deduplicateRight.getInput(0), join.getCondition(),
					join.getVariablesSet(), JoinRelType.DIRTY, deduplicateLeft.getKey(), deduplicateRight.getKey(),
					leftTableName, rightTableName, 
					deduplicateLeft.getFieldTypes().size(), deduplicateRight.getFieldTypes().size(), false);
			
		}
		else {
			newJoin = LogicalDeduplicateJoin.create(join.getLeft(), join.getRight(), join.getCondition(),
					join.getVariablesSet(), JoinRelType.CLEAN, join.getKeyLeft(), join.getKeyRight(),
					join.getTableNameLeft(), join.getTableNameRight(), 
					join.getKeyLeft(), join.getKeyRight(), true);
			
		}
		if(newJoin != null) {
			call.transformTo(newJoin);
		
		}
	}

}
