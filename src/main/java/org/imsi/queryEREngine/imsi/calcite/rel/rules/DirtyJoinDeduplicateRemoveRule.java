package org.imsi.queryEREngine.imsi.calcite.rel.rules;

import java.util.List;

import org.imsi.queryEREngine.apache.calcite.plan.RelOptRule;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptRuleCall;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptRuleOperand;
import org.imsi.queryEREngine.apache.calcite.plan.RelOptUtil;
import org.imsi.queryEREngine.apache.calcite.rel.RelNode;
import org.imsi.queryEREngine.apache.calcite.rel.RelShuttle;
import org.imsi.queryEREngine.apache.calcite.rel.RelShuttleImpl;
import org.imsi.queryEREngine.apache.calcite.rel.RelVisitor;
import org.imsi.queryEREngine.apache.calcite.rel.core.Filter;
import org.imsi.queryEREngine.apache.calcite.rel.core.Join;
import org.imsi.queryEREngine.apache.calcite.rel.core.JoinRelType;
import org.imsi.queryEREngine.apache.calcite.rel.core.RelFactories;
import org.imsi.queryEREngine.apache.calcite.rel.hint.Hintable;
import org.imsi.queryEREngine.apache.calcite.rex.RexNode;
import org.imsi.queryEREngine.apache.calcite.tools.RelBuilderFactory;
import org.imsi.queryEREngine.imsi.calcite.rel.core.Deduplicate;
import org.imsi.queryEREngine.imsi.calcite.rel.logical.LogicalDeduplicateJoin;

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

	public DirtyJoinDeduplicateRemoveRule(Class<? extends LogicalDeduplicateJoin> joinClass, Class<? extends Deduplicate> deduplicateClass, RelBuilderFactory relBuilderFactory) {
		super(
				operandJ(joinClass, null, join -> (join.getJoinType() == JoinRelType.CLEAN), 

						operand(deduplicateClass, any()), new RelOptRuleOperand[] { operand(deduplicateClass, any()) }), relBuilderFactory, null);
	}

	protected DirtyJoinDeduplicateRemoveRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory) {
		super(operand, relBuilderFactory, null);
	}

	public DirtyJoinDeduplicateRemoveRule(RelOptRuleOperand operand, String description, RelBuilderFactory relBuilderFactory) {
		super(operand, relBuilderFactory, description);
	}

	public DirtyJoinDeduplicateRemoveRule(RelOptRuleOperand operand, String description) {
		this(operand, description, RelFactories.LOGICAL_BUILDER);
	}

	public void onMatch(RelOptRuleCall call) {
		Join join = (Join)call.rel(0);
		if (join.isDirtyJoin())
			return; 
		Deduplicate deduplicateLeft = (Deduplicate)call.rel(1);
		Deduplicate deduplicateRight = (Deduplicate)call.rel(2);
		RelNode newJoin = null;
		List<String> tableLeft = deduplicateLeft.getRelTable().getQualifiedName();
		String leftTableName = "";
		List<String> tableRight = deduplicateRight.getRelTable().getQualifiedName();
		String rightTableName = "";
		if (tableLeft.size() > 0)
			leftTableName = tableLeft.get(1); 
		if (tableRight.size() > 0)
			rightTableName = tableRight.get(1); 
		List<RexNode> leftConjuctions = deduplicateLeft.getConjuctions();
		List<RexNode> rightConjuctions = deduplicateRight.getConjuctions();
		
//// TEST 		
//
//		newJoin = LogicalDeduplicateJoin.create((RelNode)deduplicateLeft, deduplicateRight.getInput(0), join.getCondition(), join
//				.getVariablesSet(), JoinRelType.DIRTY_RIGHT, deduplicateLeft.getSource(), deduplicateRight.getSource(),
//				deduplicateLeft.getFieldTypes(), deduplicateRight.getFieldTypes(), deduplicateLeft.getKey(), deduplicateRight.getKey(), leftTableName, rightTableName, 
//				Integer.valueOf(deduplicateLeft.getFieldTypes().size()), Integer.valueOf(deduplicateRight.getFieldTypes().size()), Boolean.valueOf(true));
//	

		if (leftConjuctions == null && rightConjuctions != null) {
			newJoin = LogicalDeduplicateJoin.create(deduplicateLeft.getInput(0), (RelNode)deduplicateRight, join.getCondition(), join
					.getVariablesSet(), JoinRelType.DIRTY_LEFT, deduplicateLeft.getSource(), deduplicateRight.getSource(), 
					deduplicateLeft.getFieldTypes(), deduplicateRight.getFieldTypes(), deduplicateLeft.getKey(), deduplicateRight.getKey(), leftTableName, rightTableName, 
					Integer.valueOf(deduplicateLeft.getFieldTypes().size()), Integer.valueOf(deduplicateRight.getFieldTypes().size()), Boolean.valueOf(true));
		} 
		else if (leftConjuctions != null && rightConjuctions == null) {
			newJoin = LogicalDeduplicateJoin.create((RelNode)deduplicateLeft, deduplicateRight.getInput(0), join.getCondition(), join
					.getVariablesSet(), JoinRelType.DIRTY_RIGHT,deduplicateLeft.getSource(), deduplicateRight.getSource(), 
					deduplicateLeft.getFieldTypes(), deduplicateRight.getFieldTypes(), deduplicateLeft.getKey(), deduplicateRight.getKey(),  leftTableName, rightTableName, 
					Integer.valueOf(deduplicateLeft.getFieldTypes().size()), Integer.valueOf(deduplicateRight.getFieldTypes().size()), Boolean.valueOf(true));
		} 
		else {
			Double leftComps = deduplicateLeft.calculateComparisons();
			Double rightComps = deduplicateRight.calculateComparisons();
			if (leftComps != null && rightComps != null)
				if (leftComps.doubleValue() > rightComps.doubleValue()) {
					newJoin = LogicalDeduplicateJoin.create(deduplicateLeft.getInput(0), (RelNode)deduplicateRight, join.getCondition(), join
							.getVariablesSet(), JoinRelType.DIRTY_LEFT, deduplicateLeft.getSource(), deduplicateRight.getSource(), 
							deduplicateLeft.getFieldTypes(), deduplicateRight.getFieldTypes(), deduplicateLeft.getKey(), deduplicateRight.getKey(), leftTableName, rightTableName, 
							Integer.valueOf(deduplicateLeft.getFieldTypes().size()), Integer.valueOf(deduplicateRight.getFieldTypes().size()), Boolean.valueOf(true));
				}
				else {
					newJoin = LogicalDeduplicateJoin.create((RelNode)deduplicateLeft, deduplicateRight.getInput(0), join.getCondition(), join
							.getVariablesSet(), JoinRelType.DIRTY_RIGHT, deduplicateLeft.getSource(), deduplicateRight.getSource(), 
							deduplicateLeft.getFieldTypes(), deduplicateRight.getFieldTypes(), deduplicateLeft.getKey(), deduplicateRight.getKey(), leftTableName, rightTableName, 
							Integer.valueOf(deduplicateLeft.getFieldTypes().size()), Integer.valueOf(deduplicateRight.getFieldTypes().size()), Boolean.valueOf(true));
				}  
		} 
		if (newJoin != null)
			call.transformTo(newJoin); 
	}

}
