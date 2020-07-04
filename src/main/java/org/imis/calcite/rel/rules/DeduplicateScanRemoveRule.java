package org.imis.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.tools.RelBuilderFactory;
import org.imis.calcite.rel.core.Deduplicate;
import org.imis.calcite.rel.logical.LogicalDeduplicate;

public class DeduplicateScanRemoveRule extends RelOptRule{
	
	public static final DeduplicateScanRemoveRule INSTANCE =
			new DeduplicateScanRemoveRule(LogicalDeduplicate.class, TableScan.class,
					RelFactories.LOGICAL_BUILDER);


	public DeduplicateScanRemoveRule(
			Class<? extends Deduplicate> deduplicateClass,
			Class<? extends TableScan> scanClass,

			RelBuilderFactory relBuilderFactory) {

		this(
				operand(
						deduplicateClass,
						operand(scanClass, any())),
				relBuilderFactory);
	}

	protected DeduplicateScanRemoveRule(RelOptRuleOperand operand,
			RelBuilderFactory relBuilderFactory) {
		super(operand, relBuilderFactory, null);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		// TODO Auto-generated method stub
		
		call.transformTo(call.rel(1));

	}
}
