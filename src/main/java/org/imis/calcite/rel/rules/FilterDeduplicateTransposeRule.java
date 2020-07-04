package org.imis.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.tools.RelBuilderFactory;
import org.imis.calcite.rel.core.Deduplicate;
import org.imis.calcite.rel.logical.LogicalDeduplicate;


/**
 * 
 * @author bstam
 * Rule for transposing a filter and a deduplicate rel opt. 
 * This is to push the filter down and deduplicate a subset of the data.
 */
public class FilterDeduplicateTransposeRule extends RelOptRule {

	public static final FilterDeduplicateTransposeRule INSTANCE =
			new FilterDeduplicateTransposeRule(LogicalFilter.class, LogicalDeduplicate.class,
					RelFactories.LOGICAL_BUILDER);


	public FilterDeduplicateTransposeRule(
			Class<? extends Filter> filterClass,
			Class<? extends Deduplicate> deduplicateClass,

			RelBuilderFactory relBuilderFactory) {

		this(
				operand(
						filterClass,
						operand(deduplicateClass, any())),
				relBuilderFactory);
	}

	protected FilterDeduplicateTransposeRule(RelOptRuleOperand operand,
			RelBuilderFactory relBuilderFactory) {
		super(operand, relBuilderFactory, null);
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		// TODO Auto-generated method stub
		final Filter filter = call.rel(0);
		final Deduplicate deduplicate = call.rel(1);
		RelNode newFilterRel = filter.copy(filter.getTraitSet(), deduplicate.getInput(), filter.getCondition()); // change input of filter with dedups
		RelNode newDedupRel = deduplicate.copy(deduplicate.getTraitSet(), newFilterRel);

		call.transformTo(newDedupRel);

	}
}
