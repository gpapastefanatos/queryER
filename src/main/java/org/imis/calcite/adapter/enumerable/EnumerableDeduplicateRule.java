package org.imis.calcite.adapter.enumerable;

import java.util.function.Predicate;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.imis.calcite.rel.logical.LogicalDeduplicate;

/**
 * 
 * @author bstam
 * The rule that converts a LogicalDeduplicate to an EnumerableDeduplicate for the 
 * physical plan implementation.
 */
public class EnumerableDeduplicateRule extends ConverterRule {

	public EnumerableDeduplicateRule() {
		super(LogicalDeduplicate.class,
				(Predicate<RelNode>) r -> true,
				Convention.NONE, EnumerableConvention.INSTANCE,
				RelFactories.LOGICAL_BUILDER, "EnumerableDeduplicateRule");
	}

	@Override
	public RelNode convert(RelNode rel) {
		final LogicalDeduplicate deduplicate = (LogicalDeduplicate) rel;
		return EnumerableDeduplicate.create(
				convert(deduplicate.getInput(),
						deduplicate.getInput().getTraitSet()
						.replace(EnumerableConvention.INSTANCE)), deduplicate.getRelTable(),
				deduplicate.getKey(), deduplicate.getSource(), deduplicate.getFieldTypes());
	}
}
