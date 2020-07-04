package org.imis.calcite.adapter.enumerable;

import java.util.function.Predicate;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.imis.calcite.rel.logical.LogicalMergeEntities;

/**
 * 
 * @author bstam
 * The rule that converts a LogicalMergeEntities to an EnumerableMergeEntitiesRule for the 
 * physical plan implementation.
 */
public class EnumerableMergeEntitiesRule extends ConverterRule {

	public EnumerableMergeEntitiesRule() {
		super(LogicalMergeEntities.class,
				(Predicate<RelNode>) r -> true,
				Convention.NONE, EnumerableConvention.INSTANCE,
				RelFactories.LOGICAL_BUILDER, "EnumerableMergeEntitiesRule");
	}

	@Override
	public RelNode convert(RelNode rel) {
		final LogicalMergeEntities mergeEntities = (LogicalMergeEntities) rel;
		return EnumerableMergeEntities.create(
				convert(mergeEntities.getInput(),
						mergeEntities.getInput().getTraitSet()
						.replace(EnumerableConvention.INSTANCE)));
	}
}
