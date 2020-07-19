package org.imis.calcite.rel.logical;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.imis.calcite.rel.core.BlockIndexScan;
import org.imis.er.BlockIndex.BlockIndex;

public class LogicalBlockIndexScan extends BlockIndexScan implements EnumerableRel{

	
	public LogicalBlockIndexScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable relBlockIndex) {
		super(cluster, traitSet, relBlockIndex);
	}

	public static RelNode create(RelOptCluster cluster, RelOptTable relBlockIndex) {
//		final RelTraitSet traitSet =
//				cluster.traitSetOf(Convention.NONE)
//				.replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
//					if (table != null) {
//						return table.getStatistic().getCollations();
//					}
//					return ImmutableList.of();
//		});
		final RelTraitSet traitSet =
				cluster.traitSet().replace(EnumerableConvention.INSTANCE);
		return new LogicalBlockIndexScan(cluster, traitSet, relBlockIndex);
		
	}

	@Override
	public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
		// TODO Auto-generated method stub
		PhysType physType =
				PhysTypeImpl.of(
						implementor.getTypeFactory(),
						getRowType(),
						pref.preferArray());
		
		BlockStatement blockStatement;
		
		blockStatement	= Blocks.toBlock(
				Expressions.return_(null,
						Expressions.constant(null)));
		return implementor.result( physType, blockStatement);
	}

}
