package org.imis.calcite.rel.logical;

import java.util.Set;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelNodes;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

/**
 * @author bstam
 * The DeduplicateJoin relational operator gets as input an whatever the regular join operator gets
 * but it also contains info for potentially deduplicating the two tables below it. 
 * 
 */
public class LogicalDeduplicateJoin extends Join {



	protected LogicalDeduplicateJoin(RelOptCluster cluster,
			RelTraitSet traits,
			RelNode left,
			RelNode right,
			RexNode condition,
			Set<CorrelationId> variablesSet,
			JoinRelType joinType,
			Integer keyLeft,
			Integer keyRight,
			String tableNameLeft,
			String tableNameRight,
			Integer fieldLeft,
			Integer fieldRight,
			Boolean isDirtyJoin) {
		super(
				cluster,
				traits,
				ImmutableList.of(),
				left,
				right,
				condition,
				variablesSet,
				joinType
				);
		this.setKeyLeft(keyLeft);
		this.setKeyRight(keyRight);
		this.setTableNameLeft(tableNameLeft);
		this.setTableNameRight(tableNameRight);
		this.setFieldLeft(fieldLeft);
		this.setFieldRight(fieldRight);
		this.setDirtyJoin(isDirtyJoin);

	}

	



	public static LogicalDeduplicateJoin create(
			RelNode left,
			RelNode right,
			RexNode condition,
			Set<CorrelationId> variablesSet,
			JoinRelType joinType,
			Integer keyLeft,
			Integer keyRight,
			String tableNameLeft,
			String tableNameRight,
			Integer fieldLeft,
			Integer fieldRight,
			Boolean isDirtyJoin) {
		final RelOptCluster cluster = left.getCluster();
		final RelMetadataQuery mq = cluster.getMetadataQuery();
		final RelTraitSet traitSet =
				cluster.traitSetOf(Convention.NONE)
				.replaceIfs(RelCollationTraitDef.INSTANCE,
						() -> RelMdCollation.enumerableHashJoin(mq, left, right, joinType));
		return new LogicalDeduplicateJoin(cluster, traitSet, left, right, condition,
				variablesSet, joinType, keyLeft, keyRight,  tableNameLeft, tableNameRight,
				fieldLeft, fieldRight, isDirtyJoin);
	}


	@Override
	public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType,
			boolean semiJoinDone) {
		// TODO Auto-generated method stub
		return new LogicalDeduplicateJoin(getCluster(), traitSet, left, right,
				condition, variablesSet, joinType, getKeyLeft(), getKeyRight(), getTableNameLeft(), getTableNameRight(),
				getFieldLeft(), getFieldRight(), isDirtyJoin());
	}
	
	@Override public RelOptCost computeSelfCost(RelOptPlanner planner,
			RelMetadataQuery mq) {
		double rowCount = mq.getRowCount(this);

		// Joins can be flipped, and for many algorithms, both versions are viable
		// and have the same cost. To make the results stable between versions of
		// the planner, make one of the versions slightly more expensive.
		switch (joinType) {
			case CLEAN:
				if (RelNodes.COMPARATOR.compare(left, right) > 0) {
					rowCount = RelMdUtil.addEpsilon(rowCount);
				}
			case ANTI:
				// SEMI and ANTI join cannot be flipped
				break;
			case RIGHT:
				rowCount = RelMdUtil.addEpsilon(rowCount);
				break;
			default:
				if (RelNodes.COMPARATOR.compare(left, right) > 0) {
					rowCount = RelMdUtil.addEpsilon(rowCount);
				}
		}

		// Cheaper if the smaller number of rows is coming from the LHS.
		// Model this by adding L log L to the cost.
		final double rightRowCount = right.estimateRowCount(mq);
		final double leftRowCount = left.estimateRowCount(mq);
		if (Double.isInfinite(leftRowCount)) {
			rowCount = leftRowCount;
		} else {
			rowCount += Util.nLogN(leftRowCount);
		}
		if (Double.isInfinite(rightRowCount)) {
			rowCount = rightRowCount;
		} else {
			rowCount += rightRowCount;
		}

		if (isSemiJoin()) {
			return planner.getCostFactory().makeCost(0, 0, 0).multiplyBy(.1d);
		} else {
			return planner.getCostFactory().makeCost(0, 0, 0);
		}
	}


	@Override
	public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType,
			boolean semiJoinDone, Integer keyLeft, Integer keyRight, String tableNameLeft, String tableNameRight,
			Integer fieldLeft, Integer fieldRight, Boolean isDirtyJoin) {
		// TODO Auto-generated method stub
		return new LogicalDeduplicateJoin(getCluster(), traitSet, left, right,
				condition, variablesSet, joinType, keyLeft, keyRight, tableNameLeft, tableNameRight,
				fieldLeft, fieldRight, isDirtyJoin);
	}

	
}
