package org.imis.calcite.rel.logical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Source;
import org.imis.calcite.adapter.csv.CsvFieldType;
import org.imis.calcite.rel.core.Deduplicate;

/**
 * @author bstam
 * The Deduplicate relational operator apart from the filtered data
 * gets as input a table along with its key, source and fieldTypes.
 * These are the exact inputs of the Scan relational operator and that is because
 * the Deduplication process potentially requires an extra scan during the resolution phase.
 */
public class LogicalDeduplicate extends Deduplicate {


	protected LogicalDeduplicate(
			RelOptCluster cluster,
			RelTraitSet traitSet,
			RelNode input,
			RelNode blockInput,
			RelOptTable table,
			Integer key,
			Source source,
			List<CsvFieldType> fieldTypes){
		super(cluster, traitSet,  input, blockInput, table,  key, source, fieldTypes);
	}


	public static RelNode create(
			RelOptCluster cluster, RelTraitSet traitSet,
			RelNode input, RelNode blockInput, RelOptTable table, Integer key,  Source source,
			List<CsvFieldType> fieldTypes) {
		// TODO Auto-generated method stub
		return new LogicalDeduplicate(cluster, traitSet, input, blockInput, table, key, source, fieldTypes);
	}


	@Override public RelOptCost computeSelfCost(RelOptPlanner planner,
			RelMetadataQuery mq) {
		
		RelOptCost cost =  super.computeSelfCost(planner, mq)
				.multiplyBy(this.getFieldTypes().size() * this.getRows());
		return cost;
	}


	@Override
	public RelNode copy(RelTraitSet traitSet, RelNode input, RelNode blockInput) {
		return new LogicalDeduplicate(getCluster(), traitSet, input ,blockInput,
				this.table, this.key, this.source, this.fieldTypes);

	}


}
