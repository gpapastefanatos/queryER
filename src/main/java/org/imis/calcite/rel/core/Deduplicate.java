package org.imis.calcite.rel.core;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Source;
import org.imis.calcite.adapter.csv.CsvFieldType;

/**
 * 
 * @author bstam
 * This is the base Class of all Deduplicate relational operators physical or logical.
 * For calcite we need to first create a base class that extends a RelNode class and then
 * extend this class with whatever we want.
 */
public abstract class Deduplicate extends BiRel {


	protected final RelOptTable table;
	protected final Integer key;
	protected final Source source;
	protected final List<CsvFieldType> fieldTypes;
	


	protected Deduplicate(
			RelOptCluster cluster,
			RelTraitSet traitSet,
			RelNode input,
			RelNode blockInput,
			RelOptTable table,
			Integer key,
			Source source,
			List<CsvFieldType> fieldTypes){
		super(cluster, traitSet, input, blockInput);
		this.table = table;
		this.key = key;
		this.source = source;
		
		this.fieldTypes = fieldTypes;
		if (table.getRelOptSchema() != null) {
			cluster.getPlanner().registerSchema(table.getRelOptSchema());
		}

	}



	@Override public RelOptCost computeSelfCost(RelOptPlanner planner,
			RelMetadataQuery mq) {
		// Multiply the cost by a factor that makes a scan more attractive if it
		// has significantly fewer fields than the original scan.
		//
		// The "+ 2D" on top and bottom keeps the function fairly smooth.
		//
		// For example, if table has 3 fields, project has 1 field,
		// then factor = (1 + 2) / (3 + 2) = 0.6
		//System.out.println("Computing cost");
		RelOptCost cost =  super.computeSelfCost(planner, mq)
				.multiplyBy((fieldTypes.size() + 2D)
						/ (table.getRowType().getFieldCount() + 2D));
		final ImmutableBitSet groupSet =
				ImmutableBitSet.range(this.getRowType().getFieldCount());
		
		return cost;
	}

	@Override public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return copy(traitSet, inputs.get(0), inputs.get(1));
	}

	public abstract RelNode copy(RelTraitSet traitSet, RelNode relNode, RelNode relNode2);

	@Override public RelWriter explainTerms(RelWriter pw) {
		return super.explainTerms(pw)
				.item("tables", table.getQualifiedName()).item("key", this.key);
	}

	public RelOptTable getRelTable() {
		return this.table;
	}


	public Integer getKey() {
		return this.key;
	}


	public Source getSource() {
		return source;
	}

	public List<CsvFieldType> getFieldTypes() {
		return fieldTypes;
	}
	
	@Override public RelDataType deriveRowType() {
		return this.left.getRowType();
	}
	
	@Override public double estimateRowCount(RelMetadataQuery mq) {
		return left.estimateRowCount(mq);
	}
	
	



}
