package org.imis.calcite.rel.core;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.util.Source;
import org.imis.calcite.adapter.csv.CsvFieldType;

/**
 * 
 * @author bstam
 * This is the base Class of all Deduplicate relational operators physical or logical.
 * For calcite we need to first create a base class that extends a RelNode class and then
 * extend this class with whatever we want.
 */
public abstract class Deduplicate extends SingleRel {


	protected final RelOptTable table;
	protected final Integer key;
	protected final Source source;
	protected final List<CsvFieldType> fieldTypes;


	protected Deduplicate(
			RelOptCluster cluster,
			RelTraitSet traitSet,
			RelNode input,
			RelOptTable table,
			Integer key,
			Source source,
			List<CsvFieldType> fieldTypes){
		super(cluster, traitSet, input);
		this.table = table;
		this.key = key;
		this.source = source;
		this.fieldTypes = fieldTypes;
		if (table.getRelOptSchema() != null) {
			cluster.getPlanner().registerSchema(table.getRelOptSchema());
		}

	}




	@Override public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return copy(traitSet, sole(inputs));
	}

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

	public abstract Deduplicate copy(RelTraitSet traitSet,  RelNode input);




}
