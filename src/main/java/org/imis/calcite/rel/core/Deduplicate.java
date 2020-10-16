package org.imis.calcite.rel.core;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Source;
import org.imis.calcite.adapter.csv.CsvFieldType;
import org.imis.er.Comparators.BlockCardinalityComparator;
import org.imis.er.DataStructures.AbstractBlock;
import org.imis.er.Utilities.SerializationUtilities;
import org.imis.er.Utilities.TokenStatistics;

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
	protected final List<RexNode> conjuctions;
	protected final RelOptTable blockIndex;
	protected Double comparisons;
	
	protected Deduplicate(
			RelOptCluster cluster,
			RelTraitSet traitSet,
			RelNode input,
			RelOptTable table,
			RelOptTable blockIndex,
			List<RexNode> conjuctions,
			Integer key,
			Source source,
			List<CsvFieldType> fieldTypes,
			Double comparisons){
		super(cluster, traitSet, input);
		this.table = table;
		this.blockIndex = blockIndex;
		this.key = key;
		this.source = source;
		this.conjuctions = conjuctions;
		this.fieldTypes = fieldTypes;
		this.comparisons = comparisons;
		if (table.getRelOptSchema() != null) {
			cluster.getPlanner().registerSchema(table.getRelOptSchema());
		}
	}

	@Override public RelOptCost computeSelfCost(RelOptPlanner planner,
			RelMetadataQuery mq) {
		RelOptCost cost = null;
		
		if(comparisons == null) comparisons = blockIndex.getComparisons(conjuctions);
		else {
			double comps = blockIndex.getComparisons(conjuctions);
			if(comps < comparisons) comparisons = comps;
		}
		cost =  super.computeSelfCost(planner, mq)
				.multiplyBy((fieldTypes.size() + 2D)
						/ (table.getRowType().getFieldCount() + 2D));
		return cost;
	}
	
	
	@Override public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return copy(traitSet, inputs.get(0));
	}

	public abstract RelNode copy(RelTraitSet traitSet, RelNode relNode);

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
	
	public List<RexNode> getConjuctions() {
		return this.conjuctions;
	}
	
	public Double getComparisons() {
		return comparisons;
	}

	public RelOptTable getBlockIndex() {
		return blockIndex;
	}
}
