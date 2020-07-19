package org.imis.calcite.rel.core;


import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.imis.er.BlockIndex.BlockIndex;

public class BlockIndexScan extends AbstractRelNode {
	
	protected final RelOptTable relBlockIndex;

	public BlockIndexScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable relBlockIndex) {
		super(cluster, traitSet);
		// TODO Auto-generated constructor stub
		this.relBlockIndex = relBlockIndex;
	}
	
	
	@Override public RelDataType deriveRowType() {
		return relBlockIndex.getRowType();
	}
}
