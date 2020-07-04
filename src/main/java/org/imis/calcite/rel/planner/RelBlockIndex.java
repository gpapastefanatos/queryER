
package org.imis.calcite.rel.planner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.imis.calcite.adapter.csv.CsvFieldType;
import org.imis.er.Utilities.SerializationUtilities;

import com.google.common.collect.ImmutableList;



public class RelBlockIndex extends AbstractTable {
	
	protected List<CsvFieldType> fieldTypes;
	protected int tableKey;
	protected String tableName;
	protected final Map<String, Set<Integer>> bBlocks;
	protected Double size;
	
	/** Creates a RelBlockIndex. */
	public RelBlockIndex(String name,  int tableKey, List<CsvFieldType> fieldTypes) {
		this.tableName = name;
		this.tableKey = tableKey;
		this.fieldTypes = fieldTypes;
		this.bBlocks = new HashMap<String, Set<Integer>>();
//		this.bBlocks = (Map<String, Set<Integer>>) SerializationUtilities
//				.loadSerializedObject("./data/blockIndex/" + tableName + "InvertedIndex");
	}

	public String getName() {
		return this.tableName;
	}
	public void setKey(int keyFieldIndex) {
		this.tableKey = keyFieldIndex;
	}

	public int getKey() {
		return this.tableKey;
	}

	public List<CsvFieldType> getFieldTypes() {
		return fieldTypes;
	}

	public void setFieldTypes(List<CsvFieldType> fieldTypes) {
		this.fieldTypes = fieldTypes;
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override  public Statistic getStatistic() {
		return new Statistic() {

			@Override
			public Double getRowCount() {
				if(size == null)
					size = (double) bBlocks.size();
				System.out.println("Stats" + " " + size);
				return size;
			}

			@Override
			public boolean isKey(ImmutableBitSet columns) {
				return false;
			}

			@Override
			public List<ImmutableBitSet> getKeys() {
				return ImmutableList.of();
			}

			@Override
			public List<RelReferentialConstraint> getReferentialConstraints() {
				return ImmutableList.of();
			}

			@Override
			public List<RelCollation> getCollations() {
				return ImmutableList.of();
			}

			@Override
			public RelDistribution getDistribution() {
				return RelDistributionTraitDef.INSTANCE.getDefault();
			}
		};
	}
	
}

// End CsvTable.java
