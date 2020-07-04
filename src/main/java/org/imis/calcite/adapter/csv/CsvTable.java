/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.imis.calcite.adapter.csv;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Source;

/**
 * Base class for table that reads CSV files.
 */
public abstract class CsvTable extends AbstractTable {
	protected final Source source;
	protected final RelProtoDataType protoRowType;
	protected List<CsvFieldType> fieldTypes;
	protected RelDataType fullTypes;
	protected int tableKey;
	protected String tableName;
	protected Double rows;
	/** Creates a CsvTable. */
	CsvTable(Source source, String name, RelProtoDataType protoRowType) {
		this.source = source;
		this.tableName = name;
		this.protoRowType = protoRowType;

	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		if (protoRowType != null) {
			return protoRowType.apply(typeFactory);
		}
		if (fieldTypes == null) {
			fieldTypes = new ArrayList<>();
			fullTypes =	CsvEnumerator.deduceRowType((JavaTypeFactory) typeFactory, source,
					fieldTypes);
			return fullTypes;
		} else {
			fullTypes = CsvEnumerator.deduceRowType((JavaTypeFactory) typeFactory, source,
					null);
			return fullTypes;
		}
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

//
//	@Override  public Statistic getStatistic() {
//		return new Statistic() {
//
//			@Override
//			public Double getRowCount() {
//				if(rows == null)
//					rows = CsvEnumerator.estimateRowCount(source, fieldTypes);
//				return rows;
//			}
//
//			@Override
//			public boolean isKey(ImmutableBitSet columns) {
//				return false;
//			}
//
//			@Override
//			public List<ImmutableBitSet> getKeys() {
//				return ImmutableList.of();
//			}
//
//			@Override
//			public List<RelReferentialConstraint> getReferentialConstraints() {
//				return ImmutableList.of();
//			}
//
//			@Override
//			public List<RelCollation> getCollations() {
//				return ImmutableList.of();
//			}
//
//			@Override
//			public RelDistribution getDistribution() {
//				return RelDistributionTraitDef.INSTANCE.getDefault();
//			}
//		};
//	}


	public Source getSource() {
		return source;
	}

	public List<CsvFieldType> getFieldTypes() {
		return fieldTypes;
	}

	public void setFieldTypes(List<CsvFieldType> fieldTypes) {
		this.fieldTypes = fieldTypes;
	}
	/** Various degrees of table "intelligence". */
	public enum Flavor {
		TRANSLATABLE
	}
}

// End CsvTable.java
