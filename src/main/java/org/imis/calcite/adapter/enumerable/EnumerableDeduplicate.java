/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.imis.calcite.adapter.enumerable;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
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
import org.imis.calcite.util.NewBuiltInMethod;


/**
 * @author bstam
 * Implementation of {@link org.apache.calcite.rel.core.Deduplicate} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}.
 * Physical plan implementation of the Deduplicate relational operator.
 */
public class EnumerableDeduplicate extends Deduplicate implements EnumerableRel {
	/**
	 * Creates an EnumerableDeduplicate.
	 *
	 * <p>Use {@link #create} unless you know what you're doing.
	 *
	 * @param cluster  Cluster this relational expression belongs to
	 * @param traitSet Traits of this relational expression
	 * @param input    Input relational expression
	 * @param rowType  Output row type
	 */

	protected EnumerableDeduplicate(
			RelOptCluster cluster,
			RelTraitSet traitSet,
			RelNode input,
			RelOptTable table,
			Integer key,
			Source source,
			List<CsvFieldType> fieldTypes){
		super(cluster, traitSet,  input, table, key, source, fieldTypes);
		this.traitSet =
				cluster.traitSet().replace(EnumerableConvention.INSTANCE);
	}


	public static RelNode create( RelNode input, RelOptTable table, Integer key,
			Source source, List<CsvFieldType> fieldTypes) {
		// TODO Auto-generated method stub
		final RelOptCluster cluster = input.getCluster();
		final RelMetadataQuery mq = cluster.getMetadataQuery();
		final RelTraitSet traitSet =
				cluster.traitSet().replace(EnumerableConvention.INSTANCE);
		return new EnumerableDeduplicate(cluster, traitSet, input, table, key, source, fieldTypes);
	}


	@Override public  EnumerableDeduplicate copy(RelTraitSet traitSet,  RelNode input){
		return new EnumerableDeduplicate(getCluster(), traitSet, input, this.table, this.key, this.source, this.fieldTypes);
	}

	@Override public RelOptCost computeSelfCost(RelOptPlanner planner,
			RelMetadataQuery mq) {
		double rowCount = mq.getRowCount(this);
		return planner.getCostFactory().makeCost(rowCount, 0, 0);
	}
	/**
	 * Calls the java function that implements the deduplication
	 * For inputs we get the tableName, source, key and fieldTypes
	 * as directed by the LogicalPlan and the parsing.
	 */
	@Override
	public Result implement(EnumerableRelImplementor implementor, Prefer pref) {

		final BlockBuilder builder = new BlockBuilder();

		final EnumerableRel child = (EnumerableRel) getInput();

		final Result result =
				implementor.visitChild(this, 0, child, pref);

		final PhysType physType = result.physType;
		final Expression inputEnumerable =
				builder.append(
						"inputEnumerable", result.block, false);

		String schemaName = "";
		String tableName = "";

		if(table.getQualifiedName().size() > 0) {
			schemaName = table.getQualifiedName().get(0);
			tableName = table.getQualifiedName().get(1);
		}
		AtomicBoolean ab = new AtomicBoolean();
		ab.set(false);
		builder.add(Expressions.return_(null, Expressions.call(
				NewBuiltInMethod.DEDUPLICATE_ENUM.method,
				inputEnumerable,
				Expressions.constant(tableName),
				Expressions.constant(this.key),
				Expressions.constant(this.source.toString()),
				Expressions.constant(this.fieldTypes),
				Expressions.constant(ab)
				)));

		return implementor.result(physType, builder.toBlock());

	}
}


