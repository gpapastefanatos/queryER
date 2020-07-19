package org.imis.er.BlockIndex;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Set;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.imis.calcite.adapter.csv.CsvEnumerator;
import org.imis.calcite.adapter.csv.CsvTableScan;
import org.imis.calcite.rel.logical.LogicalBlockIndexScan;
import org.imis.calcite.util.DeduplicationExecution;
import org.imis.er.DataStructures.AbstractBlock;
import org.imis.er.DataStructures.Attribute;
import org.imis.er.DataStructures.EntityProfile;
import org.imis.er.DataStructures.UnilateralBlock;
import org.imis.er.Utilities.Converter;
import org.imis.er.Utilities.SerializationUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class BlockIndex extends AbstractTable 
implements  TranslatableTable {
	protected static final Logger DEDUPLICATION_EXEC_LOGGER =  LoggerFactory.getLogger(DeduplicationExecution.class);

	public List<EntityProfile> entityProfiles;
	protected Map<String, Set<Integer>> invertedIndex;
	protected BlockIndexStatistic blockIndexStatistic;
	protected Map<String, Integer> tfIdf;
	protected Set<Integer> joinedIds;

	public BlockIndex() {
		this.entityProfiles = new ArrayList<EntityProfile>();
		this.invertedIndex = new HashMap<String, Set<Integer>>();
		this.blockIndexStatistic = new BlockIndexStatistic();
	}

	public BlockIndex(String path) {
		this.invertedIndex = (Map<String, Set<Integer>>) SerializationUtilities.loadSerializedObject(path);
		ObjectMapper objectMapper = new ObjectMapper();
	}

	public void buildQueryBlocks() {
		this.invertedIndex = indexEntities(0, this.entityProfiles);
	}


	@SuppressWarnings("unchecked")
	protected Map<String, Set<Integer>> indexEntities(int sourceId, List<EntityProfile> profiles) {
		invertedIndex = new HashMap<String, Set<Integer>>();
		HashSet<String> stopwords = (HashSet<String>) SerializationUtilities
				.loadSerializedObject("C://Works/ATHENA/Projects/ER/Code/DBLP-SCHOLAR/stopwords_SER");
		HashMap<String, Integer> tfIdf = new HashMap<>();
		for (EntityProfile profile : profiles) {
			for (Attribute attribute : profile.getAttributes()) {
				if (attribute.getValue() == null)
					continue;
				String cleanValue = attribute.getValue().replaceAll("_", " ").trim().replaceAll("\\s*,\\s*$", "")
						.toLowerCase();
				for (String token : cleanValue.split("[\\W_]")) {
					if (2 < token.trim().length()) {
						if (stopwords.contains(token.toLowerCase()))
							continue;
						Set<Integer> termEntities = invertedIndex.computeIfAbsent(token.trim(),
								x -> new HashSet<Integer>());

						termEntities.add(Integer.parseInt(profile.getEntityUrl()));
						int tokenCount = tfIdf.containsKey(token) ? tfIdf.get(token) : 0;
						tfIdf.put(token, tokenCount + 1);
					}
				}
			}

		}
		this.setTfIdf(tfIdf);
		//System.out.println("Query Block Index size: " + invertedIndex.size());
		return invertedIndex;
	}

	public void storeBlockIndex(String path, String tableName) {
		SerializationUtilities.storeSerializedObject(this.invertedIndex, path + tableName + "InvertedIndex" );
	}
	
	public void loadBlockIndex(String path, String tableName) {
		this.invertedIndex = (Map<String, Set<Integer>>) SerializationUtilities.loadSerializedObject(path + tableName + "InvertedIndex" );
	}
	

	public static List<AbstractBlock> parseIndex(Map<String, Set<Integer>> invertedIndex) {
		final List<AbstractBlock> blocks = new ArrayList<AbstractBlock>();
		for (Entry<String, Set<Integer>> term : invertedIndex.entrySet()) {
			if (1 < term.getValue().size()) {
				int[] idsArray = Converter.convertListToArray(term.getValue());
				UnilateralBlock uBlock = new UnilateralBlock(idsArray);
				blocks.add(uBlock);
			}
		}
		invertedIndex.clear();
		return blocks;
	}
	
	public Map<String, Set<Integer>> getInvertedIndex() {
		return invertedIndex;
	}

	public void setInvertedIndex(Map<String, Set<Integer>> invertedIndex) {
		this.invertedIndex = invertedIndex;
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		final List<RelDataType> types = new ArrayList<>();
		final List<String> names = new ArrayList<>();
		if (names.isEmpty()) {
			names.add("line");
			types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
		}
		return typeFactory.createStructType(Pair.zip(names, types));
	}

	public BlockIndexStatistic getBlockIndexStatistic() {
		return blockIndexStatistic;
	}

	public void setBlockIndexStatistic(BlockIndexStatistic blockIndexStatistic) {
		this.blockIndexStatistic = blockIndexStatistic;
	}

	@Override
	public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
		return LogicalBlockIndexScan.create(context.getCluster(), relOptTable);
	}

	public Map<String, Integer> getTfIdf() {
		return tfIdf;
	}
	public void setTfIdf(Map<String, Integer> tfIdf) {
		this.tfIdf = tfIdf;
	}
	
	
}
