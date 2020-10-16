package org.imis.er.BlockIndex;


import static java.util.stream.Collectors.toMap;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.Set;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.imis.calcite.adapter.csv.CsvEnumerator;
import org.imis.calcite.adapter.csv.CsvTableScan;
import org.imis.calcite.rel.logical.LogicalBlockIndexScan;
import org.imis.calcite.util.DeduplicationExecution;
import org.imis.er.BlockBuilding.AbstractIndexBasedMethod;
import org.imis.er.BlockBuilding.ExtendedCanopyClustering;
import org.imis.er.DataStructures.AbstractBlock;
import org.imis.er.DataStructures.Attribute;
import org.imis.er.DataStructures.EntityProfile;
import org.imis.er.DataStructures.UnilateralBlock;
import org.imis.er.Utilities.Converter;
import org.imis.er.Utilities.EquiFreqBinning;
import org.imis.er.Utilities.EquiWidthBinning;
import org.imis.er.Utilities.SerializationUtilities;
import org.imis.er.Utilities.TokenStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;

public class BlockIndex extends AbstractTable 
implements  TranslatableTable {
	protected static final Logger DEDUPLICATION_EXEC_LOGGER =  LoggerFactory.getLogger(DeduplicationExecution.class);

	public List<EntityProfile> entityProfiles;
	protected Map<String, Set<Integer>> invertedIndex;
	protected BlockIndexStatistic blockIndexStatistic;
	protected Map<String, Integer> tfIdf;
	protected Set<Integer> joinedIds;
	protected Map<Integer, Set<String>> entitiesToBlocks;
	protected static ExtendedCanopyClustering eCC;
	
	public BlockIndex() {
		this.entityProfiles = new ArrayList<EntityProfile>();
		this.invertedIndex = new HashMap<String, Set<Integer>>();
		this.blockIndexStatistic = new BlockIndexStatistic();
	}

	public BlockIndex(String path) {
		this.invertedIndex = (Map<String, Set<Integer>>) SerializationUtilities.loadSerializedObject(path);
	}
	
	public void buildBlocks() {
		this.invertedIndex = indexEntities(0, entityProfiles);
		createEntitiesToBlocks();
	}
	
	public static List<AbstractBlock> parseIndex(Map<String, Set<Integer>> invertedIndex) {
		List<AbstractBlock> blocks = new ArrayList<AbstractBlock>();
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
	
	@SuppressWarnings("unchecked")
	protected Map<String, Set<Integer>> indexEntities(int sourceId, List<EntityProfile> profiles) {
		invertedIndex = new HashMap<String, Set<Integer>>();
		File file = new File("resources/stopwords/stopwords_SER");

		HashSet<String> stopwords = (HashSet<String>) SerializationUtilities
				.loadSerializedObject(file.getAbsolutePath());
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
		return invertedIndex;
	}    

	public int intersectionsCount(Set set1, Set set2) {
	    if (set2.size() < set1.size()) return intersectionsCount(set2, set1);
	    int count = 0;
	    for (Object o : set1)
	        if (set2.contains(o)) count++;
	    return count;
	}

	public double commonRatio(Set set1, Set set2) {
	    int common = intersectionsCount(set1, set2);
	    int union = set1.size() + set2.size() - common;
	    return (double) common / union; // [0.0, 1.0]
	}
	
	public void sortIndex() {
		System.out.println("Sorting Index..");
		invertedIndex = invertedIndex.entrySet()
				.stream()
			    .sorted(Comparator.comparing(e -> e.getValue().size(), Comparator.reverseOrder()))
			    .collect(toMap(
			        Map.Entry::getKey,
			        Map.Entry::getValue,
			        (a, b) -> { throw new AssertionError(); },
			        LinkedHashMap::new
		));
		System.out.println("Finished Sorting!");
	}
	
	public void createEntitiesToBlocks() {
		HashMap<Integer, Set<String>> entitiesToBlocks = new HashMap<>();
		for(String key : invertedIndex.keySet()) {
			Set<Integer> blockEntities = invertedIndex.get(key);
			for(Integer blockEntity : blockEntities) {
				Set<String> termTokens = entitiesToBlocks.computeIfAbsent(blockEntity,
						x -> new HashSet<String>());
				termTokens.add(key);
			}
		}
		sortEntitiesToBlocks(entitiesToBlocks);
		this.entitiesToBlocks = entitiesToBlocks;
	}
	
	public void createBlocksToComparisonWeights() {
		HashMap<String, Integer> comparisonWeights = new HashMap<>();
		
	}
	
	public void sortEntitiesToBlocks(HashMap<Integer, Set<String>> entitiesToBlocks) {
		HashMap<String, Long> blockComparisons = new HashMap<>();
		Iterator<String> iter = invertedIndex.keySet().iterator();
		while(iter.hasNext()) {
			String t = iter.next();
			long size = invertedIndex.get(t).size();
			blockComparisons.put(t, size);
		}
		
		
		//HashMap<Integer, Set<String>> sortedEtitiesToBlocks = new HashMap<Integer, Set<String>>();
		for(Integer e : entitiesToBlocks.keySet()) {
			Set<String> blocks = entitiesToBlocks.get(e);
			List<String> blocksSorted = blocks.stream().collect(Collectors.toList());
			Collections.sort(blocksSorted, (b1, b2) -> blockComparisons.get(b1).compareTo(blockComparisons.get(b2)));

			entitiesToBlocks.put(e, blocksSorted.stream().collect(Collectors.toCollection(LinkedHashSet::new))); 

		}
	}

	        
	public void sortTfIdf() {
		Map<String, Integer> sortedTfIdf = tfIdf.entrySet()
				.stream()
				.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
				.collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
						LinkedHashMap::new)); 
		this.tfIdf = sortedTfIdf;
	}

	public void trimTfIdf() {
//		EquiWidthBinning equiWidthBinning = new EquiWidthBinning(tfIdf);
//		equiWidthBinning.calculateNumberOfBins();
//		Set<String> leads = equiWidthBinning.implement();

		EquiFreqBinning equiFreqBinning = new EquiFreqBinning(tfIdf);
		equiFreqBinning.calculateNumberOfBins();
		List<List<String>> bins = equiFreqBinning.implement();
		Set<String> removeIds = new HashSet<>();
		for(List<String> bin : bins) {
			String[] keys = bin.toArray(new String[bin.size()]);
			Set<Integer> set1 = invertedIndex.get(keys[0]);
			for(int j = 1; j < keys.length; j ++) {
				String key2 = keys[j];
				Set<Integer> set2 = invertedIndex.get(key2);
				int size = set2.size();
				//if(commonRatio(set1, set2) > 0.1) removeIds.add(key2);
				//if(set1.containsAll(set2)) removeIds.add(key2);
				set2.retainAll(set1);
				if(set2.size() < size * 0.1) removeIds.add(key2);
			}
		}
		for(String id : removeIds) invertedIndex.remove(id);
	}
	
	public void storeBlockIndex(String path, String tableName) {
		SerializationUtilities.storeSerializedObject(this.invertedIndex, path + tableName + "InvertedIndex" );
		SerializationUtilities.storeSerializedObject(this.entitiesToBlocks, path + tableName + "EntitiesToBlocks" );
	
	}
	
	public void loadBlockIndex(String path, String tableName) {
		this.invertedIndex = (Map<String, Set<Integer>>) SerializationUtilities.loadSerializedObject(path + tableName + "InvertedIndex" );
		this.entitiesToBlocks = (Map<Integer, Set<String>>) SerializationUtilities.loadSerializedObject(path + tableName + "EntitiesToBlocks");
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
	
	public Map<Integer, Set<String>> getEntitiesToBlocks() {
		return entitiesToBlocks;
	}

	public void setEntitiesToBlocks(Map<Integer, Set<String>> entitiesToBlocks) {
		this.entitiesToBlocks = entitiesToBlocks;
	}



	public static <U, T> Map<T, Set<U>> deepCopy(Map<T, Set<U>> map) {
		Map<T, Set<U>> clone = new LinkedHashMap<>();
		for (Map.Entry<T, Set<U>> e : map.entrySet())
			clone.put(e.getKey(), e.getValue().stream().collect(Collectors.toSet())); 
		return clone;
	}

	@Override
	public Statistic getStatistic() {
		
		
		final BlockIndexStatistic blockIndexStatistic = this.blockIndexStatistic;
		final Map<String, Set<Integer>> invertedIndex = deepCopy(this.invertedIndex);
		final Map<Integer, Set<String>> entitiesToBlocks = this.entitiesToBlocks;
		
		return new Statistic() {
			public Double getRowCount() {
				return Double.valueOf(0.0D);
			}

			public boolean isKey(ImmutableBitSet columns) {
				return false;
			}

			public List<ImmutableBitSet> getKeys() {
				return null;
			}

			public List<RelReferentialConstraint> getReferentialConstraints() {
				return null;
			}

			public List<RelCollation> getCollations() {
				return null;
			}

			public RelDistribution getDistribution() {
				return null;
			}

			public Double getComparisons(List<RexNode> conjuctions) {
				double begin = System.currentTimeMillis();
				TokenStatistics tokenStatistics = new TokenStatistics(invertedIndex, entitiesToBlocks, blockIndexStatistic, conjuctions);
				double comparisons = tokenStatistics.getComparisons().doubleValue();
				double end = System.currentTimeMillis();
				return Double.valueOf(comparisons);
			}
		};
	}
	
	
	
}
