package org.imis.calcite.util;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.util.Sources;
import org.imis.calcite.adapter.csv.CsvEnumerator;
import org.imis.calcite.adapter.csv.CsvFieldType;
import org.imis.er.BlockIndex.QueryBlockIndex;
import org.imis.er.DataStructures.AbstractBlock;
import org.imis.er.DataStructures.EntityResolvedTuple;
import org.imis.er.DataStructures.UnilateralBlock;
import org.imis.er.EfficiencyLayer.AbstractEfficiencyMethod;
import org.imis.er.MetaBlocking.BlockFiltering;
import org.imis.er.MetaBlocking.ComparisonsBasedBlockPurging;
import org.imis.er.Utilities.ExecuteBlockComparisons;
import org.imis.er.Utilities.SerializationUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import gnu.trove.map.hash.TIntObjectHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
/*
 * Single table deduplication execution.
 */
public class DeduplicationExecution<T> {
	
	protected static final Logger DEDUPLICATION_EXEC_LOGGER =  LoggerFactory.getLogger(DeduplicationExecution.class);
	/**
	 * Performs deduplication on a single table's entities.
	 * The steps for performing the resolution are as follows:
	 * 	1) Get filtered data from filter.
	 * 	2) Create QueryBlockIndex
	 *  3) BlockJoin
	 *  4) Apply MetaBlocking
	 *  5) Create an enumberable by index scanning with the IDs as derived from the MetaBlocking
	 *  6) Execute Block Comparisons to find matches
	 *  
	 *  @param enumerable The data after the filter
	 *  @param tableName Name of the table used to get the BlockIndex
	 *  @param key Key column id of the table for block indexing
	 *  @param source Source of the table data for the scan
	 *  @param fieldTypes Types of the data 
	 *  @param ab Used for the csv enumerator, nothing else
	 *  
	 *  @return EntityResolvedTuple contains the UnionFind + HashMap of the table to be used in merging/join
	 */		
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <T, TKey> EntityResolvedTuple deduplicateEnumerator(Enumerable<T> enumerable, String tableName,
		Integer key, String source, List<CsvFieldType> fieldTypes, AtomicBoolean ab){

		Integer hashType = 0; //0 = JDK, 1 = TROVE, 2 = FAST
		CsvEnumerator<T> originalEnumerator = new CsvEnumerator(Sources.of(new File(source)), ab, fieldTypes);
		
		List<T> queryData = enumerable.toList();
		if(DEDUPLICATION_EXEC_LOGGER.isDebugEnabled()) 
			DEDUPLICATION_EXEC_LOGGER.debug("Query Entity Profiles\t:\t" + queryData.size());
		QueryBlockIndex queryBlockIndex = new QueryBlockIndex();
		queryBlockIndex.createBlockIndex(queryData, key);
		queryBlockIndex.buildQueryBlocks();
		Set<Integer> qIds = queryBlockIndex.getIds();
		queryData.clear(); // no more need for query data
		
		List<AbstractBlock> blocks = queryBlockIndex
				.joinBlockIndices(tableName);
		if(DEDUPLICATION_EXEC_LOGGER.isDebugEnabled()) 
			DEDUPLICATION_EXEC_LOGGER.debug("QueryBLocking - Blocks Ready\t:\t" + blocks.size());
		//Get ids of final entities
		AbstractEfficiencyMethod blockPurging = new ComparisonsBasedBlockPurging();
		blockPurging.applyProcessing(blocks);
		//purge based on number of comparisons, the max_number is calculated dynamically
		if(DEDUPLICATION_EXEC_LOGGER.isDebugEnabled()) 
			DEDUPLICATION_EXEC_LOGGER.debug("B Purging - Blocks Ready\t:\t" + blocks.size());
		if (blocks.size() > 10) {
			BlockFiltering bFiltering = new BlockFiltering(0.30);
			bFiltering.applyProcessing(blocks);
			if(DEDUPLICATION_EXEC_LOGGER.isDebugEnabled()) 
				DEDUPLICATION_EXEC_LOGGER.debug("B Filtering - Blocks Ready\t:\t" + blocks.size());
		}

		List<UnilateralBlock> uBlocks = (List<UnilateralBlock>) (List<? extends AbstractBlock>) blocks;
		Set<Integer> totalIds = queryBlockIndex.blocksToEntities(uBlocks);	
		AbstractEnumerable<Object[]> comparisonEnumerable = createEnumerable((CsvEnumerator<Object[]>) originalEnumerator, totalIds, key);
		//TIntObjectHashMap<Object[]>  entityMap = createMapTrove(comparisonEnumerable, key);
		//Int2ObjectOpenHashMap<Object[]> entityMap = createMapFast(comparisonEnumerable, key);
		HashMap<Integer, Object[]>  entityMap = createMap(comparisonEnumerable, key);
		
		// To find ground truth statistics
		storeBlocks(uBlocks, tableName);
		
		if(DEDUPLICATION_EXEC_LOGGER.isDebugEnabled()) 
			DEDUPLICATION_EXEC_LOGGER.debug("Joined Entity Profiles\t:\t" + entityMap.size());
		ExecuteBlockComparisons ebc = new ExecuteBlockComparisons(entityMap);
		EntityResolvedTuple entityResolvedTuple = ebc.comparisonExecutionAll(uBlocks, qIds, key, fieldTypes.size(), hashType);
		return entityResolvedTuple;
	}
	
	private static void storeBlocks(List<UnilateralBlock> uBlocks, String tableName) {
		if(new File("./data/blocks/" + tableName).exists()) return;
		double storeStartTime = System.currentTimeMillis();
		SerializationUtilities.storeSerializedObject(uBlocks, "./data/blocks/" + tableName);
		double storeEndTime = System.currentTimeMillis();
		System.out.println("Storing blocks time: " + (storeEndTime - storeStartTime)/1000);
	}

	/**
	 * 
	 * @param <T>
	 * @param entityResolvedTuple The tuple as created by the deduplication/join
	 * @return AbstractEnumerable that combines the hashmap and the UnionFind to create the merged/fusioned data
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static<T> AbstractEnumerable<T> mergeEntities(EntityResolvedTuple entityResolvedTuple){
//		if(entityResolvedTuple.uFind != null)
//			entityResolvedTuple.sortEntities();
		if(DEDUPLICATION_EXEC_LOGGER.isDebugEnabled()) 
			DEDUPLICATION_EXEC_LOGGER.debug("Final Size: " + entityResolvedTuple.finalData.size());
		return entityResolvedTuple;

	}
	/**
	 * 
	 * @param enumerable Data of the table
	 * @param key Key column of the table
	 * @return HashMap from key to entity
	 */
	private static HashMap<Integer, Object[]> createMap(AbstractEnumerable<Object[]> enumerable, Integer key) {
		List<Object[]> entityList = enumerable.toList();
		HashMap<Integer, Object[]>entityMap = new HashMap<Integer, Object[]>(entityList.size());
		for(Object[] entity : entityList) {
			entityMap.put(Integer.parseInt(entity[key].toString()), entity);
		}
		return entityMap;
	}
	
	
	
	private static TIntObjectHashMap< Object[]> createMapTrove(AbstractEnumerable<Object[]> enumerable, Integer key) {
		List<Object[]> entityList = enumerable.toList();
		TIntObjectHashMap<Object[]>entityMap = new TIntObjectHashMap<Object[]>(entityList.size());
		for(Object[] entity : entityList) {
			entityMap.put(Integer.parseInt(entity[key].toString()), entity);
		}
		return entityMap;
	}
	
	private static Int2ObjectOpenHashMap<Object[]> createMapFast(AbstractEnumerable<Object[]> enumerable,
			Integer key) {
				
		List<Object[]> entityList = enumerable.toList();
		Int2ObjectOpenHashMap<Object[]>entityMap = new Int2ObjectOpenHashMap<Object[]>(entityList.size());
		for(Object[] entity : entityList) {
			entityMap.put(Integer.parseInt(entity[key].toString()), entity);
		}
		return entityMap;
	}
	
	/**
	 * 
	 * @param enumerator Enumerator data
	 * @param qIds Qids to pick from enumerator
	 * @param key Key column
	 * @return AbstractEnumerable filtered by ids
	 */
	private static AbstractEnumerable<Object[]> createEnumerable(CsvEnumerator<Object[]> enumerator, Set<Integer> qIds, Integer key) {
		return new AbstractEnumerable<Object[]>() {
			@Override
			public Enumerator<Object[]> enumerator() {
				return new Enumerator<Object[]>() {
					@Override
					public Object[] current() {
						return enumerator.current();
					}
					@Override
					public boolean moveNext() {
						while (enumerator.moveNext()) {
							final Object[] current = enumerator.current();
							String entityKey = current[key].toString();
							if(entityKey.contentEquals("")) continue;
							if (qIds.contains(Integer.parseInt(current[key].toString()))) {
								return true;
							}
						}
						return false;
					}
					@Override
					public void reset() {
					}
					@Override
					public void close() {
					}
				};
			}

		};
	}
}
