package org.imsi.queryEREngine.imsi.calcite.util;

import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;
import org.imsi.queryEREngine.apache.calcite.util.Source;
import org.imsi.queryEREngine.apache.calcite.util.Sources;
import org.imsi.queryEREngine.imsi.calcite.adapter.csv.CsvEnumerator;
import org.imsi.queryEREngine.imsi.calcite.adapter.csv.CsvFieldType;
import org.imsi.queryEREngine.imsi.er.BlockIndex.QueryBlockIndex;
import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.EntityResolvedTuple;
import org.imsi.queryEREngine.imsi.er.EfficiencyLayer.BlockRefinement.ComparisonsBasedBlockPurging;
import org.imsi.queryEREngine.imsi.er.MetaBlocking.BlockFiltering;
import org.imsi.queryEREngine.imsi.er.MetaBlocking.EfficientEdgePruning;
import org.imsi.queryEREngine.imsi.er.Utilities.ExecuteBlockComparisons;
import org.imsi.queryEREngine.imsi.er.Utilities.UnionFind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * A dirty right/left join takes as input one entityResolvedTuples and an enumerable.
 * The entityResolvedTuple contains the data + a hashset that for each of the entities
 * describes its duplicates as found by the comparison execution. It is a hashjoin, that hashes the
 * smallest table by the join column and the big table by its id.
 * <p>
 * We enumerate the second hash and for each entity (id only maps to one entity) we get the entities from the
 * right that join with it. Then for both the left and right entities we get their duplicates and perform the
 * left product of these two sets joining all of the entities.
 * During these process we keep track of the already visited ids of each table so we don't perform the same join
 * twice.
 */
public class DeduplicationJoinExecution {

	protected static final Logger DEDUPLICATION_EXEC_LOGGER = LoggerFactory.getLogger(DeduplicationExecution.class);
	private static final String pathToPropertiesFile = "deduplication.properties";
	private static Properties properties;

	private static final String BP = "mb.bp";
	private static final String BF = "mb.bf";
	private static final String EP = "mb.ep";
	private static final String LINKS = "links";
	private static final String FILTER_PARAM = "filter.param";

	private static boolean runBP = true;
	private static boolean runBF = true;
	private static boolean runEP = true;
	private static boolean runLinks = true;
	private static double filterParam = 0.0;
	/**
	 * Executes the algorithm of the dirty right join.
	 * Can deduplicate the table immediately after the scan or by getting the dirty matches first to
	 * minimize the right table entities.
	 *
	 * @param <TSource>            Can be used to make the Object[] type of the entities generic
	 * @param <TRight>             Can be used to make the Object[] type of the entities generic
	 * @param <TKey>               Can be used to make the Integer type of the entities generic
	 * @param <TResult>            Can be used to make the Object[] type of the entities generic
	 * @param <TLeft>             Can be used to make the Object[] type of the entities generic
	 * @param left                Left data tuple
	 * @param right                Right data enumerable
	 * @param leftKeySelector     Function that gets key column from entity of left
	 * @param rightKeySelector     Function that gets key column from entity of right
	 * @param resultSelector       Function that generates the merged entities after the join
	 * @param keyRight             Right table key position
	 * @param rightTableName       Right table Name
	 * @param rightTableSize       Right table column length
	 * @param comparer             Used for joins
	 * @param generateNullsOnLeft  Used for joins
	 * @param generateNullsOnRight Used for joins
	 * @param predicate            Used for joinss
	 * @return EntityResolvedTuple that contains the joined entities and their union find data
	 * @throws IOException 
	 */
	@SuppressWarnings("rawtypes")
	public static <TSource, TRight, TKey, TResult, TLeft> EntityResolvedTuple dirtyRightJoin(EntityResolvedTuple left,
			Enumerable<Object[]> right,
			Function1<Object[], TKey> leftKeySelector,
			Function1<Object[], TKey> rightKeySelector,
			Function2<Object[], Object[], Object[]> resultSelector,
			String sourceRight,
			List<CsvFieldType> fieldTypes,
			Integer keyRight,
			String rightTableName,
			Integer rightTableSize,
			EqualityComparer<TKey> comparer,
			boolean generateNullsOnLeft, boolean generateNullsOnRight,
			Predicate2<Object[], Object[]> predicate
			) {

		List<Object[]> filteredData = new ArrayList<>();

		filteredData = getDirtyMatches((Enumerable<Object[]>) Linq4j.asEnumerable(left.finalData),
				right, leftKeySelector, rightKeySelector, resultSelector, comparer,
				generateNullsOnLeft, generateNullsOnRight, predicate, keyRight);
		AtomicBoolean ab = new AtomicBoolean();
		ab.set(false);
		CsvEnumerator<Object[]> originalEnumerator = new CsvEnumerator(Sources.of(new File(sourceRight)),
				ab, identityList(rightTableSize));


		//Deduplicate the dirty table
		EntityResolvedTuple entityResolvedTuple = deduplicate(filteredData, keyRight, rightTableSize,
				rightTableName, originalEnumerator);


		EntityResolvedTuple joinedEntityResolvedTuple  =
				deduplicateJoin(left, entityResolvedTuple, leftKeySelector, rightKeySelector,
						resultSelector, comparer, generateNullsOnLeft, generateNullsOnRight, predicate);
		joinedEntityResolvedTuple.getAll();
		return joinedEntityResolvedTuple;
	}

	/**
	 * Executes the algorithm of the dirty left join.
	 * Can deduplicate the table immediately after the scan or by getting the dirty matches first to
	 * minimize the left table entities.
	 *
	 * @param <TSource>           Can be used to make the Object[] type of the entities generic
	 * @param <TRight>            Can be used to make the Object[] type of the entities generic
	 * @param <TKey>              Can be used to make the Integer type of the entities generic
	 * @param <TResult>           Can be used to make the Object[] type of the entities generic
	 * @param <TLeft>            Can be used to make the Object[] type of the entities generic
	 * @param left               Left data tuple
	 * @param right               Right data enumerable
	 * @param leftKeySelector    Function that gets key column from entity of left
	 * @param rightKeySelector    Function that gets key column from entity of right
	 * @param resultSelector      Function that generates the merged entities after the join
	 * @param keyleft             left table key position
	 * @param leftTableName       left table Name
	 * @param leftTableSize       left table column length
	 * @param comparer            Used for joins
	 * @param generateNullsOnLeft Used for joins
	 * @param generateNullsOnleft Used for joins
	 * @param predicate           Used for joinss
	 * @return EntityResolvedTuple that contains the joined entities and their union find data
	 * @throws IOException 
	 */
	@SuppressWarnings("rawtypes")
	public static <TSource, TRight, TKey, TResult, TLeft> EntityResolvedTuple dirtyLeftJoin(Enumerable<Object[]> left,
			EntityResolvedTuple<Object[]> right,
			Function1<Object[], TKey> leftKeySelector,
			Function1<Object[], TKey> rightKeySelector,
			Function2<Object[], Object[], Object[]> resultSelector,
			String sourceLeft,
			List<CsvFieldType> fieldTypes,
			Integer keyLeft,
			String leftTableName,
			Integer leftTableSize,
			EqualityComparer<TKey> comparer,
			boolean generateNullsOnLeft, boolean generateNullsOnRight,
			Predicate2<Object[], Object[]> predicate
			) {


		List<Object[]> filteredData = new ArrayList<>();
		filteredData = getDirtyMatches(Linq4j.asEnumerable(right.finalData), left,
				rightKeySelector, leftKeySelector, resultSelector, comparer,
				generateNullsOnRight, generateNullsOnLeft, predicate, keyLeft);
		AtomicBoolean ab = new AtomicBoolean();
		ab.set(false);
		CsvEnumerator<Object[]> originalEnumerator = new CsvEnumerator(Sources.of(new File(sourceLeft)),
				ab, identityList(leftTableSize));

		// Deduplicate the dirty table
		EntityResolvedTuple entityResolvedTuple = deduplicate(filteredData, keyLeft, leftTableSize,
				leftTableName, originalEnumerator);
		// Reverse the right, left structure
		EntityResolvedTuple  joinedEntityResolvedTuple =
				deduplicateJoin(entityResolvedTuple, right, leftKeySelector, rightKeySelector,
						resultSelector, comparer, generateNullsOnLeft, generateNullsOnRight, predicate);
		joinedEntityResolvedTuple.getAll();
		return joinedEntityResolvedTuple;
	}

	/**
	 * Clean join performs the join algorithm on two cleaned datasets
	 */
	@SuppressWarnings("rawtypes")
	public static <TSource, TRight, TKey, TResult, TLeft> EntityResolvedTuple cleanJoin(EntityResolvedTuple<Object[]> left,
			EntityResolvedTuple<Object[]> right,
			Function1<Object[], TKey> leftKeySelector,
			Function1<Object[], TKey> rightKeySelector,
			Function2<Object[], Object[], Object[]> resultSelector,
			EqualityComparer<TKey> comparer,
			boolean generateNullsOnLeft, boolean generateNullsOnRight,
			Predicate2<Object[], Object[]> predicate
			) {

		EntityResolvedTuple joinedEntityResolvedTuple =
				deduplicateJoin(left, right, leftKeySelector, rightKeySelector,
						resultSelector, comparer, generateNullsOnLeft, generateNullsOnRight, predicate);
		joinedEntityResolvedTuple.getAll();

		return joinedEntityResolvedTuple;
	}

	private static HashMap<Integer, Object[]> createMap(AbstractEnumerable<Object[]> enumerable, Integer key) {
		List<Object[]> entityList = enumerable.toList();
		int size = entityList.size();
		HashMap<Integer, Object[]> entityMap = new HashMap<Integer, Object[]>(size);
		for (Object[] entity : entityList) {
			entityMap.put(Integer.parseInt(entity[key].toString()), entity);
		}
		return entityMap;
	}


	/**
	 * Implements a faux-join only to get the entities that match for the hashing table.
	 * This way we can perform deduplication on a subset of the data.
	 *
	 * @param <TSource>
	 * @param <TRight>
	 * @param <TKey>
	 * @param left                Left data enumerable
	 * @param right                Right data enumerable
	 * @param leftKeySelector     Function that gets key column from entity of left
	 * @param rightKeySelector     Function that gets key column from entity of right
	 * @param resultSelector       Function that generates the merged entities after the join
	 * @param comparer             Used for joins
	 * @param generateNullsOnLeft  Used for joins
	 * @param generateNullsOnRight Used for joins
	 * @param predicate            Used for joinss
	 * @return
	 */
	@SuppressWarnings("unused")
	private static <TSource, TRight, TKey> List<Object[]> getDirtyMatches(Enumerable<Object[]> left,
			Enumerable<Object[]> right,
			Function1<Object[], TKey> leftKeySelector,
			Function1<Object[], TKey> rightKeySelector,
			Function2<Object[], Object[], Object[]> resultSelector,
			EqualityComparer<TKey> comparer,
			boolean generateNullsOnLeft, boolean generateNullsOnRight,
			Predicate2<Object[], Object[]> predicate,
			Integer key) {

		final Enumerable<Object[]> rightToLookUp =  Linq4j.asEnumerable(right.toList());
		final Lookup<TKey, Object[]> rightLookup = rightToLookUp.toLookup(rightKeySelector);
		rightLookup.remove("");
		Enumerator<Object[]> lefts = left.enumerator();
		Enumerator<Object[]> rights = Linq4j.emptyEnumerator();

		final Set<Integer> dirtyIds = new HashSet<>();
		final List<Object[]> dirtyData = new ArrayList<>();

		for (; ; ) {

			if (!lefts.moveNext()) {
				break;
			}
			final Object[] left2 = lefts.current();
			Enumerable<Object[]> rightEnumerable;
			if (left2 == null) {
				rightEnumerable = null;
			} else {
				final TKey leftKey = leftKeySelector.apply(left2);
				if (leftKey == null) {
					rightEnumerable = null;
				} else {
					rightEnumerable = rightLookup.get(leftKey);
					if (rightEnumerable != null) {
						try (Enumerator<Object[]> rightEnumerator =
								rightEnumerable.enumerator()) {
							while (rightEnumerator.moveNext()) {
								final Object[] right2 = rightEnumerator.current();
								try {
									String right2Key = right2[key].toString();
									if (!dirtyIds.contains(Integer.parseInt(right2Key))) {
										dirtyIds.add(Integer.parseInt(right2Key));
										dirtyData.add(right2);
									}
								}
								catch (Exception e) { continue; }
							}
						}
					}
				}
			}
		}
		return dirtyData;
	}


	/**
	 * Executes a deduplicated join, that takes as input two entity resolved tuples and returns another entity resolved join tuple
	 * The joined table gets a new column that contains the identification numbers of each row. This is needed to identify the duplicate
	 * entities in the Union Find.
	 *
	 * @param <TSource>
	 * @param <TRight>
	 * @param <TKey>
	 * @param left                Left deduplicated tuple
	 * @param right                Right deduplicated tuple
	 * @param leftKeySelector     Function that gets key column from entity of left
	 * @param rightKeySelector     Function that gets key column from entity of right
	 * @param resultSelector       Function that generates the merged entities after the join
	 * @param comparer             Used for joins
	 * @param generateNullsOnLeft  Used for joins
	 * @param generateNullsOnRight Used for joins
	 * @param predicate            Used for joins
	 * 	 * @return
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	private static <TSource, TRight, TKey> EntityResolvedTuple deduplicateJoin(EntityResolvedTuple left,
			EntityResolvedTuple right,
			Function1<Object[], TKey> leftKeySelector,
			Function1<Object[], TKey> rightKeySelector,
			Function2<Object[], Object[], Object[]> resultSelector,
			EqualityComparer<TKey> comparer,
			boolean generateNullsOnLeft, boolean generateNullsOnRight,
			Predicate2<Object[], Object[]> predicate) {

		double deduplicateJoinStartTime = System.currentTimeMillis();
		final Enumerable<Object[]> rightToLookUp = Linq4j.asEnumerable(right.finalData);
		right.finalData.forEach(row -> {
			//			Object[] r = (Object[]) row;
			//
			//			System.out.println(r[0]);
		});
		final Lookup<TKey, Object[]> rightLookup =
				comparer == null
				? rightToLookUp.toLookup(rightKeySelector)
						: rightToLookUp
						.toLookup(rightKeySelector, comparer);

				HashMap<Integer, Object[]> leftsMap = left.data;
				HashMap<Integer, Object[]> rightsMap = right.data;
				HashMap<Integer, Set<Integer>> leftMatches = left.revUF;
				HashMap<Integer, Set<Integer>> rightMatches = right.revUF;

				Set<Integer> joinedIds = new HashSet<>();
				Integer joinedId = 0; // left id to enumerate the duplicates
				joinedIds.add(joinedId);
				UnionFind joinedUFind = new UnionFind(joinedIds);
				//List<Object[]> joinedEntities = new ArrayList<>();
				HashMap<Integer, Object[]> joinedEntities = new HashMap<>();
				Set<Integer> leftCheckedIds = new HashSet<>(leftsMap.size());

				for (Integer leftId : leftsMap.keySet()) {		
					Set<Integer> leftMatchedIds = leftMatches.get(leftId);
					if(leftCheckedIds.contains(leftId)) continue;
					leftCheckedIds.addAll(leftMatchedIds);
					Set<Integer> rightJoinIds  = new HashSet<>();
					for (Integer leftMatchedId : leftMatchedIds) {
						Object[] leftCurrent = leftsMap.get(leftMatchedId);	
						TKey leftKey = leftKeySelector.apply(leftCurrent);
						Enumerable<Object[]> rightEnumerable = rightLookup.get(leftKey); // do this for each similar
						if(rightEnumerable != null) {
							try (Enumerator<Object[]> rightEnumerator =
									rightEnumerable.enumerator()) {
								while (rightEnumerator.moveNext()) {
									final Object[] rightMatched = rightEnumerator.current();
									Integer rightId = Integer.parseInt(rightMatched[0].toString());
									rightJoinIds.add(rightId);
								}
							}
						}
					}

					Set<Integer> rightCheckedIds = new HashSet<>();
					for(Integer rightJoinId : rightJoinIds) {
						Integer rightJoinedId = joinedId; 
						Set<Integer> rightMatchedIds = rightMatches.get(rightJoinId);	
						if(rightCheckedIds.contains(rightJoinId)) continue;
						rightCheckedIds.addAll(rightMatchedIds);
						for (Integer leftMatchedId : leftMatchedIds) {
							Object[] leftCurrent = leftsMap.get(leftMatchedId);
							for(Integer rightMatchedId : rightMatchedIds) {
								Object[] rightCurrent = rightsMap.get(rightMatchedId);
								Object[] joinedEntity = resultSelector.apply(leftCurrent, rightCurrent);
								joinedEntity = appendValue(joinedEntity, rightJoinedId);
								joinedEntities.put(rightJoinedId, joinedEntity);
								joinedUFind.union(joinedId, rightJoinedId);
								rightJoinedId += 1;
							}
						}
						joinedId = rightJoinedId + 1;
					}
				}

				double deduplicateJoinEndTime = System.currentTimeMillis();
				if (DEDUPLICATION_EXEC_LOGGER.isDebugEnabled())
					DEDUPLICATION_EXEC_LOGGER.debug(left.finalData.size() + "," + right.finalData.size() + "," + joinedEntities.size() + "," +
							(deduplicateJoinEndTime - deduplicateJoinStartTime) / 1000);
				System.out.println(left.finalData.size() + "," + right.finalData.size() + "," + joinedEntities.size() + "," +
						(deduplicateJoinEndTime - deduplicateJoinStartTime) / 1000);
				int len = joinedEntities.get(0).length;
				return new EntityResolvedTuple(joinedEntities, joinedUFind, len - 1, len);
	}

	/**
	 * @param obj
	 * @param newObj
	 * @return object array with obj appended on its end
	 */
	private static Object[] appendValue(Object[] obj, Object newObj) {
		ArrayList<Object> temp = new ArrayList<Object>(Arrays.asList(obj));
		temp.add(newObj);
		return temp.toArray();

	}


	public static List<CsvFieldType> identityList(int n) {
		List<CsvFieldType> csvFieldTypes = new ArrayList<>();
		for(int i = 0; i < n; i++) {
			csvFieldTypes.add(null);
		}
		return csvFieldTypes;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static EntityResolvedTuple deduplicate(List<Object[]> queryData, Integer key, Integer noOfAttributes,
			String tableName, Enumerator<Object[]> originalEnumerator) {

		setProperties();
		double deduplicateStartTime = System.currentTimeMillis();
		boolean firstDedup  = false;
		String queryDataSize = Integer.toString(queryData.size());


		// Check for links and remove qIds that have links
        double linksStartTime = System.currentTimeMillis();
		HashMap<Integer, Set<Integer>> links = DeduplicationExecution.loadLinks(tableName);
		if(links == null) firstDedup = true;

		Set<Integer> qIds = new HashSet<>();
		Set<Integer> totalIds = new HashSet<>();

		final Set<Integer> qIdsNoLinks;
		qIds = queryData.stream().map(row -> {
			return Integer.parseInt(row[key].toString());
		}).collect(Collectors.toSet());

		// Remove from data qIds with links
		if(!firstDedup) {
			queryData = queryData.stream().filter(row -> {
				Integer id = Integer.parseInt(row[key].toString());
				return !(links.containsKey(id));
			}).collect(Collectors.toList());	
			// Clear links and keep only qIds
			Set<Integer> linkedIds = DeduplicationExecution.getLinkedIds(queryData, key, links,  qIds);
			totalIds.addAll(linkedIds);  // Add links back
		}
		qIdsNoLinks = queryData.stream().map(row -> {
			return Integer.parseInt(row[key].toString());
		}).collect(Collectors.toSet());
		
        double linksEndTime = System.currentTimeMillis();
        String linksTime = Double.toString((linksEndTime - linksStartTime) / 1000);

		double blockingStartTime = System.currentTimeMillis();
		QueryBlockIndex queryBlockIndex = new QueryBlockIndex();
		queryBlockIndex.createBlockIndex(queryData, key);
		queryBlockIndex.buildQueryBlocks();
        boolean doER = queryData.size() > 0 ? true : false;
		queryData.clear(); // no more need for query data
		double blockingEndTime = System.currentTimeMillis();
		String blockingTime = Double.toString((blockingEndTime - blockingStartTime) / 1000);

		double blockJoinStart = System.currentTimeMillis();
		List<AbstractBlock> blocks = queryBlockIndex
				.joinBlockIndices(tableName, doER);
		double blockJoinEnd = System.currentTimeMillis();
		String blockJoinTime = Double.toString((blockJoinEnd - blockJoinStart) / 1000);


		String blocksSize = Integer.toString(blocks.size());
		String blockSizes = getBlockSizes(blocks);
		String blockEntities = Integer.toString(queryBlockIndex.blocksToEntities(blocks).size());

		// PURGING
		double blockPurgingStartTime = System.currentTimeMillis();
		ComparisonsBasedBlockPurging blockPurging = new ComparisonsBasedBlockPurging();
		if(runBP) blockPurging.applyProcessing(blocks);
		double blockPurgingEndTime = System.currentTimeMillis();

		String purgingBlocksSize = Integer.toString(blocks.size());
		String purgingTime = Double.toString((blockPurgingEndTime - blockPurgingStartTime) / 1000);
		String purgingBlockSizes = getBlockSizes(blocks);

		int entities = queryBlockIndex.blocksToEntities(blocks).size();
		String purgeBlockEntities = Integer.toString(entities);

		double comps = 0.0;

		for(AbstractBlock block : blocks) {
			comps += block.getNoOfComparisons();
		}

		String filterBlocksSize = "";
		String filterTime = "";
		String filterBlockSizes = "";
		String epTime = "";
		String epTotalComps = "";
		String filterBlockEntities = "";
		String ePEntities = "";
		if (blocks.size() > 10) {
			// FILTERING
			double blockFilteringStartTime = System.currentTimeMillis();
			//double filterParam = DeduplicationExecution.calculateFilterParam(entities, comps);
			BlockFiltering bFiltering = new BlockFiltering(0.35);

			if(runBF) bFiltering.applyProcessing(blocks);
			double blockFilteringEndTime = System.currentTimeMillis();
			filterBlocksSize = Integer.toString(blocks.size());

			filterTime = Double.toString((blockFilteringEndTime - blockFilteringStartTime) / 1000);
			filterBlockSizes = getBlockSizes(blocks);
			filterBlockEntities = Integer.toString(queryBlockIndex.blocksToEntities(blocks).size());
			// EDGE PRUNING
			double edgePruningStartTime = System.currentTimeMillis();
			EfficientEdgePruning eEP = new EfficientEdgePruning();
			if(runEP) eEP.applyProcessing(blocks);
			double edgePruningEndTime = System.currentTimeMillis();

			epTime = Double.toString((edgePruningEndTime - edgePruningStartTime) / 1000);
			double totalComps = 0;
			for (AbstractBlock block : blocks) {
				totalComps += block.getNoOfComparisons();
			}
			epTotalComps = Double.toString(totalComps);
			ePEntities = Integer.toString(queryBlockIndex.blocksToEntitiesD(blocks).size());

		}

		//Get ids of final entities, and add back qIds that were cut from m-blocking
        Set<Integer> blockQids = new HashSet<>();
        if(runEP)
        	blockQids = queryBlockIndex.blocksToEntitiesD(blocks);
        else
        	blockQids = queryBlockIndex.blocksToEntities(blocks);
        totalIds.addAll(blockQids);
        totalIds.addAll(qIds);

		double tableScanStartTime = System.currentTimeMillis();
		AbstractEnumerable<Object[]> comparisonEnumerable = createEnumerable((Enumerator<Object[]>) originalEnumerator, totalIds, key);
		double tableScanEndTime = System.currentTimeMillis();
		String tableScanTime = Double.toString((tableScanEndTime - tableScanStartTime) / 1000);

		HashMap<Integer, Object[]> entityMap = createMap(comparisonEnumerable, key);
		double comparisonStartTime = System.currentTimeMillis();
		ExecuteBlockComparisons ebc = new ExecuteBlockComparisons(entityMap);
		EntityResolvedTuple entityResolvedTuple = ebc.comparisonExecutionAll(blocks, qIdsNoLinks, key, noOfAttributes);
		entityResolvedTuple.mergeLinks(links, tableName, firstDedup, totalIds, runLinks);
		
		Integer executedComparisons = entityResolvedTuple.getComparisons();
		int matches = entityResolvedTuple.getMatches();
		int totalEntities = entityResolvedTuple.data.size();
		double jaroTime = entityResolvedTuple.getCompTime();
		double comparisonEndTime = System.currentTimeMillis();
		double deduplicateEndTime = System.currentTimeMillis();
		double revUfCreationTime = entityResolvedTuple.getRevUFCreationTime();
		String comparisonTime = Double.toString((comparisonEndTime - comparisonStartTime) / 1000);
		String totalDeduplicationTime = Double.toString((deduplicateEndTime - deduplicateStartTime) / 1000);

		// Log everything
		if (DEDUPLICATION_EXEC_LOGGER.isDebugEnabled())
			DEDUPLICATION_EXEC_LOGGER.debug(tableName + "," + queryDataSize + "," + blockJoinTime + "," + blockingTime + "," + blocksSize + "," + 
					blockSizes + "," + blockEntities + "," + purgingBlocksSize + "," + purgingTime + "," + purgingBlockSizes + "," + 
					purgeBlockEntities + "," + filterBlocksSize + "," + filterTime + "," + filterBlockSizes + ","  + filterBlockEntities + "," +
					epTime + "," + epTotalComps + "," + ePEntities + "," + matches + "," + executedComparisons + "," + tableScanTime + "," + jaroTime + "," +
					comparisonTime + "," + revUfCreationTime + "," + totalEntities + "," + totalDeduplicationTime);
		 System.out.println(tableName + "\nqueryDataSize " + queryDataSize + "\nlinksTime " + linksTime + "\nblockJoinTime " + blockJoinTime + "\nblockingTime " + blockingTime + "\nblocksSize " + blocksSize + "\nblockSizes " + 
	    			blockSizes + "\nblockEntities " + blockEntities + "\npurgingBlocksSize " + purgingBlocksSize + "\npurgingTime " + purgingTime + "\npurgingBlockSizes " + purgingBlockSizes + "\n, " + 
	    			purgeBlockEntities + "\nfilterBlocksSize " + filterBlocksSize + "\nfilterTime " + filterTime + "\nfilterBlockSizes " + filterBlockSizes + "\nfilterBlockSizes"  + filterBlockEntities + "\nepTime " +
	    			epTime + "\nepTotalComps " + epTotalComps + "\nePEntities " + ePEntities + "\nmatches " + matches + "\nexecutedComparisons " + executedComparisons + "\ntableScanTime " + tableScanTime + "\njaroTime " + jaroTime + "\ncomparisonTime " +
	    			comparisonTime + "\nrevUfCreationTime " + revUfCreationTime + "\ntotalEntities " + totalEntities + "\ntotalDeduplicationTime " + totalDeduplicationTime);
		return entityResolvedTuple;
	}


	private static String getBlockSizes(List<AbstractBlock> blocks) {
		double maxBlockSize = 0.0;
		double totalBlockSize = 0.0;
		double totalComps = 0.0;
		double avgBlockSize;

		for(AbstractBlock block : blocks) {
			double blockSize = block.getTotalBlockAssignments();
			if(blockSize > maxBlockSize) maxBlockSize = blockSize;
			totalBlockSize += blockSize;
			totalComps += block.getNoOfComparisons();

		}
		avgBlockSize = totalBlockSize/blocks.size();
		return String.valueOf(maxBlockSize) + "," + avgBlockSize + "," + totalComps;

	}


	/**
	 * @param enumerator Enumerator data
	 * @param qIds       Qids to pick from enumerator
	 * @param key        Key column
	 * @return AbstractEnumerable filtered by ids
	 */
	private static AbstractEnumerable<Object[]> createEnumerable(Enumerator<Object[]> enumerator, Set<Integer> qIds, Integer key) {
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
							if(entityKey.equals("")) continue;
							if (qIds.contains(Integer.parseInt(entityKey))) {
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
	
	private static void setProperties() {
		properties = loadProperties();
		if(!properties.isEmpty()) {
			runBP = Boolean.parseBoolean(properties.getProperty(BP));
            runBF = Boolean.parseBoolean(properties.getProperty(BF));
            runEP = Boolean.parseBoolean(properties.getProperty(EP));
            runLinks = Boolean.parseBoolean(properties.getProperty(LINKS));
		}
	}
	
	private static Properties loadProperties() {
		
        Properties prop = new Properties();

		try (InputStream input = new FileInputStream(pathToPropertiesFile)) {
            // load a properties file
            prop.load(input);
                       
        } catch (IOException ex) {
            ex.printStackTrace();
        }
		return prop;
	}
}
