package org.imis.er.Utilities;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.imis.calcite.util.DeduplicationExecution;
import org.imis.er.DataStructures.AbstractBlock;
import org.imis.er.DataStructures.Comparison;
import org.imis.er.DataStructures.EntityProfile;
import org.imis.er.DataStructures.EntityResolvedTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gnu.trove.map.hash.TIntObjectHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class ExecuteBlockComparisons<T> {

	private HashMap<Integer, EntityProfile> dataset = new HashMap<>();
	private HashMap<Integer, Object[]> newData = new HashMap<>();
	private TIntObjectHashMap<Object[]> newData2 = new TIntObjectHashMap<>();
	private Int2ObjectOpenHashMap<Object[]> newData3 = new Int2ObjectOpenHashMap<>();
	protected static final Logger DEDUPLICATION_EXEC_LOGGER =  LoggerFactory.getLogger(DeduplicationExecution.class);

	public ExecuteBlockComparisons(List<EntityProfile> profiles) {
		for (EntityProfile ep : profiles) {
			dataset.put(Integer.parseInt(ep.getEntityUrl()), ep);
		}
		if(DEDUPLICATION_EXEC_LOGGER.isDebugEnabled()) 
			DEDUPLICATION_EXEC_LOGGER.debug("Dataset size\t:\t" + dataset.size());
	}


	public ExecuteBlockComparisons(HashMap<Integer, Object[]> newData) {
		this.newData = newData;
	}

	public ExecuteBlockComparisons(TIntObjectHashMap<Object[]> newData2) {
		this.newData2 = newData2;
	}

	public ExecuteBlockComparisons(Int2ObjectOpenHashMap<Object[]> newData3) {
		this.newData3 = newData3;
	}


	public EntityResolvedTuple comparisonExecutionAll(List<AbstractBlock> blocks, Set<Integer> qIds,
			Integer keyIndex, Integer noOfFields, Integer hashType) {
		
		if(hashType == 0) {
			return comparisonExecutionJdk(blocks, qIds, keyIndex, noOfFields);
		}
		else if(hashType == 1) {
			return comparisonExecutionTrove(blocks, qIds, keyIndex, noOfFields);
		}
		else if(hashType == 2) {
			return comparisonExecutionFast(blocks, qIds, keyIndex, noOfFields);

		}
		else {
			return null;
		}
	}

	

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public EntityResolvedTuple comparisonExecutionJdk(List<AbstractBlock> blocks, Set<Integer> qIds,
			Integer keyIndex, Integer noOfFields) {
		int comparisons = 0;
		UnionFind uFind = new UnionFind(qIds);
		
		Set<String> matches = new HashSet<>();
		Set<AbstractBlock> nBlocks = new HashSet<>(blocks);
		Set<String> uComparisons = new HashSet<>();
		for (AbstractBlock block : nBlocks) {
			ComparisonIterator iterator = block.getComparisonIterator();
			while (iterator.hasNext()) {
				Comparison comparison = iterator.next();

				if (!qIds.contains(comparison.getEntityId1()) && !qIds.contains(comparison.getEntityId2()))
					continue;

				String uniqueComp = "";
				if (comparison.getEntityId1() > comparison.getEntityId2())
					uniqueComp = comparison.getEntityId1() + "u" + comparison.getEntityId2();
				else
					uniqueComp = comparison.getEntityId2() + "u" + comparison.getEntityId1();
				if (uComparisons.contains(uniqueComp))
					continue;
				uComparisons.add(uniqueComp);

				double similarity = ProfileComparison.getJaroSimilarity(
						newData.get(comparison.getEntityId1()),
						newData.get(comparison.getEntityId2()),
						keyIndex);

				comparisons++;
				if (similarity >= 0.92 ) {
					matches.add(uniqueComp);
					uFind.union(comparison.getEntityId1(), comparison.getEntityId2());
				}
			}
		}

		
		if(DEDUPLICATION_EXEC_LOGGER.isDebugEnabled()) {
			DEDUPLICATION_EXEC_LOGGER.debug("Matches Found " + matches.size());
			DEDUPLICATION_EXEC_LOGGER.debug("Total Comparisons " + comparisons);
		}
		EntityResolvedTuple eRT = new EntityResolvedTuple(newData, uFind);		
		eRT.getAll();
		return eRT;

	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public EntityResolvedTuple comparisonExecutionTrove(List<AbstractBlock> blocks, Set<Integer> qIds,
			Integer keyIndex, Integer noOfFields) {
		int comparisons = 0;
		UnionFind uFind = new UnionFind(qIds);
		
		Set<String> matches = new HashSet<>();
		Set<AbstractBlock> nBlocks = new HashSet<>(blocks);
		Set<String> uComparisons = new HashSet<>();
		for (AbstractBlock block : nBlocks) {
			ComparisonIterator iterator = block.getComparisonIterator();
			while (iterator.hasNext()) {
				Comparison comparison = iterator.next();

				if (!qIds.contains(comparison.getEntityId1()) && !qIds.contains(comparison.getEntityId2()))
					continue;

				String uniqueComp = "";
				if (comparison.getEntityId1() > comparison.getEntityId2())
					uniqueComp = comparison.getEntityId1() + "u" + comparison.getEntityId2();
				else
					uniqueComp = comparison.getEntityId2() + "u" + comparison.getEntityId1();
				if (uComparisons.contains(uniqueComp))
					continue;
				uComparisons.add(uniqueComp);

				double similarity = ProfileComparison.getJaroSimilarity(
						newData2.get(comparison.getEntityId1()),
						newData2.get(comparison.getEntityId2()),
						keyIndex);

				comparisons++;
				if (similarity >= 0.92 ) {
					matches.add(uniqueComp);
					uFind.union(comparison.getEntityId1(), comparison.getEntityId2());
				}
			}
		}


		if(DEDUPLICATION_EXEC_LOGGER.isDebugEnabled()) {
			DEDUPLICATION_EXEC_LOGGER.debug("Matches Found " + matches.size());
			DEDUPLICATION_EXEC_LOGGER.debug("Total Comparisons " + comparisons);
		}
		EntityResolvedTuple eRT = new EntityResolvedTuple(newData2, uFind);		
		eRT.getAll2();
		return eRT;

	}
	
	private EntityResolvedTuple comparisonExecutionFast(List<AbstractBlock> blocks, Set<Integer> qIds, Integer keyIndex,
			Integer noOfFields) {
		int comparisons = 0;
		UnionFind uFind = new UnionFind(qIds);
		
		Set<String> matches = new HashSet<>();
		Set<AbstractBlock> nBlocks = new HashSet<>(blocks);
		Set<String> uComparisons = new HashSet<>();
		for (AbstractBlock block : nBlocks) {
			ComparisonIterator iterator = block.getComparisonIterator();
			while (iterator.hasNext()) {
				Comparison comparison = iterator.next();

				if (!qIds.contains(comparison.getEntityId1()) && !qIds.contains(comparison.getEntityId2()))
					continue;

				String uniqueComp = "";
				if (comparison.getEntityId1() > comparison.getEntityId2())
					uniqueComp = comparison.getEntityId1() + "u" + comparison.getEntityId2();
				else
					uniqueComp = comparison.getEntityId2() + "u" + comparison.getEntityId1();
				if (uComparisons.contains(uniqueComp))
					continue;
				uComparisons.add(uniqueComp);

				double similarity = ProfileComparison.getJaroSimilarity(
						newData3.get(comparison.getEntityId1()),
						newData3.get(comparison.getEntityId2()),
						keyIndex);

				comparisons++;
				if (similarity >= 0.92 ) {
					matches.add(uniqueComp);
					uFind.union(comparison.getEntityId1(), comparison.getEntityId2());
				}
			}
		}


		if(DEDUPLICATION_EXEC_LOGGER.isDebugEnabled()) {
			DEDUPLICATION_EXEC_LOGGER.debug("Matches Found " + matches.size());
			DEDUPLICATION_EXEC_LOGGER.debug("Total Comparisons " + comparisons);
		}
		EntityResolvedTuple eRT = new EntityResolvedTuple(newData3, uFind);		
		eRT.getAll3();
		return eRT;
	}

}