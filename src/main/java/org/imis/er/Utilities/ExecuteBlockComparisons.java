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

	private HashMap<Integer, Object[]> newData = new HashMap<>();
	protected static final Logger DEDUPLICATION_EXEC_LOGGER =  LoggerFactory.getLogger(DeduplicationExecution.class);

	public ExecuteBlockComparisons(HashMap<Integer, Object[]> newData) {
		this.newData = newData;
	}

	public EntityResolvedTuple comparisonExecutionAll(List<AbstractBlock> blocks, Set<Integer> qIds,
			Integer keyIndex, Integer noOfFields) {
		return comparisonExecutionJdk(blocks, qIds, keyIndex, noOfFields);
	}


	@SuppressWarnings({ "rawtypes", "unchecked" })
	public EntityResolvedTuple comparisonExecutionJdk(List<AbstractBlock> blocks, Set<Integer> qIds,
			Integer keyIndex, Integer noOfFields) {
		int comparisons = 0;
		UnionFind uFind = new UnionFind(qIds);
		
		Set<String> matches = new HashSet<>();
		Set<AbstractBlock> nBlocks = new HashSet<>(blocks);
		Set<String> uComparisons = new HashSet<>();
		double compTime = 0.0;
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

				Object[] entity1 = newData.get(comparison.getEntityId1());
				Object[] entity2 = newData.get(comparison.getEntityId2());

				double compStartTime = System.currentTimeMillis();
				double similarity = ProfileComparison.getJaroSimilarity(entity1, entity2, keyIndex);
				double compEndTime = System.currentTimeMillis();
				compTime += compEndTime - compStartTime;
				comparisons++;
				if (similarity >= 0.92) {
					matches.add(uniqueComp);
					uFind.union(comparison.getEntityId1(), comparison.getEntityId2());
				}
			}
		}	
		EntityResolvedTuple eRT = new EntityResolvedTuple(newData, uFind, keyIndex, noOfFields);	
		eRT.setComparisons(comparisons);
		eRT.setMatches(matches.size());
		eRT.setCompTime(compTime/1000);
		eRT.getAll();
		return eRT;

	}
	
}