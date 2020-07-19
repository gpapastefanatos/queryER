package org.imis.calcite.util;

import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;
import org.imis.calcite.adapter.csv.CsvEnumerator;
import org.imis.er.BlockIndex.QueryBlockIndex;
import org.imis.er.DataStructures.AbstractBlock;
import org.imis.er.DataStructures.DecomposedBlock;
import org.imis.er.DataStructures.EntityResolvedTuple;
import org.imis.er.EfficiencyLayer.BlockRefinement.ComparisonsBasedBlockPurging;
import org.imis.er.MetaBlocking.BlockFiltering;
import org.imis.er.Utilities.ExecuteBlockComparisons;
import org.imis.er.Utilities.UnionFind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * A dirty right/left join takes as input one entityResolvedTuples and an enumerable.
 * The entityResolvedTuple contains the data + a hashset that for each of the entities
 * describes its duplicates as found by the comparison execution. It is a hashjoin, that hashes the
 * smallest table by the join column and the big table by its id.
 * <p>
 * We enumerate the second hash and for each entity (id only maps to one entity) we get the entities from the
 * right that join with it. Then for both the left and right entities we get their duplicates and perform the
 * outer product of these two sets joining all of the entities.
 * During these process we keep track of the already visited ids of each table so we don't perform the same join
 * twice.
 */
public class DeduplicationJoinExecution {

    protected static final Logger DEDUPLICATION_EXEC_LOGGER = LoggerFactory.getLogger(DeduplicationExecution.class);

    /**
     * Executes the algorithm of the dirty right join.
     * Can deduplicate the table immediately after the scan or by getting the dirty matches first to
     * minimize the right table entities.
     *
     * @param <TSource>            Can be used to make the Object[] type of the entities generic
     * @param <TInner>             Can be used to make the Object[] type of the entities generic
     * @param <TKey>               Can be used to make the Integer type of the entities generic
     * @param <TResult>            Can be used to make the Object[] type of the entities generic
     * @param <TOuter>             Can be used to make the Object[] type of the entities generic
     * @param outer                Outer data tuple
     * @param inner                Inner data enumerable
     * @param outerKeySelector     Function that gets key column from entity of outer
     * @param innerKeySelector     Function that gets key column from entity of inner
     * @param resultSelector       Function that generates the merged entities after the join
     * @param keyRight             Right table key position
     * @param rightTableName       Right table Name
     * @param rightTableSize       Right table column length
     * @param comparer             Used for joins
     * @param generateNullsOnLeft  Used for joins
     * @param generateNullsOnRight Used for joins
     * @param predicate            Used for joinss
     * @return EntityResolvedTuple that contains the joined entities and their union find data
     */
    @SuppressWarnings("rawtypes")
    public static <TSource, TInner, TKey, TResult, TOuter> EntityResolvedTuple dirtyRightJoin(EntityResolvedTuple outer,
                                                                                              Enumerable<Object[]> inner,
                                                                                              Function1<Object[], TKey> outerKeySelector,
                                                                                              Function1<Object[], TKey> innerKeySelector,
                                                                                              Function2<Object[], Object[], Object[]> resultSelector,
                                                                                              Integer keyRight,
                                                                                              String rightTableName,
                                                                                              Integer rightTableSize,
                                                                                              EqualityComparer<TKey> comparer,
                                                                                              boolean generateNullsOnLeft, boolean generateNullsOnRight,
                                                                                              Predicate2<Object[], Object[]> predicate
    ) {

        List<Object[]> filteredData = new ArrayList<>();

        filteredData = getDirtyMatches((Enumerable<Object[]>) Linq4j.asEnumerable(outer.finalData),
                inner, outerKeySelector, innerKeySelector, resultSelector, comparer,
                generateNullsOnLeft, generateNullsOnRight, predicate, keyRight);
//		filteredData = inner.toList();

        //Deduplicate the dirty table
        EntityResolvedTuple entityResolvedTuple = deduplicate(filteredData, keyRight, rightTableSize,
                rightTableName, (CsvEnumerator<Object[]>) inner.enumerator());
        List<Object[]> joinedEntities =
                deduplicateJoin(outer, entityResolvedTuple, outerKeySelector, innerKeySelector,
                        resultSelector, comparer, generateNullsOnLeft, generateNullsOnRight, predicate);
        if (DEDUPLICATION_EXEC_LOGGER.isDebugEnabled())
            DEDUPLICATION_EXEC_LOGGER.debug("Joined size: " + joinedEntities.size());
        EntityResolvedTuple joinedEntityResolvedTuple = new EntityResolvedTuple(joinedEntities, null);

        return joinedEntityResolvedTuple;
    }

    /**
     * Executes the algorithm of the dirty left join.
     * Can deduplicate the table immediately after the scan or by getting the dirty matches first to
     * minimize the left table entities.
     *
     * @param <TSource>           Can be used to make the Object[] type of the entities generic
     * @param <TInner>            Can be used to make the Object[] type of the entities generic
     * @param <TKey>              Can be used to make the Integer type of the entities generic
     * @param <TResult>           Can be used to make the Object[] type of the entities generic
     * @param <TOuter>            Can be used to make the Object[] type of the entities generic
     * @param outer               Outer data tuple
     * @param inner               Inner data enumerable
     * @param outerKeySelector    Function that gets key column from entity of outer
     * @param innerKeySelector    Function that gets key column from entity of inner
     * @param resultSelector      Function that generates the merged entities after the join
     * @param keyleft             left table key position
     * @param leftTableName       left table Name
     * @param leftTableSize       left table column length
     * @param comparer            Used for joins
     * @param generateNullsOnLeft Used for joins
     * @param generateNullsOnleft Used for joins
     * @param predicate           Used for joinss
     * @return EntityResolvedTuple that contains the joined entities and their union find data
     */
    @SuppressWarnings("rawtypes")
    public static <TSource, TInner, TKey, TResult, TOuter> EntityResolvedTuple dirtyLeftJoin(Enumerable<Object[]> outer,
                                                                                             EntityResolvedTuple<Object[]> inner,
                                                                                             Function1<Object[], TKey> outerKeySelector,
                                                                                             Function1<Object[], TKey> innerKeySelector,
                                                                                             Function2<Object[], Object[], Object[]> resultSelector,
                                                                                             Integer keyLeft,
                                                                                             String leftTableName,
                                                                                             Integer leftTableSize,
                                                                                             EqualityComparer<TKey> comparer,
                                                                                             boolean generateNullsOnLeft, boolean generateNullsOnRight,
                                                                                             Predicate2<Object[], Object[]> predicate
    ) {

        List<Object[]> filteredData = new ArrayList<>();

        filteredData = getDirtyMatches(Linq4j.asEnumerable(inner.finalData), outer,
                innerKeySelector, outerKeySelector, resultSelector, comparer,
                generateNullsOnLeft, generateNullsOnRight, predicate, keyLeft);
//		filteredData = inner.toList();

        // Deduplicate the dirty table
        EntityResolvedTuple entityResolvedTuple = deduplicate(filteredData, keyLeft, leftTableSize,
                leftTableName, (CsvEnumerator<Object[]>) outer.enumerator());
        // Reverse the inner, outer structure
        List<Object[]> joinedEntities =
                deduplicateJoin(entityResolvedTuple, inner, outerKeySelector, innerKeySelector,
                        resultSelector, comparer, generateNullsOnLeft, generateNullsOnRight, predicate);
        if (DEDUPLICATION_EXEC_LOGGER.isDebugEnabled())
            DEDUPLICATION_EXEC_LOGGER.debug("Joined size: " + joinedEntities.size());
        EntityResolvedTuple joinedEntityResolvedTuple = new EntityResolvedTuple(joinedEntities, null);

        return joinedEntityResolvedTuple;
    }


    /**
     * Executes the algorithm of the dirty-dirty join.
     * Can deduplicate the table immediately after the scan or by getting the dirty matches first to
     * minimize the left table entities.
     *
     * @param <TSource>           Can be used to make the Object[] type of the entities generic
     * @param <TInner>            Can be used to make the Object[] type of the entities generic
     * @param <TKey>              Can be used to make the Integer type of the entities generic
     * @param <TResult>           Can be used to make the Object[] type of the entities generic
     * @param <TOuter>            Can be used to make the Object[] type of the entities generic
     * @param outer               Outer data tuple
     * @param inner               Inner data enumerable
     * @param outerKeySelector    Function that gets key column from entity of outer
     * @param innerKeySelector    Function that gets key column from entity of inner
     * @param resultSelector      Function that generates the merged entities after the join
     * @param keyleft             left table key position
     * @param leftTableName       left table Name
     * @param leftTableSize       left table column length
     * @param comparer            Used for joins
     * @param generateNullsOnLeft Used for joins
     * @param generateNullsOnleft Used for joins
     * @param predicate           Used for joinss
     * @return EntityResolvedTuple that contains the joined entities and their union find data
     */
    @SuppressWarnings("rawtypes")
    public static <TSource, TInner, TKey, TResult, TOuter> EntityResolvedTuple dirtyJoin(Enumerable<Object[]> outer,
                                                                                         Enumerable<Object[]> inner,
                                                                                         Function1<Object[], TKey> outerKeySelector,
                                                                                         Function1<Object[], TKey> innerKeySelector,
                                                                                         Function2<Object[], Object[], Object[]> resultSelector,
                                                                                         Integer keyLeft,
                                                                                         String leftTableName,
                                                                                         Integer leftTableSize,
                                                                                         Integer keyRight,
                                                                                         String rightTableName,
                                                                                         Integer rightTableSize,
                                                                                         EqualityComparer<TKey> comparer,
                                                                                         boolean generateNullsOnLeft, boolean generateNullsOnRight,
                                                                                         Predicate2<Object[], Object[]> predicate
    ) {

        List<List<Object[]>> filteredData = getAllDirtyMatches(outer, inner,
                outerKeySelector, innerKeySelector, resultSelector, comparer,
                generateNullsOnLeft, generateNullsOnRight, predicate, keyRight);

        return null;
    }

    /**
     * Clean join performs the join algorithm on two cleaned datasets
     */
    @SuppressWarnings("rawtypes")
    public static <TSource, TInner, TKey, TResult, TOuter> EntityResolvedTuple cleanJoin(EntityResolvedTuple<Object[]> outer,
                                                                                         EntityResolvedTuple<Object[]> inner,
                                                                                         Function1<Object[], TKey> outerKeySelector,
                                                                                         Function1<Object[], TKey> innerKeySelector,
                                                                                         Function2<Object[], Object[], Object[]> resultSelector,
                                                                                         EqualityComparer<TKey> comparer,
                                                                                         boolean generateNullsOnLeft, boolean generateNullsOnleft,
                                                                                         Predicate2<Object[], Object[]> predicate
    ) {


        List<Object[]> joinedEntities =
                deduplicateJoin(outer, inner, outerKeySelector, innerKeySelector,
                        resultSelector, comparer, generateNullsOnLeft, generateNullsOnleft, predicate);
        if (DEDUPLICATION_EXEC_LOGGER.isDebugEnabled())
            DEDUPLICATION_EXEC_LOGGER.debug("Joined size: " + joinedEntities.size());
        EntityResolvedTuple joinedEntityResolvedTuple = new EntityResolvedTuple(joinedEntities, null);

        return joinedEntityResolvedTuple;
    }

    private static HashMap<Integer, Object[]> createMap(AbstractEnumerable<Object[]> enumerable, Integer key) {
        return (HashMap<Integer, Object[]>) enumerable.toMap(
                new Function1<Object[], Integer>() {
                    @Override
                    public Integer apply(Object[] entity) {
                        // TODO Auto-generated method stub
                        String entityKey = entity[key].toString();
                        if (!entityKey.contentEquals(""))
                            return Integer.parseInt(entity[key].toString());
                        return null;
                    }
                }
        );
    }

    /**
     * Implements a faux-join only to get the entities that match for the hashing table.
     * This way we can perform deduplication on a subset of the data.
     *
     * @param <TSource>
     * @param <TInner>
     * @param <TKey>
     * @param outer                Left data enumerable
     * @param inner                Right data enumerable
     * @param outerKeySelector     Function that gets key column from entity of outer
     * @param innerKeySelector     Function that gets key column from entity of inner
     * @param resultSelector       Function that generates the merged entities after the join
     * @param comparer             Used for joins
     * @param generateNullsOnLeft  Used for joins
     * @param generateNullsOnRight Used for joins
     * @param predicate            Used for joinss
     * @return
     */
    @SuppressWarnings("unused")
    private static <TSource, TInner, TKey> List<Object[]> getDirtyMatches(Enumerable<Object[]> outer,
                                                                          Enumerable<Object[]> inner,
                                                                          Function1<Object[], TKey> outerKeySelector,
                                                                          Function1<Object[], TKey> innerKeySelector,
                                                                          Function2<Object[], Object[], Object[]> resultSelector,
                                                                          EqualityComparer<TKey> comparer,
                                                                          boolean generateNullsOnLeft, boolean generateNullsOnRight,
                                                                          Predicate2<Object[], Object[]> predicate,
                                                                          Integer key) {

        final Enumerable<Object[]> innerToLookUp = generateNullsOnLeft
                ? Linq4j.asEnumerable(inner.toList())
                : inner;


        final Lookup<TKey, Object[]> innerLookup =
                comparer == null
                        ? innerToLookUp.toLookup(innerKeySelector)
                        : innerToLookUp
                        .toLookup(innerKeySelector, comparer);
        Enumerator<Object[]> outers = outer.enumerator();
        Enumerator<Object[]> inners = Linq4j.emptyEnumerator();
        List<Object[]> innersUnmatched =
                generateNullsOnLeft
                        ? new ArrayList<>(innerToLookUp.toList())
                        : null;
        final Set<Integer> dirtyIds = new HashSet<>();
        final List<Object[]> dirtyData = new ArrayList<>();

        for (; ; ) {

            if (!outers.moveNext()) {
                break;
            }
            final Object[] outer2 = outers.current();
            Enumerable<Object[]> innerEnumerable;
            if (outer2 == null) {
                innerEnumerable = null;
            } else {
                final TKey outerKey = outerKeySelector.apply(outer2);
                if (outerKey == null) {
                    innerEnumerable = null;
                } else {
                    innerEnumerable = innerLookup.get(outerKey);
                    //apply predicate to filter per-row
                    if (innerEnumerable != null) {
                        try (Enumerator<Object[]> innerEnumerator =
                                     innerEnumerable.enumerator()) {
                            while (innerEnumerator.moveNext()) {
                                final Object[] inner2 = innerEnumerator.current();
                                String inner2Key = inner2[key].toString();
                                if (inner2Key.contentEquals("")) continue;
                                if (!dirtyIds.contains(Integer.parseInt(inner2Key))) {
                                    dirtyIds.add(Integer.parseInt(inner2Key));
                                    dirtyData.add(inner2);
                                }
                            }
                        }
                    }
                }
            }
        }
        return dirtyData;
    }

    /**
     * Implements a faux-join only to get the entities that match for the hashing table.
     * This way we can perform deduplication on a subset of the data but for both tables
     *
     * @param <TSource>
     * @param <TInner>
     * @param <TKey>
     * @param outer                Left data enumerable
     * @param inner                Right data enumerable
     * @param outerKeySelector     Function that gets key column from entity of outer
     * @param innerKeySelector     Function that gets key column from entity of inner
     * @param resultSelector       Function that generates the merged entities after the join
     * @param comparer             Used for joins
     * @param generateNullsOnLeft  Used for joins
     * @param generateNullsOnRight Used for joins
     * @param predicate            Used for joinss
     * @return
     */
    @SuppressWarnings("unused")
    private static <TSource, TInner, TKey> List<List<Object[]>> getAllDirtyMatches(Enumerable<Object[]> outer,
                                                                                   Enumerable<Object[]> inner,
                                                                                   Function1<Object[], TKey> outerKeySelector,
                                                                                   Function1<Object[], TKey> innerKeySelector,
                                                                                   Function2<Object[], Object[], Object[]> resultSelector,
                                                                                   EqualityComparer<TKey> comparer,
                                                                                   boolean generateNullsOnLeft, boolean generateNullsOnRight,
                                                                                   Predicate2<Object[], Object[]> predicate,
                                                                                   Integer key) {


        final Enumerable<Object[]> innerToLookUp = generateNullsOnLeft
                ? Linq4j.asEnumerable(inner.toList())
                : inner;


        final Lookup<TKey, Object[]> innerLookup =
                comparer == null
                        ? innerToLookUp.toLookup(innerKeySelector)
                        : innerToLookUp
                        .toLookup(innerKeySelector, comparer);
        Enumerator<Object[]> outers = outer.enumerator();
        Enumerator<Object[]> inners = Linq4j.emptyEnumerator();
        List<Object[]> innersUnmatched =
                generateNullsOnLeft
                        ? new ArrayList<>(innerToLookUp.toList())
                        : null;
        final Set<Integer> dirtyIds = new HashSet<>();
        final List<Object[]> innerDirtyData = new ArrayList<>();
        final List<Object[]> outerDirtyData = new ArrayList<>();
        for (; ; ) {

            if (!outers.moveNext()) {
                break;
            }
            final Object[] outer2 = outers.current();
            Enumerable<Object[]> innerEnumerable;
            if (outer2 == null) {
                innerEnumerable = null;
            } else {
                final TKey outerKey = outerKeySelector.apply(outer2);
                if (outerKey == null) {
                    innerEnumerable = null;
                } else {
                    innerEnumerable = innerLookup.get(outerKey);
                    //apply predicate to filter per-row
                    if (innerEnumerable != null) {
                        outerDirtyData.add(outer2);
                        try (Enumerator<Object[]> innerEnumerator =
                                     innerEnumerable.enumerator()) {
                            while (innerEnumerator.moveNext()) {
                                final Object[] inner2 = innerEnumerator.current();
                                String inner2Key = inner2[key].toString();
                                if (inner2Key.contentEquals("")) continue;
                                if (!dirtyIds.contains(Integer.parseInt(inner2Key))) {
                                    dirtyIds.add(Integer.parseInt(inner2Key));
                                    innerDirtyData.add(inner2);
                                }
                            }
                        }
                    }
                }
            }
        }
        List<List<Object[]>> finalTuple = new ArrayList<>();
        finalTuple.add(outerDirtyData);
        finalTuple.add(innerDirtyData);
        return finalTuple;
    }

    /**
     * Executes a deduplicated join, that takes as input two enity resolved tuples and returns another entity resolved join tuple
     * The joined table gets a new column that contains the identification numbers of each row. This is needed to identify the duplicate
     * entities in the Union Find.
     *
     * @param <TSource>
     * @param <TInner>
     * @param <TKey>
     * @param outer                Outer deduplicated tuple
     * @param inner                Inner deduplicated tuple
     * @param outerKeySelector     Function that gets key column from entity of outer
     * @param innerKeySelector     Function that gets key column from entity of inner
     * @param resultSelector       Function that generates the merged entities after the join
     * @param comparer             Used for joins
     * @param generateNullsOnLeft  Used for joins
     * @param generateNullsOnRight Used for joins
     * @param predicate            Used for joinss
     * @return
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static <TSource, TInner, TKey> List<Object[]> deduplicateJoin(EntityResolvedTuple outer,
                                                                          EntityResolvedTuple inner,
                                                                          Function1<Object[], TKey> outerKeySelector,
                                                                          Function1<Object[], TKey> innerKeySelector,
                                                                          Function2<Object[], Object[], Object[]> resultSelector,
                                                                          EqualityComparer<TKey> comparer,
                                                                          boolean generateNullsOnLeft, boolean generateNullsOnRight,
                                                                          Predicate2<Object[], Object[]> predicate) {

        double deduplicateJoinStartTime = System.currentTimeMillis();
        final Enumerable<Object[]> innerToLookUp = Linq4j.asEnumerable(inner.finalData);
        System.out.println(outer.finalData.size());
        System.out.println(inner.finalData.size());
        final Lookup<TKey, Object[]> innerLookup =
                comparer == null
                        ? innerToLookUp.toLookup(innerKeySelector)
                        : innerToLookUp
                        .toLookup(innerKeySelector, comparer);

        HashMap<Integer, Object[]> outersMap = outer.data;
        HashMap<Integer, Object[]> innersMap = inner.data;

        HashMap<Integer, Set<Integer>> outerMatches = outer.revUF;
        HashMap<Integer, Set<Integer>> innerMatches = inner.revUF;

        Set<Integer> outerCheckedIds = new HashSet<>();
        Set<Integer> joinedIds = new HashSet<>();
        Integer joinedId = 0; // outer id to enumerate the duplicates
        joinedIds.add(joinedId);
        UnionFind joinedUFind = new UnionFind(joinedIds);
        List<Object[]> joinedEntities = new ArrayList<>();

        for (Integer outerId : outersMap.keySet()) {
            Integer innerJoinedId = joinedId; // for all the inner ids that an outer and its duplicates contain
            Object[] outerCurrent = outersMap.get(outerId);
            if (outerCurrent == null) continue;

            TKey outerKey = outerKeySelector.apply(outerCurrent);
            if (!outerCheckedIds.contains(outerId)) {
                Enumerable<Object[]> innerEnumerable = innerLookup.get(outerKey);
                if (innerEnumerable != null) {
                    //System.out.println("JoinedId: " + joinedId);
                    joinedUFind.union(joinedId, joinedId);

                    Set<Integer> outerMatchedIds = outerMatches.get(outerId);
                    Set<Integer> innerCheckedIds = new HashSet<>();
                    //System.out.print(outerId);
                    if (outerMatchedIds != null) {
                        outerCheckedIds.addAll(outerMatchedIds);

                        //System.out.print(": " + outerMatchedIds.toString());
                        for (Integer outerMatchedId : outerMatchedIds) {
                            outerCurrent = outersMap.get(outerMatchedId);

                            try (Enumerator<Object[]> innerEnumerator =
                                         innerEnumerable.enumerator()) {
                                while (innerEnumerator.moveNext()) {
                                    final Object[] innerMatched = innerEnumerator.current();
                                    if (!innerCheckedIds.contains(innerMatched[0])) {
                                        //Get duplicates of each inner entity
                                        Set<Integer> innerMatchedIds = innerMatches.get(Integer.parseInt(innerMatched[0].toString()));

                                        if (innerMatchedIds != null) {
                                            //System.out.println("YES/YES");

                                            innerCheckedIds.addAll(innerMatchedIds);
                                            // For each of the inner matched entities get the duplicates.
                                            // Also check that they are not already in the lookup
                                            for (Integer innerMatchedId : innerMatchedIds) {
                                                //System.out.println(joinedId + " " + innerJoinedId);
                                                // Merge the outer, inner, innerJoinedId
                                                Object[] innerCurrent = innersMap.get(innerMatchedId);
                                                Object[] joinedEntity = resultSelector.apply(outerCurrent, innerCurrent);
                                                joinedEntity = appendValue(joinedEntity, innerJoinedId);
                                                joinedEntities.add(joinedEntity);
                                                joinedUFind.union(joinedId, innerJoinedId);
                                                innerJoinedId += 1;

                                            }

                                        } else {
                                            //System.out.println("YES/NO");
                                            //System.out.println(joinedId + " " + innerJoinedId);
                                            // Merge the outer, inner, innerJoinedId
                                            Object[] joinedEntity = resultSelector.apply(outerCurrent, innerMatched);
                                            joinedEntity = appendValue(joinedEntity, innerJoinedId);
                                            joinedEntities.add(joinedEntity);
                                            joinedUFind.union(joinedId, innerJoinedId);
                                            innerJoinedId += 1;

                                        }

                                    }

                                }
                            }
                        }
                    } else {
                        try (Enumerator<Object[]> innerEnumerator =
                                     innerEnumerable.enumerator()) {
                            while (innerEnumerator.moveNext()) {
                                final Object[] innerMatched = innerEnumerator.current();
                                if (!innerCheckedIds.contains(innerMatched[0])) {
                                    //Get duplicates of each inner entity
                                    Set<Integer> innerMatchedIds = innerMatches.get(Integer.parseInt(innerMatched[0].toString()));
                                    if (innerMatchedIds != null) {
                                        //System.out.println("NO/YES");
                                        innerCheckedIds.addAll(innerMatchedIds);
                                        // For each of the inner matched entities get the duplicates.
                                        // Also check that they are not already in the lookup
                                        for (Integer innerMatchedId : innerMatchedIds) {
                                            //System.out.println(joinedId + " " + innerJoinedId);
                                            // Merge the outer, inner, innerJoinedId
                                            Object[] innerCurrent = innersMap.get(innerMatchedId);
                                            Object[] joinedEntity = resultSelector.apply(outerCurrent, innerCurrent);
                                            joinedEntity = appendValue(joinedEntity, innerJoinedId);
                                            joinedEntities.add(joinedEntity);
                                            joinedUFind.union(joinedId, innerJoinedId);
                                            innerJoinedId += 1;

                                        }

                                    } else {
                                        //System.out.println("NO/NO");
                                        //System.out.println(joinedId + " " + innerJoinedId);
                                        // Merge the outer, inner, innerJoinedId
                                        Object[] joinedEntity = resultSelector.apply(outerCurrent, innerMatched);
                                        joinedEntity = appendValue(joinedEntity, innerJoinedId);
                                        joinedEntities.add(joinedEntity);
                                        joinedUFind.union(joinedId, innerJoinedId);
                                        innerJoinedId += 1;
                                    }

                                }
                            }

                        }
                    }
                    joinedId = innerJoinedId;
                    //System.out.println();
                }

            }
        }
        double deduplicateJoinEndTime = System.currentTimeMillis();
        System.out.println("Deduplicate join time: " + (deduplicateJoinEndTime - deduplicateJoinStartTime) / 1000);

        // Test that its all working good by commenting in the following
//	    HashMap<Integer, Set<Integer>> revUF = new HashMap<>();
//		for (int child : joinedUFind.getParent().keySet()) {
//			revUF.computeIfAbsent(joinedUFind.getParent().get(child), x -> new HashSet<>()).add(child);
//		}
//		for (int id : revUF.keySet()) {
//    	    System.out.print(id + ": " );
//
//			for (Integer idInner : revUF.get(id)) {
//                System.out.print(idInner.toString() + " ");
//			}
//			System.out.println();
//
//		}
//		// Print joined entities
//		for(Object[] joinedEntity : joinedEntities) {
//			for(int j = 0; j < joinedEntity.length; j ++) {
//				System.out.print(joinedEntity[j] + " || ");
//			}
//			System.out.println();
//		}
        return joinedEntities;
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

    private static EntityResolvedTuple deduplicate(List<Object[]> data, Integer key, Integer noOfAttributes,
                                                   String tableName, CsvEnumerator<Object[]> originalEnumerator) {

        double deduplicateStartTime = System.currentTimeMillis();
        Integer hashType = 0; //0 = JDK, 1 = TROVE, 2 = FAST

        double blockingStartTime = System.currentTimeMillis();
        QueryBlockIndex queryBlockIndex = new QueryBlockIndex();
        queryBlockIndex.createBlockIndex(data, key);
        queryBlockIndex.buildQueryBlocks();
        data.clear(); // no more need for query data

        List<AbstractBlock> blocks = queryBlockIndex
                .joinBlockIndices(tableName);
        double blockingEndTime = System.currentTimeMillis();
        System.out.println("Blocking time: " + (blockingEndTime - blockingStartTime) / 1000);

        Set<Integer> qIds = queryBlockIndex.getIds();

        if (DEDUPLICATION_EXEC_LOGGER.isDebugEnabled())
            DEDUPLICATION_EXEC_LOGGER.debug("QueryBLocking - Blocks Ready\t:\t" + blocks.size());
        double metaBlockingStartTime = System.currentTimeMillis();
        //Get ids of final entities
        ComparisonsBasedBlockPurging blockPurging = new ComparisonsBasedBlockPurging();
        blockPurging.applyProcessing(blocks);
        //purge based on number of comparisons, the max_number is calculated dynamically
        if (DEDUPLICATION_EXEC_LOGGER.isDebugEnabled())
            DEDUPLICATION_EXEC_LOGGER.debug("B Purging - Blocks Ready\t:\t" + blocks.size());
        if (blocks.size() > 10) {
            BlockFiltering bFiltering = new BlockFiltering(0.60);
            bFiltering.applyProcessing(blocks);
            if (DEDUPLICATION_EXEC_LOGGER.isDebugEnabled())
                DEDUPLICATION_EXEC_LOGGER.debug("B Filtering - Blocks Ready\t:\t" + blocks.size());
        }
        double metaBlockingEndTime = System.currentTimeMillis();
        System.out.println("Meta Blocking time: " + (metaBlockingEndTime - metaBlockingStartTime) / 1000);

        List<DecomposedBlock> dBlocks = (List<DecomposedBlock>) (List<? extends AbstractBlock>) blocks;
        Set<Integer> totalIds = queryBlockIndex.blocksToEntitiesD(dBlocks);
        AbstractEnumerable<Object[]> comparisonEnumerable = createEnumerable(originalEnumerator, totalIds, key);
        //TIntObjectHashMap<Object[]>  entityMap = createMapTrove(comparisonEnumerable, key);
        //Int2ObjectOpenHashMap<Object[]> entityMap = createMapFast(comparisonEnumerable, key);
        HashMap<Integer, Object[]> entityMap = createMap(comparisonEnumerable, key);

        if (DEDUPLICATION_EXEC_LOGGER.isDebugEnabled())
            DEDUPLICATION_EXEC_LOGGER.debug("Joined Entity Profiles\t:\t" + entityMap.size());
        double comparisonStartTime = System.currentTimeMillis();

        ExecuteBlockComparisons ebc = new ExecuteBlockComparisons(entityMap);

        EntityResolvedTuple entityResolvedTuple = ebc.comparisonExecutionAll(dBlocks, qIds, key, noOfAttributes, hashType);
        double comparisonEndTime = System.currentTimeMillis();
        System.out.println("Comparison execution time: " + (comparisonEndTime - comparisonStartTime) / 1000);
        double deduplicateEndTime = System.currentTimeMillis();
        System.out.println("Total Deduplication time: " + (deduplicateEndTime - deduplicateStartTime) / 1000);

        return entityResolvedTuple;
    }

    /**
     * @param enumerator Enumerator data
     * @param qIds       Qids to pick from enumerator
     * @param key        Key column
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
                            if (entityKey.contentEquals("")) continue;
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
