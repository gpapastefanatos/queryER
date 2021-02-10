package org.imsi.queryEREngine.imsi.calcite.util;

import gnu.trove.map.hash.TIntObjectHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.imsi.queryEREngine.apache.calcite.util.Sources;
import org.imsi.queryEREngine.imsi.calcite.adapter.csv.CsvEnumerator;
import org.imsi.queryEREngine.imsi.calcite.adapter.csv.CsvFieldType;
import org.imsi.queryEREngine.imsi.er.BlockIndex.QueryBlockIndex;
import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.Comparison;
import org.imsi.queryEREngine.imsi.er.DataStructures.DecomposedBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.EntityResolvedTuple;
import org.imsi.queryEREngine.imsi.er.DataStructures.UnilateralBlock;
import org.imsi.queryEREngine.imsi.er.EfficiencyLayer.BlockRefinement.ComparisonsBasedBlockPurging;
import org.imsi.queryEREngine.imsi.er.MetaBlocking.BlockFiltering;
import org.imsi.queryEREngine.imsi.er.MetaBlocking.EfficientEdgePruning;
import org.imsi.queryEREngine.imsi.er.Utilities.ComparisonIterator;
import org.imsi.queryEREngine.imsi.er.Utilities.ExecuteBlockComparisons;
import org.imsi.queryEREngine.imsi.er.Utilities.SerializationUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/*
 * Single table deduplication execution.
 */
public class DeduplicationExecution<T> {

    protected static final Logger DEDUPLICATION_EXEC_LOGGER = LoggerFactory.getLogger(DeduplicationExecution.class);

    /**
     * Performs deduplication on a single table's entities.
     * The steps for performing the resolution are as follows:
     * 1) Get filtered data from filter.
     * 2) Create QueryBlockIndex
     * 3) BlockJoin
     * 4) Apply MetaBlocking
     * 5) Create an enumberable by index scanning with the IDs as derived from the MetaBlocking
     * 6) Execute Block Comparisons to find matches
     *
     * @param enumerable The data after the filter
     * @param tableName  Name of the table used to get the BlockIndex
     * @param key        Key column id of the table for block indexing
     * @param source     Source of the table data for the scan
     * @param fieldTypes Types of the data
     * @param ab         Used for the csv enumerator, nothing else
     * @return EntityResolvedTuple contains the UnionFind + HashMap of the table to be used in merging/join
     * @throws IOException
     */



	private static final String FILTER_PARAM = "filter.param";

	private static double filterParam = 0.0;
	

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T, TKey> EntityResolvedTuple deduplicateEnumerator(Enumerable<T> enumerable, String tableName,
    		Integer key, String source, List<CsvFieldType> fieldTypes, AtomicBoolean ab) {
    	boolean firstDedup = false;
    	double setPropertiesStartTime = System.currentTimeMillis();
    	double setPropertiesTime = (System.currentTimeMillis() - setPropertiesStartTime);
    	System.out.println("Deduplicating: " + tableName);
    	
        double deduplicateStartTime = System.currentTimeMillis() - setPropertiesTime;
        
        CsvEnumerator<Object[]> originalEnumerator = new CsvEnumerator(Sources.of(new File(source)), ab, fieldTypes);
        // Check for links and remove qIds that have links
        double linksStartTime = System.currentTimeMillis();
        HashMap<Integer, Set<Integer>> links = loadLinks(tableName);
        if(links == null) firstDedup = true;
        List<Object[]> queryData = (List<Object[]>) enumerable.toList();
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
    		Set<Integer> linkedIds = getLinkedIds(queryData, key, links,  qIds);
    		totalIds.addAll(linkedIds);  // Add links back
        }
        
        qIdsNoLinks = queryData.stream().map(row -> {
        	return Integer.parseInt(row[key].toString());
        }).collect(Collectors.toSet());

        double linksEndTime = System.currentTimeMillis();
        String linksTime = Double.toString((linksEndTime - linksStartTime) / 1000);

        String queryDataSize = Integer.toString(queryData.size());
        
        double blockingStartTime = System.currentTimeMillis();
        QueryBlockIndex queryBlockIndex = new QueryBlockIndex();
        queryBlockIndex.createBlockIndex(queryData, key);
        queryBlockIndex.buildQueryBlocks();
        double blockingEndTime = System.currentTimeMillis();
        String blockingTime = Double.toString((blockingEndTime - blockingStartTime) / 1000);
        boolean doER = queryData.size() > 0 ? true : false;
        queryData.clear(); // no more need for query data
        
        
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
        blockPurging.applyProcessing(blocks);
        
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
        boolean flag = false;
        if (blocks.size() > 10) {
        	
        	// FILTERING
            double blockFilteringStartTime = System.currentTimeMillis();
            filterParam = 0.35;
            if(tableName.contains("publications")) filterParam = 0.55;
	        BlockFiltering bFiltering = new BlockFiltering(filterParam);
	        bFiltering.applyProcessing(blocks);
            
            double blockFilteringEndTime = System.currentTimeMillis();
            filterBlocksSize = Integer.toString(blocks.size());
            
            filterTime = Double.toString((blockFilteringEndTime - blockFilteringStartTime) / 1000);
            filterBlockSizes = getBlockSizes(blocks);
            filterBlockEntities = Integer.toString(queryBlockIndex.blocksToEntities(blocks).size());
            // EDGE PRUNING
            flag = true;
            double edgePruningStartTime = System.currentTimeMillis();
            EfficientEdgePruning eEP = new EfficientEdgePruning();
            eEP.applyProcessing(blocks);
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
        if(flag)
        	blockQids = queryBlockIndex.blocksToEntitiesD(blocks);
        else
        	blockQids = queryBlockIndex.blocksToEntities(blocks);
        totalIds.addAll(blockQids);
        totalIds.addAll(qIds);
        double storeTime = storeIds(qIds);
        double tableScanStartTime = System.currentTimeMillis() - storeTime;
        AbstractEnumerable<Object[]> comparisonEnumerable = createEnumerable((Enumerator<Object[]>) originalEnumerator, totalIds, key);
        double tableScanEndTime = System.currentTimeMillis();
        String tableScanTime = Double.toString((tableScanEndTime - tableScanStartTime) / 1000);

        HashMap<Integer, Object[]> entityMap = createMap(comparisonEnumerable, key);
        // To find ground truth statistics
//        storeTime = storeBlocks(blocks, tableName);

        double comparisonStartTime = System.currentTimeMillis() - storeTime;
        ExecuteBlockComparisons ebc = new ExecuteBlockComparisons(entityMap);
        EntityResolvedTuple entityResolvedTuple = ebc.comparisonExecutionAll(blocks, qIdsNoLinks, key, fieldTypes.size());
        entityResolvedTuple.getAll();
        entityResolvedTuple.setLinks(links);
        if(!firstDedup) entityResolvedTuple.combineLinks(links);
		entityResolvedTuple.storeLinks(tableName);
		entityResolvedTuple.filterData(totalIds);
        
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

    public static Set<Integer> getLinkedIds(List<Object[]> queryData, 
    		Integer key, Map<Integer, Set<Integer>> links, Set<Integer> qIds) {

    	Set<Integer> totalIds = new HashSet<>();
    	Set<Set<Integer>> sublinks = links.entrySet().stream().filter(entry -> {
    		return qIds.contains(entry.getKey());
    	}).map(entry -> {
    		return entry.getValue();
    	}).collect(Collectors.toSet());
    	for (Set<Integer> sublink : sublinks) {
    		totalIds.addAll(sublink);
    	}   	
    	return totalIds;
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


    private static double storeIds(Set<Integer> qIds) {
    	double startTime = System.currentTimeMillis();
        SerializationUtilities.storeSerializedObject(qIds, "/usr/share/data/qIds");
        return System.currentTimeMillis() - startTime;
    }

    private static double storeBlocks(List<AbstractBlock> blocks, String tableName) {
    	double startTime = System.currentTimeMillis();
        List<DecomposedBlock> dBlocks = (List<DecomposedBlock>) (List<? extends AbstractBlock>) blocks;
        SerializationUtilities.storeSerializedObject(dBlocks, "/usr/share/data/blocks/" + tableName);
        return System.currentTimeMillis() - startTime;
    }

    /**
     * @param <T>
     * @param entityResolvedTuple The tuple as created by the deduplication/join
     * @return AbstractEnumerable that combines the hashmap and the UnionFind to create the merged/fusioned data
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T> AbstractEnumerable<T> mergeEntities(EntityResolvedTuple entityResolvedTuple) {
//		if(entityResolvedTuple.uFind != null)
//			entityResolvedTuple.sortEntities();
//		if(DEDUPLICATION_EXEC_LOGGER.isDebugEnabled()) 
//			DEDUPLICATION_EXEC_LOGGER.debug("Final Size: " + entityResolvedTuple.finalData.size());
    	entityResolvedTuple.groupEntities();
        return entityResolvedTuple;

    }

    /**
     * @param enumerable Data of the table
     * @param key        Key column of the table
     * @return HashMap from key to entity
     */
    private static HashMap<Integer, Object[]> createMap(AbstractEnumerable<Object[]> enumerable, Integer key) {
        List<Object[]> entityList = enumerable.toList();
        HashMap<Integer, Object[]> entityMap = new HashMap<Integer, Object[]>(entityList.size());
        for (Object[] entity : entityList) {
            entityMap.put(Integer.parseInt(entity[key].toString()), entity);
        }
        return entityMap;
    }

    public static HashMap<Integer, Set<Integer>> loadLinks(String table) {
    	if(new File("/usr/share/data/links/" + table).exists())
    		return (HashMap<Integer, Set<Integer>>) SerializationUtilities.loadSerializedObject("/usr/share/data/links/" + table);
    	else  return null;
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
                            if (entityKey.equals("")) continue;
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

}