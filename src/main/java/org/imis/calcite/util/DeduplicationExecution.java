package org.imis.calcite.util;

import gnu.trove.map.hash.TIntObjectHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.util.Sources;
import org.imis.calcite.adapter.csv.CsvEnumerator;
import org.imis.calcite.adapter.csv.CsvFieldType;
import org.imis.er.BlockIndex.QueryBlockIndex;
import org.imis.er.DataStructures.AbstractBlock;
import org.imis.er.DataStructures.Comparison;
import org.imis.er.DataStructures.DecomposedBlock;
import org.imis.er.DataStructures.EntityResolvedTuple;
import org.imis.er.DataStructures.UnilateralBlock;
import org.imis.er.EfficiencyLayer.BlockRefinement.ComparisonsBasedBlockPurging;
import org.imis.er.MetaBlocking.BlockFiltering;
import org.imis.er.MetaBlocking.EfficientEdgePruning;
import org.imis.er.Utilities.ComparisonIterator;
import org.imis.er.Utilities.ExecuteBlockComparisons;
import org.imis.er.Utilities.SerializationUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private static String pathToPropertiesFile = "deduplication.properties";
	private static Properties properties;

	private static final String USE_FILTER = "use.filter";
	private static final String USE_PURGING = "use.purging";
	private static final String USE_PRUNING = "use.pruning";

	private static final String FILTER_PARAM = "filter.param";
	
	private static boolean usePurging = true;
	private static boolean useFilter = true;
	private static boolean usePruning = true;
	private static double filterParam = 0.0;
	
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T, TKey> EntityResolvedTuple deduplicateEnumerator(Enumerable<T> enumerable, String tableName,
    		Integer key, String source, List<CsvFieldType> fieldTypes, AtomicBoolean ab) {
    	double setPropertiesStartTime = System.currentTimeMillis();
    	setProperties();
    	double setPropertiesTime = (System.currentTimeMillis() - setPropertiesStartTime);
    	
        double deduplicateStartTime = System.currentTimeMillis() - setPropertiesTime;
        CsvEnumerator<T> originalEnumerator = new CsvEnumerator(Sources.of(new File(source)), ab, fieldTypes);
        
        List<T> queryData = enumerable.toList();
        String queryDataSize = Integer.toString(queryData.size());
        
        double blockingStartTime = System.currentTimeMillis();
        QueryBlockIndex queryBlockIndex = new QueryBlockIndex();
        queryBlockIndex.createBlockIndex(queryData, key);
        queryBlockIndex.buildQueryBlocks();
        queryData.clear(); // no more need for query data
        Set<Integer> qIds = queryBlockIndex.getIds();

        double blockJoinStart = System.currentTimeMillis();
        List<AbstractBlock> blocks = queryBlockIndex
                .joinBlockIndices(tableName);
        double blockJoinEnd = System.currentTimeMillis();
        String blockJoinTime = Double.toString((blockJoinEnd - blockJoinStart) / 1000);
        double blockingEndTime = System.currentTimeMillis();
        String blockingTime = Double.toString((blockingEndTime - blockingStartTime) / 1000);

        
        String blocksSize = Integer.toString(blocks.size());
        String blockSizes = getBlockSizes(blocks);
        String blockEntities = Integer.toString(queryBlockIndex.blocksToEntities(blocks).size());

        // PURGING
        double blockPurgingStartTime = System.currentTimeMillis();
        if(usePurging) {
	        ComparisonsBasedBlockPurging blockPurging = new ComparisonsBasedBlockPurging();
	        blockPurging.applyProcessing(blocks);
        }
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
            if(useFilter) {
            	if(filterParam == 0.0) 
            		filterParam = calculateFilterParam(entities, comps);
	            BlockFiltering bFiltering = new BlockFiltering(filterParam);
	            bFiltering.applyProcessing(blocks);
            }
            double blockFilteringEndTime = System.currentTimeMillis();
            filterBlocksSize = Integer.toString(blocks.size());
            
            filterTime = Double.toString((blockFilteringEndTime - blockFilteringStartTime) / 1000);
            filterBlockSizes = getBlockSizes(blocks);
            filterBlockEntities = Integer.toString(queryBlockIndex.blocksToEntities(blocks).size());
            // EDGE PRUNING
            if(usePruning) {
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
        }

        //Get ids of final entities, and add back qIds that were cut from m-blocking
        Set<Integer> totalIds = new HashSet<>();
        if(usePruning)
        	totalIds = queryBlockIndex.blocksToEntitiesD(blocks);
        else
        	totalIds = queryBlockIndex.blocksToEntities(blocks);
        totalIds.addAll(qIds);

        double storeTime = storeIds(qIds);
        double tableScanStartTime = System.currentTimeMillis() - storeTime;
        AbstractEnumerable<Object[]> comparisonEnumerable = createEnumerable((CsvEnumerator<Object[]>) originalEnumerator, totalIds, key);
        double tableScanEndTime = System.currentTimeMillis();
        String tableScanTime = Double.toString((tableScanEndTime - tableScanStartTime) / 1000);

        HashMap<Integer, Object[]> entityMap = createMap(comparisonEnumerable, key);
        // To find ground truth statistics
        storeTime = storeBlocks(blocks, tableName);

        double comparisonStartTime = System.currentTimeMillis() - storeTime;
        ExecuteBlockComparisons ebc = new ExecuteBlockComparisons(entityMap);
        EntityResolvedTuple entityResolvedTuple = ebc.comparisonExecutionAll(blocks, qIds, key, fieldTypes.size());
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

        return entityResolvedTuple;
    }

    public static double calculateFilterParam(int purgeBlockEntities, double totalComps) {
		//double p = 1.986 * Math.pow(purgeBlockEntities, -0.105);
    	//double p = (-3.911 * Math.log(purgeBlockEntities) + 96.7)/100;
    	double p = 1.18 * Math.pow(totalComps, -0.045);
		if(p > 0.6) return 0.6;
		else if(p < 0.3) return 0.3;
		else return p;
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

	private static void getBlockDistribution(List<AbstractBlock> blocks, String tableName) throws IOException {
        // TODO Auto-generated method stub
        File csvFile = new File("./data/blockDistr_" + tableName + ".csv");
        FileWriter csvWriter = new FileWriter(csvFile);
        csvWriter.append("size\n");
        for (AbstractBlock block : blocks) {
            csvWriter.append(Double.toString(block.getNoOfComparisons()) + "\n");
        }
        csvWriter.flush();
        csvWriter.close();
    }
	

    private static TIntObjectHashMap<Object[]> createMapTrove(AbstractEnumerable<Object[]> enumerable, Integer key) {
        List<Object[]> entityList = enumerable.toList();
        TIntObjectHashMap<Object[]> entityMap = new TIntObjectHashMap<Object[]>(entityList.size());
        for (Object[] entity : entityList) {
            entityMap.put(Integer.parseInt(entity[key].toString()), entity);
        }
        return entityMap;
    }

    private static double storeIds(Set<Integer> qIds) {
    	double startTime = System.currentTimeMillis();
        SerializationUtilities.storeSerializedObject(qIds, "./data/qIds");
        return System.currentTimeMillis() - startTime;
    }

    private static double storeBlocks(List<AbstractBlock> blocks, String tableName) {
    	double startTime = System.currentTimeMillis();
        List<DecomposedBlock> dBlocks = (List<DecomposedBlock>) (List<? extends AbstractBlock>) blocks;
        SerializationUtilities.storeSerializedObject(dBlocks, "./data/blocks/" + tableName);
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
    
    private static void setProperties() {
		properties = loadProperties();
		if(!properties.isEmpty()) {
			usePurging = Boolean.parseBoolean(properties.getProperty(USE_PURGING));
			useFilter = Boolean.parseBoolean(properties.getProperty(USE_FILTER));
			usePruning = Boolean.parseBoolean(properties.getProperty(USE_PRUNING));
			filterParam = Double.parseDouble(properties.getProperty(FILTER_PARAM));
		}
	}
}