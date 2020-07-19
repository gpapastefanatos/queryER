package org.imis.er.BlockIndex;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.imis.er.DataStructures.AbstractBlock;
import org.imis.er.DataStructures.Comparison;
import org.imis.er.DataStructures.EntityIndex;
import org.imis.er.Utilities.ComparisonIterator;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BlockIndexStatistic {

	private String tableName;
	protected Map<String, Integer> blocksHistogram;
	protected double totalBlocks;
	protected double validComparisons;
	protected double[] comparisonsPerBlock;
	protected double[] redundantCPE;
	protected double[] comparisonsPerEntity;
	protected List<AbstractBlock> blocks;
	protected EntityIndex entityIndex;
	

	public BlockIndexStatistic() {
		super();
	}
	public BlockIndexStatistic(Map<String, Integer> blocksHistogram, double totalBlocks, double validComparisons,
			double[] comparisonsPerBlock, double[] redundantCPE, double[] comparisonsPerEntity,
			List<AbstractBlock> blocks, EntityIndex entityIndex) {
		this.blocksHistogram = blocksHistogram;
		this.totalBlocks = totalBlocks;
		this.validComparisons = validComparisons;
		this.comparisonsPerBlock = comparisonsPerBlock;
		this.redundantCPE = redundantCPE;
		this.comparisonsPerEntity = comparisonsPerEntity;
		this.blocks = blocks;
		this.entityIndex = entityIndex;
	}

	public BlockIndexStatistic(Map<String, Set<Integer>> invertedIndex, String tableName) {
		this.blocksHistogram = 
				invertedIndex.entrySet().stream()
					.collect(Collectors.toMap(Entry::getKey, e -> Integer.valueOf(e.getValue().size())));
		this.tableName = tableName;
//		this.blocks = BlockIndex.parseIndex(invertedIndex);
	}
	
	public BlockIndexStatistic(Map<String, Set<Integer>> invertedIndex) {
		this.blocksHistogram = 
				invertedIndex.entrySet().stream()
					.collect(Collectors.toMap(Entry::getKey, e -> Integer.valueOf(e.getValue().size())));
//		this.blocks = BlockIndex.parseIndex(invertedIndex);
	}
	
	public void getStatistics() {
        if (entityIndex == null) {
            entityIndex = new EntityIndex(blocks);
        }
        
        validComparisons = 0;
        totalBlocks = blocks.size();
        redundantCPE = new double[entityIndex.getNoOfEntities()];
        comparisonsPerBlock = new double[(int)(totalBlocks + 1)];
        comparisonsPerEntity = new double[entityIndex.getNoOfEntities()];
        for (AbstractBlock block : blocks) {
            comparisonsPerBlock[block.getBlockIndex()] = block.getNoOfComparisons();
            ComparisonIterator iterator = block.getComparisonIterator();
            while (iterator.hasNext()) {
                Comparison comparison = iterator.next();
                int entityId2 = comparison.getEntityId2()+entityIndex.getDatasetLimit();
                
                redundantCPE[comparison.getEntityId1()]++;
                redundantCPE[entityId2]++;
                if (!entityIndex.isRepeated(block.getBlockIndex(), comparison)) {
                    validComparisons++;
                    comparisonsPerEntity[comparison.getEntityId1()]++;
                    comparisonsPerEntity[entityId2]++;
                }
            }
        }
        
    }

	public void storeStatistics() throws IOException {
		File file = new File("./data/tableStats/blockIndex/" + tableName + ".json");
		FileOutputStream fOut = null;

		fOut = new FileOutputStream(file);

	    ObjectMapper mapper = new ObjectMapper();

		JsonGenerator jGenerator = null;
		jGenerator = mapper.getFactory().createGenerator(fOut);
		mapper.writeValue(jGenerator, this);
	}

	public Map<String, Integer> getBlocksHistogram() {
		return blocksHistogram;
	}

	public void setBlocksHistogram(Map<String, Integer> blocksHistogram) {
		this.blocksHistogram = blocksHistogram;
	}

	public double getTotalBlocks() {
		return totalBlocks;
	}

	public void setTotalBlocks(double totalBlocks) {
		this.totalBlocks = totalBlocks;
	}

	public double getValidComparisons() {
		return validComparisons;
	}

	public void setValidComparisons(double validComparisons) {
		this.validComparisons = validComparisons;
	}

	public double[] getComparisonsPerBlock() {
		return comparisonsPerBlock;
	}

	public void setComparisonsPerBlock(double[] comparisonsPerBlock) {
		this.comparisonsPerBlock = comparisonsPerBlock;
	}

	public double[] getRedundantCPE() {
		return redundantCPE;
	}

	public void setRedundantCPE(double[] redundantCPE) {
		this.redundantCPE = redundantCPE;
	}

	public double[] getComparisonsPerEntity() {
		return comparisonsPerEntity;
	}

	public void setComparisonsPerEntity(double[] comparisonsPerEntity) {
		this.comparisonsPerEntity = comparisonsPerEntity;
	}

	public List<AbstractBlock> getBlocks() {
		return blocks;
	}

	public void setBlocks(List<AbstractBlock> blocks) {
		this.blocks = blocks;
	}

	public EntityIndex getEntityIndex() {
		return entityIndex;
	}

	public void setEntityIndex(EntityIndex entityIndex) {
		this.entityIndex = entityIndex;
	}
	
	
	
}
