package org.imis.er.BlockIndex;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.imis.er.DataStructures.AbstractBlock;
import org.imis.er.DataStructures.DecomposedBlock;
import org.imis.er.DataStructures.EntityProfile;
import org.imis.er.DataStructures.UnilateralBlock;
import org.imis.er.Utilities.SerializationUtilities;

public class QueryBlockIndex extends BlockIndex {

	protected Set<Integer> qIds;

	public QueryBlockIndex() {
		this.qIds = new HashSet<>();
	}

	public <T> void createBlockIndex(List<T> dataset, Integer key) {
		// Get project results from previous enumerator
		for(T row : dataset) {
			Object[] currentLine = (Object[]) row;
			Integer fields = currentLine.length;
			String entityKey = currentLine[key].toString();
			if(entityKey.contentEquals("")) continue;
			EntityProfile eP = new EntityProfile(currentLine[key].toString()); // 0 is id, must put this in schema catalog
			qIds.add(Integer.valueOf(entityKey));
			int index = 0;
			while(index < fields) {
				if(index != key) {
					eP.addAttribute(index, currentLine[index].toString());;
				}
				index ++;
			}
			this.entityProfiles.add(eP);
			
		}
	}
	
	@SuppressWarnings("unchecked")
	public List<AbstractBlock> joinBlockIndices(String name) {
		final Map<String, Set<Integer>> bBlocks = (Map<String, Set<Integer>>) SerializationUtilities
				.loadSerializedObject("./data/blockIndex/" + name + "InvertedIndex");
//		if(DEDUPLICATION_EXEC_LOGGER.isDebugEnabled()) 
//			DEDUPLICATION_EXEC_LOGGER.debug("BlockIndex size: " + bBlocks.size());
		bBlocks.keySet().retainAll(this.invertedIndex.keySet());
//		if(DEDUPLICATION_EXEC_LOGGER.isDebugEnabled()) 
//			DEDUPLICATION_EXEC_LOGGER.debug("JoinedBlockIndex size " + bBlocks.size());
		return parseIndex(bBlocks);

	}


	
	public Set<Integer> blocksToEntitiesU(List<UnilateralBlock> blocks){
		Set<Integer> joinedEntityIds = new HashSet<>();
		for(UnilateralBlock block : blocks) {
			int[] entities = block.getEntities();
			joinedEntityIds.addAll(Arrays.stream(entities).boxed().collect(Collectors.toSet()));
		}
		return joinedEntityIds;
	}
	
	public Set<Integer> blocksToEntitiesD(List<DecomposedBlock> blocks){
		Set<Integer> joinedEntityIds = new HashSet<>();
		for(DecomposedBlock block : blocks) {
			int[] entities1 = block.getEntities1();
			int[] entities2 = block.getEntities2();
			joinedEntityIds.addAll(Arrays.stream(entities1).boxed().collect(Collectors.toSet()));
			joinedEntityIds.addAll(Arrays.stream(entities2).boxed().collect(Collectors.toSet()));

		}
		return joinedEntityIds;
	}
	

	public Set<Integer> getIds() {
		// TODO Auto-generated method stub
		return qIds;
	}


}
