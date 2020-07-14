package org.imis.er.BlockIndex;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.imis.calcite.util.DeduplicationExecution;
import org.imis.er.DataStructures.AbstractBlock;
import org.imis.er.DataStructures.Attribute;
import org.imis.er.DataStructures.EntityProfile;
import org.imis.er.DataStructures.UnilateralBlock;
import org.imis.er.Utilities.Converter;
import org.imis.er.Utilities.SerializationUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockIndex {
	protected static final Logger DEDUPLICATION_EXEC_LOGGER =  LoggerFactory.getLogger(DeduplicationExecution.class);

	public List<EntityProfile> entityProfiles;
	protected Map<String, Set<Integer>> invertedIndex;
	protected Set<Integer> joinedIds;

	public BlockIndex() {
		this.entityProfiles = new ArrayList<EntityProfile>();
		this.invertedIndex = new HashMap<String, Set<Integer>>();
	}

	public void buildQueryBlocks() {
		this.invertedIndex = indexEntities(0, this.entityProfiles);
	}


	@SuppressWarnings("unchecked")
	protected Map<String, Set<Integer>> indexEntities(int sourceId, List<EntityProfile> profiles) {
		invertedIndex = new HashMap<String, Set<Integer>>();
		HashSet<String> stopwords = (HashSet<String>) SerializationUtilities
				.loadSerializedObject(getClass().getClassLoader().getResource("stopwords/stopwords_SER").getPath());
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

					}
				}
			}

		}
		//System.out.println("Query Block Index size: " + invertedIndex.size());
		return invertedIndex;
	}

	public void storeBlockIndex(String path, String tableName) {
		SerializationUtilities.storeSerializedObject(this.invertedIndex, path + tableName + "InvertedIndex" );
	}

	protected List<AbstractBlock> parseIndex(Map<String, Set<Integer>> invertedIndex) {
		final List<AbstractBlock> blocks = new ArrayList<AbstractBlock>();
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


}
