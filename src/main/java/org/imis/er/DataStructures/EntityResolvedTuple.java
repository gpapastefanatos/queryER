package org.imis.er.DataStructures;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.imis.er.Utilities.EntityGrouping;
import org.imis.er.Utilities.SerializationUtilities;
import org.imis.er.Utilities.UnionFind;

import gnu.trove.map.hash.TIntObjectHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class EntityResolvedTuple<T> extends AbstractEnumerable<T> {

	public HashMap<Integer, Object[]> data;

	public UnionFind uFind;
	public HashMap<Integer, Set<Integer>> revUF;
	public List<T> finalData;
	private int matches;
	private Integer comparisons;
	private double compTime;
	private double revUFCreationTime;
	private Integer keyIndex;
	private Integer noOfAttributes;
	
	public EntityResolvedTuple(HashMap<Integer, Object[]> data, UnionFind uFind, Integer keyIndex, Integer noOfAttributes) {
		super();
		this.data = data;
		this.uFind = uFind;
		this.finalData = new ArrayList<>();
		this.revUF = new HashMap<>();
		this.keyIndex = keyIndex;
		this.noOfAttributes = noOfAttributes;
	}
	
	public EntityResolvedTuple(List<Object[]> finalData, UnionFind uFind, Integer keyIndex, Integer noOfAttributes) {
		super();
		this.finalData = (List<T>) finalData;
		this.revUF = new HashMap<>();
	}
	

	@Override
	public Enumerator<T> enumerator() {
		Enumerator<T> originalEnumerator = Linq4j.enumerator(this.finalData);
		// TODO Auto-generated method stub
		return new Enumerator<T>() {

			@Override
			public T current() {
				return originalEnumerator.current();
			}

			@Override
			public boolean moveNext() {
				while (originalEnumerator.moveNext()) {
					return true;
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

	@SuppressWarnings("unchecked")
	public void sortEntities() {
		// TODO Auto-generated method stub
		this.finalData = (List<T>) EntityGrouping.sortSimilar(this.revUF, this.data);	

	}
	
	@SuppressWarnings("unchecked")
	public void groupEntities() {
		this.finalData = (List<T>) EntityGrouping.groupSimilar(this.revUF, 	this.data, keyIndex, noOfAttributes);	

	}
	
	@SuppressWarnings("unchecked")
	public void getAll() {
		double revUFCreationStartTime = System.currentTimeMillis();

		HashMap<Integer, Object[]> filteredData = new HashMap<>();


		
		for (int child : uFind.getParent().keySet()) {
			int parent = uFind.getParent().get(child);
			this.revUF.computeIfAbsent(parent, x -> new HashSet<>()).add(child);
			this.revUF.computeIfAbsent(child, x -> new HashSet<>()).add(parent);
			// For both of these go to their similarities and recompute them
			for(int simPar : this.revUF.get(parent)) {
				if(simPar != parent)
					this.revUF.computeIfAbsent(simPar, x -> new HashSet<>()).addAll(this.revUF.get(parent));
			}
			for(int simPar : this.revUF.get(child)) {
				if(simPar != child)
					this.revUF.computeIfAbsent(simPar, x -> new HashSet<>()).addAll(this.revUF.get(child));
			}
		}

		
		for (int id : this.revUF.keySet()) {
			Object[] datum = this.data.get(id);
			filteredData.put(id, datum);
			this.finalData.add((T) datum);
		}
		this.data = filteredData;
		double revUFCreationEndTime = System.currentTimeMillis();
		this.setRevUFCreationTime((revUFCreationEndTime - revUFCreationStartTime)/1000);
	}
	

	public int getMatches() {
		return matches;
	}

	public void setMatches(int i) {
		this.matches = i;
	}

	public Integer getComparisons() {
		return comparisons;
	}

	public void setComparisons(Integer comparisons) {
		this.comparisons = comparisons;
	}

	public HashMap<Integer, Object[]> getData() {
		return data;
	}

	public void setData(HashMap<Integer, Object[]> data) {
		this.data = data;
	}

	public HashMap<Integer, Set<Integer>> getRevUF() {
		return revUF;
	}

	public void setRevUF(HashMap<Integer, Set<Integer>> revUF) {
		this.revUF = revUF;
	}

	public List<T> getFinalData() {
		return finalData;
	}

	public void setFinalData(List<T> finalData) {
		this.finalData = finalData;
	}

	public double getCompTime() {
		return compTime;
	}

	public void setCompTime(double compTime) {
		this.compTime = compTime;
	}

	public double getRevUFCreationTime() {
		return revUFCreationTime;
	}

	public void setRevUFCreationTime(double revUFCreationTime) {
		this.revUFCreationTime = revUFCreationTime;
	}
	
	

}
