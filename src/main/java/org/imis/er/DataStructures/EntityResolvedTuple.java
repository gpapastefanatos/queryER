package org.imis.er.DataStructures;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.imis.er.Utilities.EntityGrouping;
import org.imis.er.Utilities.UnionFind;

import gnu.trove.map.hash.TIntObjectHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class EntityResolvedTuple<T> extends AbstractEnumerable<T> {

	public HashMap<Integer, Object[]> data;
	public TIntObjectHashMap<Object[]> data2;
	public Int2ObjectOpenHashMap<Object[]> data3;

	public UnionFind uFind;
	public HashMap<Integer, Set<Integer>> revUF;
	public List<T> finalData;
	
	public EntityResolvedTuple(HashMap<Integer, Object[]> data, UnionFind uFind) {
		super();
		this.data = data;
		this.uFind = uFind;
		this.finalData = new ArrayList<>();
		this.revUF = new HashMap<>();
	}
	
	public EntityResolvedTuple(TIntObjectHashMap<Object[]> data2, UnionFind uFind) {
		super();
		this.data2 = data2;
		this.uFind = uFind;
		this.finalData = new ArrayList<>();
		this.revUF = new HashMap<>();
	}
	
	public EntityResolvedTuple(Int2ObjectOpenHashMap<Object[]> data3, UnionFind uFind) {
		super();
		this.data3 = data3;
		this.uFind = uFind;
		this.finalData = new ArrayList<>();
		this.revUF = new HashMap<>();
	}
	
	public EntityResolvedTuple(List<Object[]> finalData, UnionFind uFind) {
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
		this.finalData = (List<T>) EntityGrouping.sortSimilar(this.uFind, this.data);	

	}
	
	@SuppressWarnings("unchecked")
	public void groupEntities() {
		// TODO Auto-generated method stub
		this.finalData = (List<T>) EntityGrouping.sortSimilar(this.uFind, this.data);	

	}
	
	@SuppressWarnings("unchecked")
	public void getAll() {
		// TODO Auto-generated method stub
		HashMap<Integer, Object[]> data = new HashMap<>();
		for (int child : uFind.getParent().keySet()) {
			revUF.computeIfAbsent(uFind.getParent().get(child), x -> new HashSet<>()).add(child);
		}
		for (int id : revUF.keySet()) {
			for (int idInner : revUF.get(id)) {
				if(this.data.get(idInner) != null) {
					data.put(idInner, this.data.get(idInner));
					this.finalData.add((T) this.data.get(idInner));
				}
			}
		}
		this.data = data;
	}

	public void getAll2() {
		// TODO Auto-generated method stub
		TIntObjectHashMap<Object[]> data2 = new TIntObjectHashMap<Object[]>(revUF.size());
		for (int child : uFind.getParent().keySet()) {
			revUF.computeIfAbsent(uFind.getParent().get(child), x -> new HashSet<>()).add(child);
		}
		for (int id : revUF.keySet()) {
			for (int idInner : revUF.get(id)) {
				if(this.data2.get(idInner) != null) {
					data2.put(idInner, this.data2.get(idInner));
					this.finalData.add((T) this.data2.get(idInner));
				}
			}
		}
		this.data2 = data2;
	}
	
	public void getAll3() {
		// TODO Auto-generated method stub
		Int2ObjectOpenHashMap<Object[]> data3 = new Int2ObjectOpenHashMap<Object[]>(revUF.size());
		for (int child : uFind.getParent().keySet()) {
			revUF.computeIfAbsent(uFind.getParent().get(child), x -> new HashSet<>()).add(child);
		}
		for (int id : revUF.keySet()) {
			for (int idInner : revUF.get(id)) {
				if(this.data3.get(idInner) != null) {
					data3.put(idInner, this.data3.get(idInner));
					this.finalData.add((T) this.data3.get(idInner));
				}
			}
		}
		this.data3 = data3;
	}

}
