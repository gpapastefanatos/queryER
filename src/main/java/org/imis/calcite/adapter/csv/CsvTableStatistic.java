package org.imis.calcite.adapter.csv;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import net.agkn.hll.HLL;

public class CsvTableStatistic {

	protected CsvEnumerator<Object[]> csvEnumerator;
	protected Map<Integer, Long> columnCardinalities;
	protected int columnCount;
	protected int tableKey;
	
	public CsvTableStatistic(CsvEnumerator<Object[]> csvEnumerator, int columnCount, int tableKey) {
		this.csvEnumerator = csvEnumerator;
		this.columnCount = columnCount;
		this.tableKey = tableKey;
		this.columnCardinalities = new HashMap<Integer, Long>(columnCount);
	}
	
	public void getStatistics() {
		getCardinalities();
	}
	
	public void getCardinalities() {
		HashFunction hashFunction = Hashing.murmur3_128();
		List<HLL> cardinalityList = new ArrayList<>();
		int col = 0;
		while(col < columnCount) {
			cardinalityList.add(new HLL(14, 5));
			col += 1;
		}
		while(csvEnumerator.moveNext()) {
			Object[] curr = csvEnumerator.current();
			for(int i = 0 ; i < columnCount; i++) {
				String attribute = curr[i].toString();
				long hashedValue = hashFunction.newHasher().putString(attribute, Charset.forName("UTF-8")).hash().asLong();
				cardinalityList.get(i).addRaw(hashedValue);	
			}
		}
		col = 0;
		for(HLL columnCardinality : cardinalityList) {
			long cardinality = columnCardinality.cardinality();
			columnCardinalities.put(col, cardinality);
		}
		
	}

	public void storeStatistics() {
		// TODO Auto-generated method stub
		
	}
}
