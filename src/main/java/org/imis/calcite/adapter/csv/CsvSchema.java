/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.imis.calcite.adapter.csv;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;
import org.imis.er.BlockIndex.BaseBlockIndex;
import org.imis.er.BlockIndex.BlockIndexStatistic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema
 * is a CSV file in that directory.
 */
public class CsvSchema extends AbstractSchema {
	private final File directoryFile;
	private final CsvTable.Flavor flavor;
	private Map<String, Table> tableMap;

	/**
	 * Creates a CSV schema.
	 *
	 * @param directoryFile Directory that holds {@code .csv} files
	 * @param flavor		 Whether to instantiate flavor tables that undergo
	 *									 query optimization
	 */
	public CsvSchema(File directoryFile, CsvTable.Flavor flavor) {
		super();
		this.directoryFile = directoryFile;
		this.flavor = flavor;
	}

	/** Looks for a suffix on a string and returns
	 * either the string with the suffix removed
	 * or the original string. */
	private static String trim(String s, String suffix) {
		String trimmed = trimOrNull(s, suffix);
		return trimmed != null ? trimmed : s;
	}

	/** Looks for a suffix on a string and returns
	 * either the string with the suffix removed
	 * or null. */
	private static String trimOrNull(String s, String suffix) {
		return s.endsWith(suffix)
				? s.substring(0, s.length() - suffix.length())
						: null;
	}

	@Override protected Map<String, Table> getTableMap() {
		if (tableMap == null) {
			tableMap = createTableMap();
		}
		return tableMap;
	}

	private Map<String, Table> createTableMap() {
		// Look for files in the directory ending in ".csv", ".csv.gz", ".json",
		// ".json.gz".
		final Source baseSource = Sources.of(directoryFile);
		File[] files = directoryFile.listFiles((dir, name) -> {
			final String nameSansGz = trim(name, ".gz");
			return nameSansGz.endsWith(".csv")
					|| nameSansGz.endsWith(".json");
		});
		if (files == null) {
			System.out.println("directory " + directoryFile + " not found");
			files = new File[0];
		}
		// Build a map from table name to table; each file becomes a table.
		final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
		for (File file : files) {
			Source source = Sources.of(file);
			Source sourceSansGz = source.trim(".gz");
			final Source sourceSansJson = sourceSansGz.trimOrNull(".json");

			final Source sourceSansCsv = sourceSansGz.trimOrNull(".csv");
			if (sourceSansCsv != null) {
				final CsvTranslatableTable table = createTable(source, sourceSansCsv.relative(baseSource).path());
				
				String tableName = table.getName();
				System.out.println(tableName + ": " + table.getRowType(new JavaTypeFactoryImpl()));
				List<RelDataTypeField> fields = (table.getRowType(new JavaTypeFactoryImpl()).getFieldList());
				List<String> fieldNames = new ArrayList<String>();
				List<String> fieldTypes = new ArrayList<String>();
				// Instantiate keyFieldName here
				for(RelDataTypeField field : fields) {
					fieldNames.add(field.getName());
					fieldTypes.add(field.getType().toString());

				}
				//String[] keys = {"rec_id", "rec_id", "rec_id", "rec_id"};
				String[] keys = {"id"};
				for(String key : keys) {
					if(fieldNames.contains(key)) {
						table.setKey(fieldNames.indexOf(key));
						break;
					}
					else {
						System.out.println("Column name does not exist!");
					}
				}
				// Compute CsvTableStatistic
				if(!new File("./data/tableStats/table" + tableName + ".json").exists()) {
					AtomicBoolean ab = new AtomicBoolean();
					ab.set(false);
					CsvEnumerator csvEnumerator = new CsvEnumerator(table.getSource(), ab,
							table.getFieldTypes());
					CsvTableStatistic csvTableStatistic = new CsvTableStatistic(csvEnumerator,
							table.getFieldTypes().size(), table.getKey());
					csvTableStatistic.getStatistics();
					csvTableStatistic.storeStatistics();
					table.setCsvTableStatistic(csvTableStatistic);
				}
				builder.put(sourceSansCsv.relative(baseSource).path(), table);

				// Create Block index and store into data folder (only if not already created)
				BaseBlockIndex blockIndex = new BaseBlockIndex();
				if(!new File("./data/blockIndex/" + tableName + "InvertedIndex").exists() ||
				   !new File("./data/tableStats/blockIndex/" + tableName + ".json").exists() ) {
					System.out.println("Creating Block Index..");
					AtomicBoolean ab = new AtomicBoolean();
					ab.set(false);
					@SuppressWarnings({ "unchecked", "rawtypes" })
					CsvEnumerator<Object[]> enumerator = new CsvEnumerator(table.getSource(), ab,
							table.getFieldTypes());
					
					blockIndex.createBlockIndex(enumerator, table.getKey());
					blockIndex.buildQueryBlocks();
					blockIndex.storeBlockIndex("./data/blockIndex/", tableName );
					BlockIndexStatistic blockIndexStatistic = new BlockIndexStatistic(blockIndex.getInvertedIndex(), tableName);
					blockIndex.setBlockIndexStatistic(blockIndexStatistic);
					//blockIndexStatistic.getStatistics();
					try {
						blockIndexStatistic.storeStatistics();
					} catch (IOException e) {
						e.printStackTrace();
					}
					// Print block Index Statistics
				}
				else {
					System.out.println("Block Index already created!");
					//blockIndex.loadBlockIndex("./data/blockIndex/", tableName);
					ObjectMapper objectMapper = new ObjectMapper();
					try {
						blockIndex.setBlockIndexStatistic(objectMapper.readValue(new File("./data/tableStats/blockIndex/" + tableName + ".json"),
								BlockIndexStatistic.class));
					} catch (IOException e) {
						e.printStackTrace();
					}

				}
				builder.put("./data/blockIndex/" + tableName + "InvertedIndex", blockIndex);
			}
			
		}
		return builder.build();
	}

	/** Creates table */
	private CsvTranslatableTable createTable(Source source, String name) {
		return new CsvTranslatableTable(source, name, null);

	}
}

// End CsvSchema.java
