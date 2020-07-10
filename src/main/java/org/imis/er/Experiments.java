package org.imis.er;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.commons.io.FileUtils;
import org.imis.calcite.adapter.csv.CsvEnumerator;
import org.imis.calcite.adapter.csv.CsvTranslatableTable;
import org.imis.er.BlockIndex.BaseBlockIndex;
import org.imis.er.DataStructures.UnilateralBlock;
import org.imis.er.Utilities.SerializationUtilities;

import org.imis.er.Utilities.BlockStatistics;

import org.imis.er.EfficiencyLayer.ComparisonRefinement.AbstractDuplicatePropagation;
import org.imis.er.EfficiencyLayer.ComparisonRefinement.UnilateralDuplicatePropagation;
import org.imis.er.DataStructures.AbstractBlock;
import org.imis.er.DataStructures.IdDuplicates;
import au.com.bytecode.opencsv.CSVWriter;


public class Experiments {

	/**
	 * @param args
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws ValidationException
	 * @throws RelConversionException
	 * @throws SqlParseException
	 * Main function of the program. 
	 * @throws IOException 
	 */
	private static String pathToPropertiesFile = "config.properties";
	private static Properties properties;

	private static final String QUERY_FILE_PROPERTY = "query.filepath";
	private static final String QUERY_TOTAL_RUNS = "query.runs";
	private static final String SCHEMA_NAME = "schema.name";
	private static final String CALCITE_CONNECTION = "calcite.connection";
	
	private static String queryFilePath = "";
	private static Integer totalRuns = 1;
	private static String schemaName = "";
	private static String calciteConnection = "";

	public static void main(String[] args)
			throws  ClassNotFoundException, SQLException, ValidationException, RelConversionException, SqlParseException, IOException
	{
		setProperties();
		// Create Connection
		Class.forName("org.apache.calcite.jdbc.Driver");
		Properties info = new Properties();
		info.setProperty("lex", "JAVA");
		Connection connection =
				DriverManager.getConnection(calciteConnection, info);
		CalciteConnection calciteConnection =
				connection.unwrap(CalciteConnection.class);

		// Create and add schema
		SchemaPlus rootSchema = calciteConnection.getRootSchema();

		// Create big Block Indexes for each table
		for(String tableName : rootSchema.getSubSchema(schemaName).getTableNames()){
			CsvTranslatableTable table = (CsvTranslatableTable) rootSchema.getSubSchema(schemaName).getTable(tableName);
			System.out.println(tableName + ": " + table.getRowType(new JavaTypeFactoryImpl()));
			List<RelDataTypeField> fields = (table.getRowType(new JavaTypeFactoryImpl()).getFieldList());
			List<String> fieldNames = new ArrayList<String>();
			List<String> fieldTypes = new ArrayList<String>();
			// Instantiate keyFieldName here
			for(RelDataTypeField field : fields) {
				fieldNames.add(field.getName());
				fieldTypes.add(field.getType().toString());

			}
			// Set key field for each table
			String[] keys = {"rec_id", "rec_id", "rec_id"};
			for(String key : keys) {
				if(fieldNames.contains(key)) {
					table.setKey(fieldNames.indexOf(key));
					break;
				}
				else {
					System.out.println("Column name does not exist!");
				}
			}
			// Create Block index and store into data folder (only if not already created)
			if(!new File("./data/blockIndex/" + tableName + "InvertedIndex").exists()) {
				System.out.println("Creating Block Index..");
				AtomicBoolean ab = new AtomicBoolean();
				ab.set(false);
				@SuppressWarnings({ "unchecked", "rawtypes" })
				CsvEnumerator<Object[]> enumerator = new CsvEnumerator(table.getSource(), ab,
						table.getFieldTypes());
				BaseBlockIndex blockIndex = new BaseBlockIndex();
				blockIndex.createBlockIndex(enumerator, table.getKey());
				blockIndex.buildQueryBlocks();
				blockIndex.storeBlockIndex("./data/blockIndex/", tableName );
				// Print block Index Statistics
			}
			else {
				System.out.println("Block Index already created!");
			}
		}
		// Enter a query or read query from file
		List<String> queries = new ArrayList<>();
		
		if(queryFilePath == null) {
			String query = readQuery();
			queries.add(query);
		}
		else {
			readQueries(queries, queryFilePath);
		}
		runQueries(calciteConnection, queries, totalRuns, schemaName);
		
	}

	private static void readQueries(List<String> queries, String queryFilePath) {
		// read query line by line
	 	BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(queryFilePath));
			String line = reader.readLine();
			
			while (line != null) {
				if(!line.contains("--") && !line.isEmpty()) {
					queries.add(line);
					// read next line
				}
				line = reader.readLine();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	private static String readQuery() throws IOException {
		Scanner  reader = new Scanner(new BufferedInputStream(System.in));;
		System.out.println("Enter a query: ");
		String query = "";
		String line;
		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		while ((line = stdin.readLine()) != null && line.length()!= 0) {
			query += line;
		}
		query = query.replaceAll("[\r\n]+", " ");
		reader.close();

		return query;
	}


	private static void runQueries(CalciteConnection calciteConnection, List<String> queries, 
			Integer totalRuns, String schemaName) throws IOException, SQLException {
		int index = 1;

		File resultDir = new File("./data/queryResults");
		if(resultDir.exists()) {
            FileUtils.cleanDirectory(resultDir); //clean out directory (this is optional -- but good know)
            FileUtils.forceDelete(resultDir); //delete directory
            FileUtils.forceMkdir(resultDir); //create directory
		}
        else FileUtils.forceMkdir(resultDir); //create directory
		File blocksDir = new File("./data/blocks");
		if(blocksDir.exists()) {
            FileUtils.cleanDirectory(blocksDir); //clean out directory (this is optional -- but good know)
            FileUtils.forceDelete(blocksDir); //delete directory
            FileUtils.forceMkdir(blocksDir); //create directory
		}
        else FileUtils.forceMkdir(blocksDir); //create directory
        File queryFile = new File("./data/queryResults/queryResults.csv");
        FileWriter csvWriter = new FileWriter(queryFile);
        csvWriter.append("query,runs,time,no_of_blocks,agg_cardinality,CC,total_entities,entities_in_blocks,singleton_entities,average_block,BC,detected_duplicates,PC,PQ\n");
  
		for(String query : queries) {
			// Create results directory

            //File queryFile = new File("./data/queryResults/query" + index + ".csv");
            //FileWriter csvWriter = new FileWriter(queryFile);
            //csvWriter.append("query,runs,time,no_of_blocks,agg_cardinality,CC,total_entities,entities_in_blocks,singleton_entities,average_block,BC,detected_duplicates,PC,PQ\n");
            double totalRunTime = 0.0;
			for(int i = 0; i < totalRuns; i++) {
				Double runTime = 0.0;
				double queryStartTime = System.currentTimeMillis();
				ResultSet queryResults = runQuery(calciteConnection, query);
				//printQueryContents(queryResults);
				//exportQueryContent(queryResults, "./data/universities.csv");
				double queryEndTime = System.currentTimeMillis();
				runTime = (queryEndTime - queryStartTime)/1000;
				Integer run = i + 1;
				totalRunTime += runTime;
			}
			csvWriter.append("\"" + query + "\"" + "," + totalRuns + "," + totalRunTime/totalRuns + ",");
			System.out.println("Finished query: " + index + " runs: " + totalRuns + " time: " + totalRunTime/totalRuns);

			// Get the ground truth for this query
			//calculateGroundTruth(calciteConnection, query, schemaName, csvWriter);
			csvWriter.append("\n");
			csvWriter.flush();
			index ++;
		}
		csvWriter.close();
	}

	private static ResultSet runQuery(CalciteConnection calciteConnection, String query) throws SQLException {
		return calciteConnection.createStatement().executeQuery(query);
	}
	
	@SuppressWarnings("unchecked")
	private static void calculateGroundTruth(CalciteConnection calciteConnection, String query, String schemaName, FileWriter csvWriter) throws SQLException, IOException {
		// Trick to get table name from a single sp query
		String tableName = query.substring(query.indexOf(schemaName) + schemaName.length() + 1  , query.indexOf("WHERE")); 
		tableName = tableName.trim();
		// Construct ground truth query
		String groundTruthQuery = "SELECT id_d, id_s FROM ground_truth.ground_truth_" + tableName;
		ResultSet queryResults = runQuery(calciteConnection, groundTruthQuery);
		List<AbstractBlock> blocks = (List<AbstractBlock>) SerializationUtilities.loadSerializedObject("./data/blocks/" + tableName);
		//remove file now
        FileUtils.forceDelete(new File("./data/blocks/" + tableName)); //delete directory

		Set<IdDuplicates> groundDups = new HashSet<IdDuplicates>();
		while (queryResults.next()) {
			Integer id_d = Integer.parseInt(queryResults.getString("id_d"));
			Integer id_s = Integer.parseInt(queryResults.getString("id_s"));
			IdDuplicates idd = new IdDuplicates(id_d, id_s);
			groundDups.add(idd);
		}
		final AbstractDuplicatePropagation duplicatePropagation = new UnilateralDuplicatePropagation(groundDups);
		duplicatePropagation.resetDuplicates();
		BlockStatistics bStats = new BlockStatistics(blocks, duplicatePropagation, csvWriter);
		bStats.applyProcessing();		
	}

	private static void exportQueryContent(ResultSet queryResults, String path) throws SQLException, IOException {
		 CSVWriter writer = new CSVWriter(new FileWriter(path),',');

	     writer.writeAll(queryResults, true);
		 
	}


	private static void printQueryContents(ResultSet resultSet) throws SQLException {
		ResultSetMetaData rsmd = resultSet.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		while (resultSet.next()) {
			//Print one row
			for(int i = 1 ; i <= columnsNumber; i++){
				System.out.print(resultSet.getString(i) + " || "); //Print one element of a row
			}
			System.out.println();//Move to the next line to print the next row.
		}
	}
	

	private static void setProperties() {
		properties = loadProperties();
		if(!properties.isEmpty()) {
			queryFilePath = properties.getProperty(QUERY_FILE_PROPERTY);
            totalRuns = Integer.parseInt(properties.getProperty(QUERY_TOTAL_RUNS));
            schemaName = properties.getProperty(SCHEMA_NAME);
            calciteConnection = properties.getProperty(CALCITE_CONNECTION);
		}
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
	
	
}
