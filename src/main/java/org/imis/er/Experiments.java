package org.imis.er;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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

import javax.sql.DataSource;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.commons.io.FileUtils;
import org.imis.calcite.adapter.csv.CsvEnumerator;
import org.imis.calcite.adapter.csv.CsvTableStatistic;
import org.imis.calcite.adapter.csv.CsvTranslatableTable;
import org.imis.calcite.util.DeduplicationExecution;
import org.imis.er.BlockIndex.BaseBlockIndex;
import org.imis.er.BlockIndex.BlockIndexStatistic;
import org.imis.er.ConnectionPool.CalciteConnectionPool;
import org.imis.er.DataStructures.UnilateralBlock;
import org.imis.er.Utilities.SerializationUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

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
	private static final String CALCULATE_GROUND_TRUTH = "ground_truth.calculate";
	private static final String DIVIDE_GROUND_TRUTH = "ground_truth.divide";

	
	private static String queryFilePath = "";
	private static Integer groundTruthDivide = 500;
	private static Integer totalRuns = 1;
	private static String schemaName = "";
	private static String calciteConnectionString = "";
	private static Boolean calculateGroundTruth = false;
	private static CalciteConnectionPool calciteConnectionPool = null;
	
	public static void main(String[] args)
			throws  ClassNotFoundException, SQLException, ValidationException, RelConversionException, SqlParseException, IOException
	{
		setProperties();
		// Create output folders
		generateDumpDirectories();
		// Create Connection
		calciteConnectionPool = new CalciteConnectionPool();
		CalciteConnection calciteConnection = null;
		try {
			calciteConnection = (CalciteConnection) calciteConnectionPool.setUp(calciteConnectionString);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
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
        //csvWriter.append("query,runs,time,no_of_blocks,agg_cardinality,CC,total_entities,entities_in_blocks,singleton_entities,average_block,BC,detected_duplicates,PC,PQ\n");
        csvWriter.append("query,runs,time,no_of_blocks,agg_cardinality,CC,entities_in_blocks,detected_duplicates,PC,PQ\n");
    	final Logger DEDUPLICATION_EXEC_LOGGER =  LoggerFactory.getLogger(DeduplicationExecution.class);

        if(DEDUPLICATION_EXEC_LOGGER.isDebugEnabled()) 
			DEDUPLICATION_EXEC_LOGGER.debug("table_name,query_entities,block_join_time,blocking_time,query_blocks,max_query_block_size,avg_query_block_size,total_query_comps,block_entities,"
					+ "purge_blocks,purge_time,max_purge_block_size,avg_purge_block_size,total_purge_comps,purge_entities,filter_blocks,filter_time,max_filter_block_size,avg_filter_block_size,"
					+ "total_filter_comps,filter_entities,ep_time,ep_comps,ep_entities,matches_found,executed_comparisons,table_scan_time,jaro_time,comparison_time,rev_uf_creation_time,total_entities,total_dedup_time\n");
		for(String query : queries) {
			double totalRunTime = 0.0;
            ResultSet queryResults = null;
			for(int i = 0; i < totalRuns; i++) {
				Double runTime = 0.0;
				double queryStartTime = System.currentTimeMillis();
				queryResults = runQuery(calciteConnection, query);
				//printQueryContents(queryResults);
				exportQueryContent(queryResults, "./data/queryResults.csv");
				double queryEndTime = System.currentTimeMillis();
				runTime = (queryEndTime - queryStartTime)/1000;
				totalRunTime += runTime;
			}
			csvWriter.append("\"" + query + "\"" + "," + totalRuns + "," + totalRunTime/totalRuns + ",");
			System.out.println("Finished query: " + index + " runs: " + totalRuns + " time: " + totalRunTime/totalRuns);
			// Get the ground truth for this query
			if(calculateGroundTruth)
				calculateGroundTruth(calciteConnection, query, schemaName, csvWriter);
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
		if(!query.contains("DEDUP")) return;
		final String tableName;
		if(query.indexOf("WHERE") != -1) {
			tableName = query.substring(query.indexOf(schemaName) + schemaName.length() + 1  , query.indexOf("WHERE")).trim();;
		}
		else {
			tableName = query.substring(query.indexOf(schemaName) + schemaName.length() + 1, query.length()).trim();;
		}
		String name = query.replace("'", "").replace("*","ALL").replace(">", "BIGGER").replace("<", "LESS");
	
		// Construct ground truth query
		Set<IdDuplicates> groundDups = new HashSet<IdDuplicates>();
		File blocksDir = new File("./data/groundTruth/" + name);
		if(blocksDir.exists()) {
			groundDups = (Set<IdDuplicates>) SerializationUtilities.loadSerializedObject("./data/groundTruth/" + name);
		}
		else {
			System.out.println("Calculating ground truth..");

			CalciteConnection qCalciteConnection = null;
			try {
				qCalciteConnection = (CalciteConnection) calciteConnectionPool.setUp(calciteConnectionString);
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			Set<Integer> qIds = (Set<Integer>) SerializationUtilities.loadSerializedObject("./data/qIds");
			List<Set<Integer>> inIdsSets = new ArrayList<>();
			Set<Integer> currSet = null;
			for (Integer value : qIds) {
			    if (currSet == null || currSet.size() == groundTruthDivide)
			    	inIdsSets.add(currSet = new HashSet<>());
			    currSet.add(value);
			}
			List<String> inIds = new ArrayList<>();
			inIdsSets.forEach(inIdSet -> {
				String inId = "(";
				for(Integer qId : inIdSet) {
					inId += qId + ",";
				}
				inId = inId.substring(0, inId.length() - 1) + ")";
				inIds.add(inId);
			});
			System.out.println("Will execute " + inIds.size() + " queries");

			for(String inIdd : inIds) {
				String groundTruthQuery = "SELECT id_d, id_s FROM ground_truth.ground_truth_" + tableName +
						" WHERE id_s IN " + inIdd + " OR id_d IN " + inIdd ;
				ResultSet gtQueryResults = runQuery(calciteConnection, groundTruthQuery);
				while (gtQueryResults.next()) {
					Integer id_d = Integer.parseInt(gtQueryResults.getString("id_d"));
					Integer id_s = Integer.parseInt(gtQueryResults.getString("id_s"));
					IdDuplicates idd = new IdDuplicates(id_d, id_s);
					groundDups.add(idd);
				}		
			}
			SerializationUtilities.storeSerializedObject(groundDups, "./data/groundTruth/" + name);
		}
		

		final AbstractDuplicatePropagation duplicatePropagation = new UnilateralDuplicatePropagation(groundDups);
		System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());

		duplicatePropagation.resetDuplicates();
		List<AbstractBlock> blocks = (List<AbstractBlock>) SerializationUtilities.loadSerializedObject("./data/blocks/" + tableName);
		//remove file now
        FileUtils.forceDelete(new File("./data/blocks/" + tableName)); //delete directory
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
            calciteConnectionString = properties.getProperty(CALCITE_CONNECTION);
            calculateGroundTruth = Boolean.parseBoolean(properties.getProperty(CALCULATE_GROUND_TRUTH));
            groundTruthDivide = Integer.parseInt(properties.getProperty(DIVIDE_GROUND_TRUTH));
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
	private static void generateDumpDirectories() throws IOException {
		File logsDir = new File("./data/logs");
		File blockIndexDir = new File("./data/blockIndex");
		File groundTruthDir = new File("./data/groundTruth");
		File tableStatsDir = new File("./data/tableStats/tableStats");
		File blockIndexStats = new File("./data/tableStats/blockIndexStats");
		if(!logsDir.exists()) {
            FileUtils.forceMkdir(logsDir); //create directory
		}
		if(!blockIndexDir.exists()) {
            FileUtils.forceMkdir(blockIndexDir); //create directory
		}
		if(!groundTruthDir.exists()) {
            FileUtils.forceMkdir(groundTruthDir); //create directory
		}
		if(!tableStatsDir.exists()) {
            FileUtils.forceMkdir(tableStatsDir); //create directory
		}
		if(!blockIndexStats.exists()) {
            FileUtils.forceMkdir(blockIndexStats); //create directory
		}
	}
	
	
}
