package org.imsi.queryEREngine.imsi.er;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;


import org.imsi.queryEREngine.apache.calcite.jdbc.CalciteConnection;
import org.imsi.queryEREngine.imsi.er.ConnectionPool.CalciteConnectionPool;
import org.imsi.queryEREngine.imsi.er.Utilities.DumpDirectories;

import au.com.bytecode.opencsv.CSVWriter;

public class QueryEngine {

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

	private static final String SCHEMA_NAME = "schema.name";
	private static final String CALCITE_CONNECTION = "calcite.connection";

	
	private static String schemaName = "";
	private static String calciteConnectionString = "";
	private static CalciteConnectionPool calciteConnectionPool = null;
	private CalciteConnection calciteConnection = null;

	public void initialize() throws IOException, SQLException {
		setProperties();
		// Create output folders
		DumpDirectories dumpDirectories = new DumpDirectories("");
		dumpDirectories.generateDumpDirectories();
		// Create Connection
		calciteConnectionPool = new CalciteConnectionPool();
		CalciteConnection calciteConnection = null;
		try {
			calciteConnection = (CalciteConnection) calciteConnectionPool.setUp(calciteConnectionString);
			this.calciteConnection = calciteConnection;
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		// Enter a query or read query from file
		List<String> queries = new ArrayList<>();
		initializeDB(calciteConnection, schemaName);
	}

	private void initializeDB(CalciteConnection calciteConnection, String schemaName2) throws SQLException {
		System.out.println("Initializing Database...");
		Set<String> schemas = calciteConnection.getRootSchema().getSubSchemaNames();
		HashMap<String, Set<String>> tables = new HashMap<>();
		for(String schemaName : schemas) {
			if(schemaName.contentEquals("metadata") || schemaName.contentEquals("test")) continue;
			Set<String> tablesInSchema = calciteConnection.getRootSchema().getSubSchema(schemaName).getTableNames();
			tables.put(schemaName, tablesInSchema);
		}
		String fSchema = "";
		String fTable = "";
		for(String schema : tables.keySet()) {
			Set<String> tablesInSchema = tables.get(schema);
			boolean flag = false;
			for(String table: tablesInSchema)
				if(!table.contains("/")) {
					fSchema = schema;
					fTable = table;
					flag = true;
					break;
				}
			if(flag) break;
		}
		String query = "SELECT 1 FROM " + fSchema + "." + fTable;
		runQuery(query);	
		System.out.println("Initializing Finished!");

	}


	public ResultSet runQuery(String query) throws SQLException {
		System.out.println("Running query...");
		ResultSet resultSet;
		Connection connection = calciteConnectionPool.getConnection();
		
		double queryStartTime = System.currentTimeMillis();
		resultSet = connection.createStatement().executeQuery(query);
		double queryEndTime = System.currentTimeMillis();
		double runTime = (queryEndTime - queryStartTime)/1000;
		System.out.println("Finished query, time: " + runTime);	
		return resultSet;
		
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
		resultSet.close();
	}
	

	private void setProperties() {
		properties = loadProperties();
		if(!properties.isEmpty()) {
            schemaName = properties.getProperty(SCHEMA_NAME);
            calciteConnectionString = properties.getProperty(CALCITE_CONNECTION);
		}
	}
	
	private Properties loadProperties() {
		
        Properties prop = new Properties();
       
		try (InputStream input = this.getClass().getClassLoader().getResourceAsStream(pathToPropertiesFile)) {
            // load a properties file
            prop.load(input);
                       
        } catch (IOException ex) {
            ex.printStackTrace();
        }
		return prop;
	}
	
}
