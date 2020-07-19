package org.imis.er.ConnectionPool;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.google.common.util.concurrent.MoreExecutors;

public class CalciteConnectionPool {
	  private final ExecutorService executor = Executors.newFixedThreadPool(10);

	  /**
	   * Connection pool to underlying database (hsqldb)
	   */
	  private DataSource dataSource;

	  /**
	   * Calcite connection ("singleton")
	   */
	  private Connection connection;
	  Properties info = new Properties();
	  
	  public Connection setUp(String calciteConnectionString) throws Exception {
	    this.dataSource = createDataSource(calciteConnectionString);
		this.info.setProperty("lex", "JAVA");

	    try (Connection connection = dataSource.getConnection()) {
	      connection.createStatement().execute("DROP TABLE IF EXISTS dummy;");
	      try (Statement stm = connection.createStatement()) {
	        stm.execute("create table dummy (id INTEGER IDENTITY PRIMARY KEY);");
	        for (int i = 0; i < 10; i++) {
	          stm.execute(String.format("INSERT INTO dummy (id) VALUES (%d);", i));
	        }
	      }
	    }

	    Class.forName("org.apache.calcite.jdbc.Driver");
	    final Connection connection = DriverManager.getConnection(calciteConnectionString, info);
	    final SchemaPlus rootSchema = connection.unwrap(CalciteConnection.class).getRootSchema();
	    final JdbcSchema schema = JdbcSchema.create(rootSchema, "test", dataSource, null, null);
	    rootSchema.add("test", schema);

	    this.connection = connection; // calcite connection
	    return connection;
	  }


	  /**
	   * Close all connections
	   */
	  public void tearDown() throws Exception {
	    // shutdown executor
	    MoreExecutors.shutdownAndAwaitTermination(executor, 1, TimeUnit.SECONDS);

	    if (connection != null) {
	      connection.close();
	    }

	    if (dataSource instanceof AutoCloseable) {
	      ((AutoCloseable) dataSource).close();
	    }
	  }

	  private static DataSource createDataSource(String calciteConnectionString) throws SQLException {
	    final BasicDataSource ds = new BasicDataSource();
	    ds.setDriverClassName("org.hsqldb.jdbcDriver");
	    ds.setUrl("jdbc:hsqldb:mem:db:leak");
	    ds.setMaxTotal(15);
	    return ds;
	  }

}
