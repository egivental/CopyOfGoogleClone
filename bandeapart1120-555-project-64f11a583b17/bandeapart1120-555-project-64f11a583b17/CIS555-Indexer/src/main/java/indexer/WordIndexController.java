package indexer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.HashMap;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;

import javax.sql.DataSource;

import com.amazonaws.services.cloudformation.model.Output;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;    

public class WordIndexController {
	private static final String DATABASE_NAME = "searchengine_dev";
	private static final String JDBC_HOST = "searchengine-dev.cfy6nalba13c.us-east-1.rds.amazonaws.com";
	private static final String JDBC_URL = "jdbc:mysql://" + JDBC_HOST + "/" + DATABASE_NAME;
	private static final String JDBC_USER = "admin";
	private static final String JDBC_PASSWORD = "cis555cis555";
	private static final DataSource ds = getDataSource();
	
	private static final DataSource getDataSource() {
        if (WordIndexController.ds != null) {
            return WordIndexController.ds;
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(JDBC_URL);
        config.setUsername(JDBC_USER);
        config.setPassword(JDBC_PASSWORD);
        config.setConnectionTimeout(30000);
        config.setMaximumPoolSize(20);
        config.setLeakDetectionThreshold(5 * 60 * 1000);
        config.addDataSourceProperty("cachePrepStmts", true);
        config.addDataSourceProperty("prepStmtCacheSize", 250);
        config.addDataSourceProperty("prepStmtCacheSqlLimit", 2048);
        config.addDataSourceProperty("useServerPrepStmts", true);
        config.addDataSourceProperty("useLocalSessionState", true);
        config.addDataSourceProperty("rewriteBatchedStatements", true);
        config.addDataSourceProperty("cacheResultSetMetadata", true);
        config.addDataSourceProperty("cacheServerConfiguration", true);
        config.addDataSourceProperty("elideSetAutoCommits", true);
        config.addDataSourceProperty("maintainTimeStats", false);
        HikariDataSource ds = new HikariDataSource(config);
        return ds;
	}
	
	public WordIndexController() {
		//dummy constructor
	}


	static public boolean addWord(HashMap<String, Double> outputs) throws SQLException {
		boolean ret = true;
		
		try (Connection conn = ds.getConnection()) {
			conn.setAutoCommit(false);
			Statement stmt = conn.createStatement();
			for (String d : outputs.keySet()) {   //creates a new file instance  
				String word = d.split("\t")[0];  
				String file = d.split("\t")[1];
				if (word.length() > 45) {
					word = word.substring(0, 45);
				}
				stmt.addBatch("INSERT INTO INDEX_TABLE (word, docId, tf) VALUES ('"+ word + "', '" + file + "', " + String.valueOf(outputs.get(d)) + " )");
			}
			stmt.executeBatch();
			conn.commit();
		}
		return ret;
	}
	
	static public boolean addBlock(String blockId) throws SQLException {
		boolean ret = true;
		java.sql.Date d = new java.sql.Date(new java.util.Date().getTime());
		try (Connection conn = ds.getConnection()) {
		    try (PreparedStatement ps = conn.prepareStatement(WordSQLQuery.ADD_BLOCK.value())) {
		    	ps.setDate(2, d);;
		    	ps.setString(1, blockId);
		    	try {
		    		ps.executeUpdate();
		    	} catch (SQLIntegrityConstraintViolationException e) {
		    		ret = false;
		    	}
		    }
		}
		return ret;
	}
	
	static public int blockIndexed(String blockName) throws SQLException {
		int ret = 0;
		try (Connection conn = ds.getConnection()) {
		    try (PreparedStatement ps = conn.prepareStatement(WordSQLQuery.CHECK_BLOCK_INDEX.value())) {
		    	ps.setString(1, blockName);
		    	try (ResultSet rs = ps.executeQuery()) {
		    		rs.next();
		    		ret = rs.getInt(1);
		    	}
		    }
		}
		return ret;
	}
	
	public static void main(String[] args) throws SQLException {
        int red = blockIndexed("Test");
        System.out.println(red);
        addBlock("Test");
        red = blockIndexed("Test");
        System.out.println(red);
    }
}
