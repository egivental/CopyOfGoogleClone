package storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import java.util.List;


import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import front.FrontController;
import front.Utils;
import opennlp.tools.stemmer.PorterStemmer;

public class IndexerController {

	private static final String DATABASE_NAME = "searchengine_dev";
	private static final String JDBC_HOST = "searchengine-dev.cfy6nalba13c.us-east-1.rds.amazonaws.com:3306";
	private static final String JDBC_URL = "jdbc:mysql://" + JDBC_HOST + "/" + DATABASE_NAME;
	private static final String JDBC_USER = "admin";
	private static final String JDBC_PASSWORD = "cis555cis555";
	private static final DataSource ds = getDataSource();
	private static PorterStemmer stemmer = new PorterStemmer();
	
	/**
	 * connect to the RDS database
	 * 
	 * @return DataSource, corresponding database
	 */
	private final static DataSource getDataSource() {
		if (IndexerController.ds != null) {
			return IndexerController.ds;
		}
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(JDBC_URL);
		config.setUsername(JDBC_USER);
		config.setPassword(JDBC_PASSWORD);
		config.setConnectionTimeout(10000);
		config.setMaximumPoolSize(10);
		
		config.addDataSourceProperty("cachePrepStmts", true);
		config.addDataSourceProperty("prepStmtCacheSize", 500);
		config.addDataSourceProperty("prepStmtCacheSqlLimit", 2048);
		config.addDataSourceProperty("useServerPrepStmts", true);
		config.addDataSourceProperty("useLocalSessionState", true);
		config.addDataSourceProperty("rewriteBatchedStatements", true);
		config.addDataSourceProperty("cacheResultSetMetadata", true);
		config.addDataSourceProperty("cacheServerConfiguration", true);
		config.addDataSourceProperty("elideSetAutoCommits", true);
		config.addDataSourceProperty("maintainTimeStats", false);
		
		DataSource ds = (DataSource) new HikariDataSource(config);
		return ds;
	}
			
	public List<IndexerEntity> getIndexFromQuery(String query) {
		List<IndexerEntity> res = new ArrayList<>();

		String[] wordsArray = Utils.normalizeInputStr(query);
		
		List<String> stemmedWords = Utils.stemTheWords(wordsArray, stemmer);
		
		for (String stemmedWord: stemmedWords) {
			System.out.println("stemmedWord : " + stemmedWord);
			IndexerEntity wordIndex = getIndex(stemmedWord);
			long numOfDocWithWord = wordIndex.getTfs().size();
			double idf = Math.log(FrontController.NUM_OF_DISTINCT_DOC/(1 + numOfDocWithWord));
			wordIndex.setIdf(idf);
			res.add(wordIndex);
		}
		
		return res;
	}
	
	

	public static IndexerEntity getIndex(String word) {
		System.out.println(word);
		String query = "SELECT * FROM INDEX_TABLE WHERE word = \"" + word + "\";";
		System.out.println(query);
		IndexerEntity res = new IndexerEntity();
		res.setWord(word);
		int x = 0;
		try (Connection conn = ds.getConnection()) {
		    PreparedStatement ps = conn.prepareStatement(query);
		    ResultSet rs = ps.executeQuery();
		    
		    while (rs.next()) {
		    	String docId = rs.getString("docId");
		    	double tf = rs.getDouble("tf");
		    	res.addDoc(docId, tf);
		    }
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return res;
		
	}	

	public static void main(String[] args) throws SQLException {
		getDataSource();
		IndexerController idxController = new IndexerController();
		List<IndexerEntity> list = idxController.getIndexFromQuery("good");
		
		for (IndexerEntity entity: list) {
			System.out.println(entity);
		}
	}
	
		
}
