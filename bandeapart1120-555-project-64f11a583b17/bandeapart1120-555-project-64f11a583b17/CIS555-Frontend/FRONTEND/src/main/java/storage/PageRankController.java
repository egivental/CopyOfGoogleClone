package storage;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class PageRankController {
    DataSource ds;

//    final String DATABASE_NAME = "cis555project";
//    final String JDBC_HOST = "searchengine-555-rds.cg7a8oblggud.us-east-1.rds.amazonaws.com:3306";
//    final String JDBC_URL  = "jdbc:mysql://" + JDBC_HOST + "/" + DATABASE_NAME;
//    final String CREATE_URL  = "jdbc:mysql://" + JDBC_HOST + "/";
//    final String JDBC_USER  ="admin";
//    final String JDBC_PASSWORD = "cis555cis555";
	private static final String DATABASE_NAME = "searchengine_dev";
	private static final String JDBC_HOST = "searchengine-dev.cfy6nalba13c.us-east-1.rds.amazonaws.com:3306";
	private static final String JDBC_URL = "jdbc:mysql://" + JDBC_HOST + "/" + DATABASE_NAME;
	private static final String JDBC_USER = "admin";
	private static final String JDBC_PASSWORD = "cis555cis555";

    public PageRankController() {
        this.ds = getDataSource();
    }

    public List<Float> queryPRbyURLs(List<String> urls) throws SQLException {

        HashMap<String, Float> result = new HashMap<>();
        StringBuilder sb = new StringBuilder();
        for (int i =  0; i < urls.size(); i++ ) {
            if (i == 0) {
                sb.append('(');
            }
            sb.append("\"").append(urls.get(i)).append("\"");
            if (i == urls.size() - 1) {
                sb.append(");");
            } else {
                sb.append(',');
            }

        }

        try (Connection conn = ds.getConnection()) {

            try (PreparedStatement ps = conn.prepareStatement(SQLQuery.SQL_QUERY_PRS_BY_URLS.value() + sb.toString())) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String url = rs.getString("url");
                        float rank = rs.getFloat("pageRank");
                        result.put(url, rank);
                    }
                }
            }
        }
        return urls.stream().map(url -> result.getOrDefault(url, -1.0f)).collect(Collectors.toList());
    }

    private final DataSource getDataSource() {
        if (this.ds != null) {
            return this.ds;
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(JDBC_URL);
        config.setUsername(JDBC_USER);
        config.setPassword(JDBC_PASSWORD);
        config.setConnectionTimeout(10000);
        config.setMaximumPoolSize(10);

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

        DataSource ds = (DataSource) new HikariDataSource(config);
        return ds;
    }
}
