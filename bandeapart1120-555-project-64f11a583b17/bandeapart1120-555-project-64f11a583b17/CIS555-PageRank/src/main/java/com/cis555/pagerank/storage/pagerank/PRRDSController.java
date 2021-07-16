package com.cis555.pagerank.storage.pagerank;

import com.cis555.pagerank.PageRank;
import com.cis555.pagerank.configs.PageRankConfig;
import com.cis555.pagerank.utils.PageRankUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PRRDSController {

    private static Logger logger = LoggerFactory.getLogger(PRRDSController.class);

    private static final String DATABASE_NAME = "searchengine_dev";
    private static final String JDBC_HOST = "searchengine-dev.cfy6nalba13c.us-east-1.rds.amazonaws.com:3306";
    private static final String JDBC_URL  = "jdbc:mysql://" + JDBC_HOST + "/" + DATABASE_NAME + "?autoReconnect=true";
    private static final String CREATE_URL  = "jdbc:mysql://" + JDBC_HOST + "/";
    private static final String JDBC_USER  ="admin";
    private static final String JDBC_PASSWORD = "cis555cis555";

    private static String SQL_ADD_PAGERANK = "REPLACE INTO "+ PageRankConfig.PAGERANK_TABLE
            + " (urlID, url, pageRank) "
            + "VALUES (?, ?, ?);";

    private static String SQL_ADD_PAGERANKS = "REPLACE INTO "+ PageRankConfig.PAGERANK_TABLE
            + " (urlID, url, pageRank) "
            + "VALUES ";

    private static String SQL_QUERY_PR_BY_URL = "SELECT pageRank FROM "+ PageRankConfig.PAGERANK_TABLE +
            " WHERE url=?;";


    private static String SQL_QUERY_PRS_BY_URLS = "SELECT * FROM "+ PageRankConfig.PAGERANK_TABLE +
            " WHERE url IN ";


    private static final DataSource ds = getDataSource();

    public PRRDSController() {
    }

    /**
     * GET THE CONNECTION WITH RDS AND CONFIGURE THE CONNECTION SETTINGS
     * @return
     */
    private static final DataSource getDataSource() {
        if (PRRDSController.ds != null) {
            return PRRDSController.ds;
        }
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(JDBC_URL);
        config.setUsername(JDBC_USER);
        config.setPassword(JDBC_PASSWORD);
//		config.setIdleTimeout(60000);
        config.setConnectionTimeout(100000);
        config.setMaximumPoolSize(20);
//        config.setMaxLifetime(20000); // test


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

    public boolean addPageRank(String url, double pagerank) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(SQL_ADD_PAGERANK)) {
                ps.setString(1, PageRankUtil.toUrlId(url));
                ps.setString(2, url);
                ps.setDouble(3, pagerank);

                try {
                    ps.executeUpdate(); // ret = 1
                } catch (SQLIntegrityConstraintViolationException e) {
                    return false;
                }
            }
        }

        return true;
    }

    public boolean addPageRanks(List<String[]> prs) throws SQLException {

        StringBuilder sb = new StringBuilder();

        for (String[] pr : prs) {
//            System.out.println(Arrays.toString(pr));
            String url = pr[0];
            String urlId = PageRankUtil.toUrlId(url);
            String pagerank = pr[1];

            sb.append('(').append('"').append(urlId).append('"');
            sb.append(',').append('"').append(url).append('"');
            sb.append(',').append('"').append(pagerank).append('"');
            sb.append(')').append(",");
        }

        sb.deleteCharAt(sb.length() - 1);
        sb.append(';');

//        System.out.println(sb.toString());
        try (Connection conn = ds.getConnection()) {
            //  String values = urlIds.stream().map((s) -> {return "\""+s+"\"";}).collect(Collectors.joining(",", "(", ")"));
            try (PreparedStatement ps = conn.prepareStatement(SQL_ADD_PAGERANKS + sb.toString())) {
//                ps.setString(1, sb.toString());
                System.out.println(ps.toString());
                try {
                    ps.executeUpdate(); // ret = 1
                } catch (SQLIntegrityConstraintViolationException e) {
                    return false;
                }
            }
        }

        return true;

    }

    public double queryPRbyURL(String url) throws SQLException {

        try (Connection conn = ds.getConnection()) {
            try (PreparedStatement ps = conn.prepareStatement(SQL_QUERY_PR_BY_URL)) {
                ps.setString(1, url);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getFloat("pageRank");
                    }
                }
            }
        }
        return -1;
    }

    public List<Double> queryPRbyURLs(List<String> urls) throws SQLException {

        HashMap<String, Double> result = new HashMap<>();
        StringBuilder sb = new StringBuilder();
        for (int i =  0; i < urls.size(); i++ ) {
            if (i == 0) {
                sb.append('(');
            }
            sb.append("'").append(urls.get(i)).append("'");
            if (i == urls.size() - 1) {
                sb.append(");");
            } else {
                sb.append(',');
            }

        }

        try (Connection conn = ds.getConnection()) {

            try (PreparedStatement ps = conn.prepareStatement(SQL_QUERY_PRS_BY_URLS + sb.toString())) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String url = rs.getString("url");
                        double rank = rs.getFloat("pageRank");
                        result.put(url, rank);
                    }
                }
            }
        }
        return urls.stream().map(url -> result.getOrDefault(url, -1.0)).collect(Collectors.toList());
    }

    public static void main(String[] args) throws SQLException {
        List<String[]> urls = new ArrayList<>();
        urls.add(new String[]{"http://google.com?search=", "2.123321"});
        urls.add(new String[]{"test123123123", "12312323"});

        PRRDSController prrdsController = new PRRDSController();
        prrdsController.addPageRanks(urls);
    }





}
