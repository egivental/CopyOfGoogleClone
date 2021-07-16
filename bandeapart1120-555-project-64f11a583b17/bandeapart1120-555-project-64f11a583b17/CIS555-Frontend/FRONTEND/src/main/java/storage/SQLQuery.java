package storage;

public enum SQLQuery {
    ADD_DOC("INSERT INTO DOCUMENT_TABLE (url, urlId, docId, docBlockName, docBlockIndex, lastCrawledTs, lastModifiedTs) VALUES (?, ?, ?, ?, ?, ?, ?);"),
    SELECT_DOC_BY_DOCID("SELECT * FROM DOCUMENT_TABLE USE INDEX (docIdIndex) WHERE docId=?;"),
    SELECT_DOC_BY_URLID("SELECT * FROM DOCUMENT_TABLE WHERE urlId=? LIMIT 1;"),
    SELECT_URL_BY_DOCID("SELECT url, docId FROM DOCUMENT_TABLE USE INDEX (docIdIndex) WHERE docId IN "),
    SELECT_URL_BY_URLID("SELECT url, urlId FROM DOCUMENT_TABLE WHERE urlId IN "),
    SELECT_URL_BY_ONE_URLID("SELECT url, urlId FROM DOCUMENT_TABLE WHERE urlId = "),
    URL_IS_SEEN("SELECT 1 FROM DOCUMENT_TABLE WHERE urlId=?;"),
    SQL_URL_UNDER_DOMAIN("SELECT url FROM DOCUMENT_TABLE HAVING SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(url, '/', 3), '://', -1), '/', 1), '?', 1)=? LIMIT ?;"),
    
    // for index and idf table
    SQL_TOP_K_FREQ("SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(url, '/', 3), '://', -1), '/', 1), '?', 1) AS DOMAIN, COUNT(*) AS FREQ FROM DOCUMENT_TABLE GROUP BY DOMAIN ORDER BY FREQ DESC LIMIT ?;"),
    SQL_CREATE_INDEX_TABLE("CREATE TABLE IF NOT EXISTS `INDEX_TABLE` (" + 
			"`entryId` int NOT NULL AUTO_INCREMENT," + 
			"`word` VARCHAR(50) NOT NULL," + 
			"`docId` VARCHAR(40) NOT NULL," + 
			"`tf` DOUBLE NOT NULL," + 
			"PRIMARY KEY (`entryID`)" + 
			");"),
	SQL_CREATE_IDF_TABLE("CREATE TABLE IF NOT EXISTS `IDF_TABLE` (" + 
			"`word` VARCHAR(50) NOT NULL," + 
			"`idf` DOUBLE NOT NULL," + 
			"PRIMARY KEY (`word`)" + 
			");"),
	SQL_QUERY_PRS_BY_URLS("SELECT * FROM `PAGERANK_TABLE` WHERE url IN "),
	SQL_QUERY_COUNT_DISTINCT_DOC("SELECT COUNT(distinct docId) AS TOTAL FROM DOCUMENT_TABLE"),
    ;


    private String value;

    SQLQuery(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }
}
