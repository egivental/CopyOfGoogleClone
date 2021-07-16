package storage;

public enum SQLQuery {
    ADD_DOC("INSERT INTO DOCUMENT_TABLE (url, urlId, docId, docBlockName, docBlockIndex, lastCrawledTs, lastModifiedTs) VALUES (?, ?, ?, ?, ?, ?, ?);"),
    SELECT_DOC_BY_DOCID("SELECT * FROM DOCUMENT_TABLE USE INDEX (docIdIndex) WHERE docId=?;"),
    SELECT_DOC_BY_URLID("SELECT * FROM DOCUMENT_TABLE WHERE urlId=? LIMIT 1;"),
    SELECT_URL_BY_DOCID("SELECT url, docId FROM DOCUMENT_TABLE USE INDEX (docIdIndex) WHERE docId IN "),
    SELECT_URL_BY_URLID("SELECT url, urlId FROM DOCUMENT_TABLE WHERE urlId IN "),
    SELECT_URL_BY_ONE_URLID("SELECT url, urlId FROM DOCUMENT_TABLE WHERE urlId = "),
    URL_IS_SEEN("SELECT 1 FROM DOCUMENT_TABLE WHERE urlId=?;"),
    ;


    private String value;

    SQLQuery(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }
}
