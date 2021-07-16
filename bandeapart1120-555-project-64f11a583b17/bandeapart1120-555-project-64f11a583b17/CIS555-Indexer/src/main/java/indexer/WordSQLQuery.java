package indexer;

public enum WordSQLQuery {
	ADD_WORD("INSERT INTO INDEX_TABLE (word, docId, tf) " +
            "VALUES (?, ?, ?);"),
	ADD_BLOCK("INSERT INTO BLOCK_INDEX_TABLE (blockName, timeLastIndexed) VALUES (?, ?);"),
	CHECK_BLOCK_INDEX("SELECT COUNT(*) FROM BLOCK_INDEX_TABLE WHERE blockName = ?;");
    

    private String value;

    WordSQLQuery(String value){
        this.value = value;
    }

    public String value(){
        return this.value;
    }
}
