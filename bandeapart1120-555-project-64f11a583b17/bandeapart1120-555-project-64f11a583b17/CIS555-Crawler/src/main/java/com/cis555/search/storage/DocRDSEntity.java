package com.cis555.search.storage;

/**
 * urlId and docId are both hashed string
 */
public class DocRDSEntity {
	private String url;
	private String urlId; //Hashed by SHA-1
	private String docId; //Hashed by SHA-1
	private String docBlockName;
	private int docBlockIndex;
	private long lastCrawledTs;
	private long lastModifiedTs;

	
	public DocRDSEntity(String url, String urlId, String docId, String docBlockName, int docBlockIndex,
						long lastCrawledTs, long lastModifiedTs) {
		this.url = url;
		this.urlId = urlId;
		this.docId = docId;
		this.docBlockName = docBlockName;
		this.docBlockIndex = docBlockIndex;
		this.lastCrawledTs = lastCrawledTs;
		this.lastModifiedTs = lastModifiedTs;
	}

	public final String getUrl() {
		return url;
	}
	public final void setUrl(String url) {
		this.url = url;
	}
	public final String getUrlId() {
		return urlId;
	}
	public final String getDocId() {
		return docId;
	}
	public final String getDocBlockName() {
		return docBlockName;
	}
	public final int getDocBlockIndex() {
		return docBlockIndex;
	}
	public final long getLastCrawledTs() {
		return lastCrawledTs;
	}
	public final long getLastModifiedTs() {
		return lastModifiedTs;
	}
	
	@Override
	public String toString() {
		return url + " " + urlId + " " + docId + " " + docBlockName + " " + docBlockIndex + " " + lastCrawledTs + " " + lastModifiedTs;
	}
}
