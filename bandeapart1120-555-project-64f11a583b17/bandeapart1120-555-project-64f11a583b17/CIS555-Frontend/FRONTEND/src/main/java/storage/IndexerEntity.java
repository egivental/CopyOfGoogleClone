package storage;

import java.util.HashMap;
import java.util.Map;


/**
 * This class is for an indexer operation
 * 
 *
 */
public class IndexerEntity {

	
	private double idf;
	private String word;
	// docId, TF
	private Map<String, Double> tfs;
	
	
	public IndexerEntity() {
		tfs = new HashMap<>();
	}
	
	public IndexerEntity(double idf, String word, Map<String, Double> tfs) {
		super();
		this.idf = idf;
		this.word = word;
		this.tfs = tfs;
	}

	public double getIdf() {
		return idf;
	}

	public void setIdf(double idf) {
		this.idf = idf;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public Map<String, Double> getTfs() {
		return tfs;
	}

	public void setTfs(Map<String, Double> tfs) {
		this.tfs = tfs;
	}
	
	
	public void addDoc(String docId, double tf) {
		tfs.put(docId, tf);
	}

	@Override
	public String toString() {
		return "IndexerEntity [idf=" + idf + ", word=" + word + ", tfs=" + tfs + "]";
	}
	
}
