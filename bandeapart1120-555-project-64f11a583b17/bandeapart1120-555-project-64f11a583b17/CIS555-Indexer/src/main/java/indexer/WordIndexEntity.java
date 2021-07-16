package indexer;

public class WordIndexEntity {
	private String word;
	private String docId; // string hash
	private double occurences;
	
	public WordIndexEntity(String docId, String word, double tf) {
		this.word = word;
		this.docId = docId;
		this.occurences = Math.log(1 + tf);
	}
	
	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public String getDocId() {
		return docId;
	}

	public void setDocId(String docId) {
		this.docId = docId;
	}

	public double getOccurences() {
		return occurences;
	}

	public void setOccurences(double occurences) {
		this.occurences = occurences;
	}
}
//
//	
//	
//
//}
