package front;

import java.util.List;

//structure to stroe search results
public class ResultUnit {

    String url;
    String docId;
    double tfidf;
    double pr;
    double weight;
    String content;
    List<String> others;
    boolean other;

    public void setUrl(String url) {
        this.url = url;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }

    public void setTfidf(Double tfidf) {
        this.tfidf = tfidf;
    }

    public void setPr(Double pr) {
        this.pr = pr;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

}
