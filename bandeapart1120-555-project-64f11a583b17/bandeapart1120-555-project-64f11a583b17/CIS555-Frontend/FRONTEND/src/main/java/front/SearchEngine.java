package front;

import opennlp.tools.stemmer.PorterStemmer;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import storage.*;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


//search engine funcitons
public class SearchEngine {
	
	enum MatchLevel {  
	    LOW, MID, HIGH  
	}  

	private static final int PAGE_NUM_PER_PAGE = 15;
    private String query;
    private DocRDSController docRDSController;
    private DocS3Controller docS3Controller;
    private PageRankController pageRankController;
    private IndexerController indexerController;

    public SearchEngine(String query, DocRDSController DocRDSController, DocS3Controller docS3Controller, PageRankController pageRankController, IndexerController indexerController)
    {
        this.query = query;
        this.docS3Controller = docS3Controller;
        this.docRDSController = DocRDSController;
        this.pageRankController = pageRankController;
        this.indexerController = indexerController;
    }
    
    public Map<String, Double> getTFIDFResults() {
        //receive indexer results
        long time = System.currentTimeMillis();
        List<IndexerEntity> indexerResults = indexerController.getIndexFromQuery(this.query);
        long time2 = System.currentTimeMillis();
        System.out.println("indexer run time: " + (time2 - time));
        System.out.println("Indexer results got.");
        if (indexerResults.size() == 0) {
            System.out.println("Indexer returned no results.");
            return null;
        }
        //	
        Map<String, Double> tfidfs = new HashMap<>();
        for (int i = 0 ; i < indexerResults.size(); i++) {
            IndexerEntity indexerResult = indexerResults.get(i);
            System.out.println(indexerResult);
            // docId : TF
            for (Map.Entry<String, Double> entry : indexerResult.getTfs().entrySet()) {
                double curr = tfidfs.getOrDefault(entry.getKey(), 0.0);// curr is the tfidfs sum of former words
                tfidfs.put(entry.getKey(), curr + entry.getValue() * (indexerResult.getIdf() == 0 ? 1 : indexerResult.getIdf()));
            }
        }
        return tfidfs; // doc, score
    }
    
    public List<Float> pageRank(List<List<String>> urls, boolean[] infos) {
		List<String> allUrls = new ArrayList<>();
		
		// infos is true, one docId MORE urls
		for (int i = 0 ; i < urls.size(); i++) {
		    List<String> list = urls.get(i);
		    if (list.size() > 1) {
		        infos[i] = true;
		    }
		    else {
		        infos[i] = false;
		    }
		    allUrls.addAll(list);
		}
		
		List<Float> prs = null;
		
		//get page ranks of urls
		    try {
		//                System.out.println(tfidfs.size());
		//                prs = myRDS.queryPRbyURLIDs(new ArrayList<>(tfidfs.keySet()));
		        prs = pageRankController.queryPRbyURLs(allUrls);
		    }
		    catch (Exception e) {
		        e.printStackTrace();
		        return null;
		    }
		//        }	
		    return prs;
    }
    
    public List<ResultUnit> getSortedCandidates(Map<String, Double> tfidfs, List<List<String>> urls, List<Float> prs, boolean[] infos) {
    	List<ResultUnit> candidates = new ArrayList<ResultUnit>();
    	int index = 0;
        int urlidx = 0;
        
        double maxPr = Integer.MIN_VALUE;
        double maxTfidf = Integer.MIN_VALUE;
        for (Map.Entry<String, Double> entry : tfidfs.entrySet()) { 
        	maxPr = Math.max(maxPr, entry.getValue());
        }
        
        for(Float pr : prs) {
        	maxTfidf = Math.max(maxTfidf, pr);
        }
        
        
        //computing weights for all url results
        for (Map.Entry<String, Double> entry2 : tfidfs.entrySet()) {
            ResultUnit candidate = new ResultUnit();
            candidate.docId = entry2.getKey();
            candidate.tfidf = entry2.getValue();
            
            List<String> partial = urls.get(index);
            if(partial.size() != 0) {
	            double max = 0;
	            int maxidx = 0;
	            int maxNum = 0;
	
	            //for urls which have the same web content with other urls, select the one with the highest score to store
	            for (int i = 0; i < partial.size(); i++) {
	                String url = partial.get(i);
	                double score = (candidate.tfidf/maxTfidf) * 0.7 + (prs.get(urlidx)/maxPr) * 0.3;
	                if (score >= max) {
	                    max = score;
	                    maxidx = i;
	                    maxNum = urlidx;
	                }
	                urlidx++;
	            }
	            
	            candidate.pr = prs.get(maxNum);
	            candidate.url = partial.get(maxidx);
	            candidate.weight = max;
	            candidate.other = false;
	            if (infos[index]) {
	            	//we have others, others are...
	                candidate.others = urls.get(index).stream().filter(e -> !e.equals(candidate.url)).collect(Collectors.toList());
	                candidate.other = true;
	            }
	
	            candidates.add(candidate);
	            }
            index++;
        }

        //sort all results base on the weight
        Comparator<ResultUnit> comparator = (ResultUnit c1, ResultUnit c2) -> Double.compare(c2.weight, c1.weight);
        Collections.sort(candidates, comparator);
        return candidates;
    }
    
    public boolean getContent(Queue<String> docids, Map<String, Integer> pos, String[] contents) {
        //concurrently get web contents for the results
        ContentGetter[] getters = new ContentGetter[15];

        for (int i = 0; i < 15; i++) {
            getters[i] = new ContentGetter(docids, pos, contents, docRDSController, docS3Controller);
        }
        for (int i = 0; i < 15; i++) {
            getters[i].start();
        }
        try {
            for (int i= 0; i < 15; i++) {
                getters[i].join();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
    
    public ArrayList<Integer> getPreResultIndex(List<ResultUnit> candidates, int startIndex, int numPageToShow, String[] contents, List<String> titles, List<String> briefs) {
    	ArrayList<Integer> prefin = new ArrayList<Integer>();
        List<Integer> top = new ArrayList<>();
        List<Integer> mid = new ArrayList<>();
        List<Integer> low = new ArrayList<>();
        //check if the title of the urls match any of the keywords in query, all match would go to top and would be desplayed at the top, partial match would go to medium, and other matches would go to low
        for (int i = startIndex; i < startIndex + numPageToShow; i++) {
//        	System.out.println("-------------numPageToShow is: "+ numPageToShow);
            ResultUnit candidate = candidates.get(i);
            String content = contents[i - startIndex];
            Document doc;
            doc = Jsoup.parse(content);
            if (doc.title() != null && doc.title().length() != 0) {
                titles.add(doc.title());
                switch (StemmMatch(doc.title(), query)) {
                case HIGH:
                	top.add(i);
                	break;
                case MID:
                	mid.add(i);
                	break;
                case LOW:
                	low.add(i);
                	break;
                default:
                	if(candidate.url.contains("wikipedia")) {
                		mid.add(i);
                	}
                }
            }
            else {
                titles.add(candidate.url);
                low.add(i);
            }
            String des = doc.select("meta[name=description]").attr("content");
            if (des != null && des.length() != 0) {
                briefs.add(des);
            }
            else {
                briefs.add(doc.body().text().substring(0, Math.min(200, doc.body().text().length())));
            }
        }
        prefin.addAll(top);
        prefin.addAll(mid);
        prefin.addAll(low);
        return prefin;
    }
    
    public ArrayList<Integer> sortByFileTitles(List<ResultUnit> candidates, List<String> titles, List<String> briefs, int targetPageNum) {
        Queue<String> docids = new LinkedList<>();
        Map<String, Integer> pos = new HashMap<>();
        String[] contents= new String[15];

        int startIndex = PAGE_NUM_PER_PAGE * (targetPageNum - 1);
        int numPageToShow = Math.min(PAGE_NUM_PER_PAGE, candidates.size() - startIndex);
        
        for (int i = startIndex; i < startIndex + numPageToShow; i++) {
            String docid = candidates.get(i).docId;
            docids.add(docid);
            pos.put(docid, i - startIndex); //docid and position 
        }

        if(!getContent(docids, pos, contents)) {
        	return null;
        }


        ArrayList<Integer> prefin = getPreResultIndex(candidates, startIndex, numPageToShow, contents, titles, briefs);
        ArrayList<Integer> fin = new ArrayList<>();
        ArrayList<Integer> discard = new ArrayList<>();

        //manually put down net spam sites
        String[] stopUrls = {"mediaman.com", "booked.net", "addtoany.com", "hudsonreview.com"};
        for (int i = 0 ; i < prefin.size(); i++) {
        	// idx of candidate
            int idx = prefin.get(i);
            if (Arrays.stream(stopUrls).parallel().anyMatch(candidates.get(idx).url::contains)) {
                discard.add(idx);
            }
            else {
                fin.add(idx);
            }
        }
        fin.addAll(discard);
		return fin;    	
    }
    
    
    public String writeResult(List<ResultUnit> candidates, int startIndex, ArrayList<Integer> fin, List<String> titles, List<String> briefs) {
        //writing dynamic content results
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fin.size(); i++) {
        	sb.append("<div class=\"serp__result\">\n");
            int idx = fin.get(i);
//            int idx = i;
            ResultUnit candidate = candidates.get(idx);
            String title = titles.get(idx - startIndex);
            String des = briefs.get(idx - startIndex);
            sb.append("<a href=\"" + candidate.url + "\" target=\"_blank\">\n");
            sb.append("<div class=\"serp__title\">" + title + "</div>\n");
            sb.append("<div class=\"serp__url\">" + candidate.url + "</div>\n");
            sb.append("<div><a href=\"/crawler/showurl/?url=" + candidate.url + "\">see this page in cached</a><a> or </a>");
            sb.append("<a href = \"javascript:void(0)\" onclick = \"document.getElementById('light" + i + "').style.display='block';document.getElementById('fade').style.display='block'\">check information of this page</a>");
            sb.append("<div id=\"light" + i + "\" class=\"white_content\"><p>TF * IDF: " + candidate.tfidf + "</p><p>PageRank: " + candidate.pr + "</p><p>Weight: " + candidate.weight + "</p>");
            if (candidate.other) {
            	sb.append("<p>Other pages with the same content: </p>");
                System.out.println(i);
                for (String url : candidate.others) {
                	sb.append("<p><a href=\"" + url + "\">" + url + "</a></p>");
                }
            }
            sb.append("\n<a href = \"javascript:void(0)\" onclick = \"document.getElementById('light" + i + "').style.display='none';document.getElementById('fade').style.display='none'\">click here to close</a></div> \n");
            sb.append("        <div id=\"fade\" class=\"black_overlay\"></div> ");
            sb.append("</div>");
            sb.append("</a>\n<span class=\"serp__description\">" + des + "</span>\n</div>\n");
        }    
        return sb.toString();
    }
    
    public String searchQuery(int targetPageNum, int[] totalPageNum) { 
        if (this.query == null) {
            System.out.println("Please specify a query.");
            return null;
        }
        long timeStart = System.currentTimeMillis();
        Map<String, Double> tfidfs = getTFIDFResults();
        if(tfidfs == null) {
        	System.out.println("tfidfs result is null");
        	return null;
        }
        
        if (tfidfs.size() == 0) {
            return "<div class=\"serp__no-results\">\n" +
            		"<h1>Hmmm...<h1>" +
                    "          <h3><strong>We couldn't find any matches for \"" + this.query + "\"</strong></h3>\n" +
                    "          <p>Suggestions:</p>\n" +
                    "		   <p> Double check your search for any typos or spelling errors or try a different search term"+			
                    "        </div>\n";
        }

        //receve urls of the document ids
        long time2 = System.currentTimeMillis();
        List<List<String>> urls = new ArrayList<>();
        try {
             urls = docRDSController.queryUrlsByDocIds(new ArrayList(tfidfs.keySet()));
        }

        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        long time3 = System. currentTimeMillis();
        System.out.println("url time: " + (time3 - time2));
        System.out.println("urls got.");

        time3 = System. currentTimeMillis();
		boolean[] infos = new boolean[urls.size()];
        List<Float> prs = pageRank(urls, infos);
        if(prs == null) {
        	System.out.println("prs is null");
        	return null;
        }
		long time4 = System.currentTimeMillis();
		System.out.println("page rank time: " + (time4 - time3));
		System.out.println("page rank got.");  
		
        List<ResultUnit> candidates = getSortedCandidates(tfidfs, urls, prs, infos);
        totalPageNum[0] = candidates.size() % PAGE_NUM_PER_PAGE == 0 ? candidates.size() / PAGE_NUM_PER_PAGE : 1 + candidates.size() / PAGE_NUM_PER_PAGE;
        
        
        List<String> titles = new ArrayList<>();
        List<String> briefs = new ArrayList<>();
        ArrayList<Integer> fin = sortByFileTitles(candidates, titles, briefs, targetPageNum);
        if(fin == null) {
        	return null;
        }
        int startIndex = PAGE_NUM_PER_PAGE * (targetPageNum - 1);
        long time5 = System. currentTimeMillis();
        StringBuilder sb = new StringBuilder();
        DecimalFormat df = new DecimalFormat("0.0000");
        sb.append("<div><p color = \"gray\">About "+candidates.size()+" results("+ df.format((double)TimeUnit.MILLISECONDS.toSeconds(time5 - timeStart))+ " seconds)</p>\n\n</div>\n");
        sb.append(writeResult(candidates, startIndex, fin, titles, briefs));
       
        System.out.println("content time: " + (time5 - time4));
        System.out.println("contents got.");
        System.out.println("Search ended.");
        return sb.toString();
    }
     
    //check title and query match
    private MatchLevel StemmMatch(String s1, String s2) {
        PorterStemmer stemmer = new PorterStemmer();
        
        String[] wordsArray1 = Utils.normalizeInputStr(s1);
        String[] wordsArray2 = Utils.normalizeInputStr(s2);

        List<String> stemmedWords1 = Utils.stemTheWords(wordsArray1, stemmer);
        List<String> stemmedWords2 = Utils.stemTheWords(wordsArray2, stemmer);

        if (stemmedWords1.containsAll(stemmedWords2)) {
            return MatchLevel.HIGH;
        }
        else {
            for (int i = 0; i < stemmedWords2.size(); i++) {
                if (stemmedWords1.contains(stemmedWords2.get(i))) {
                    return MatchLevel.MID;
                }
            }
            return MatchLevel.LOW;
        }
    }
}
