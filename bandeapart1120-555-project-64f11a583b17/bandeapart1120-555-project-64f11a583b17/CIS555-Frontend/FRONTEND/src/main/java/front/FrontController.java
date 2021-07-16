package front;

//nohup mvn clean compile exec:java -Dexec.mainClass="front.FrontController" &
//ssh -i mykeypem.pem ubuntu@3.84.146.60

import storage.*;
import handlers.CSSHandler;
import handlers.ContentInDomainHandler;
import handlers.HomePageHandler;
import handlers.IMGHandler;
import handlers.PageContentHandler;
import handlers.ResultPageHandler;
import handlers.ShowUrlHandler;
import handlers.TOPPagesHandler;

import static spark.Spark.*;

import java.sql.SQLException;

public class FrontController {

    private final static String PAGEPATH = "../pages/";
    private static DocRDSController docRDSController;
    private static DocS3Controller docS3Controller;
    private static IndexerController indexController;
    private static PageRankController pageRankController;
    public static int NUM_OF_DISTINCT_DOC = 0;
	

	private static void initialization() {
        System.out.println("Server started.");
        docRDSController = new DocRDSController();
        docS3Controller = new DocS3Controller();
        indexController = new IndexerController();
        pageRankController = new PageRankController();
        try {
        	NUM_OF_DISTINCT_DOC = docRDSController.getDistictDocNum();
			System.out.println("totoal number is " + NUM_OF_DISTINCT_DOC);
		} catch (SQLException e) {
			e.printStackTrace();
		}
        port(8080);
	}

	
	
	
    public static String getPagepath() {
		return PAGEPATH;
	}




	public static DocRDSController getDocRDSController() {
		return docRDSController;
	}




	public static DocS3Controller getDocS3Controller() {
		return docS3Controller;
	}




	public static IndexerController getIndexerController() {
		return indexController;
	}




	public static PageRankController getPageRankController() {
		return pageRankController;
	}




	//main function to registe r routes
    public static void main(String[] args) {
    	
    	initialization();

        //home page
        get("/", new HomePageHandler());

        //css getter 
        get("/css/:name", new CSSHandler());

        //images getter
        get("/img/:name", new IMGHandler());

        //result page
        get("/resultPage.html", new ResultPageHandler());

        //list top pages
        get("/crawler/list", new TOPPagesHandler());

        get("/crawler/showurl/", new ShowUrlHandler());

        get("/crawler/cached/", new PageContentHandler());

        get("/crawler/domain/*/", new ContentInDomainHandler());
        }

}
