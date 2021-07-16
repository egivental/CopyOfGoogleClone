package handlers;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

import javax.imageio.ImageIO;

import front.FrontController;
import front.SearchEngine;
import front.WeatherDemo;
import spark.Request;
import spark.Response;
import spark.Route;
public class ResultPageHandler implements Route{

	@Override
	public Object handle(Request request, Response response) throws Exception {
		System.out.println("in result page handler");
		String res = "";
        String query = request.queryParams("query");
        String pagenum = request.queryParams("pageNum");
        int pn = 1;
        if (pagenum != null) {
            pn = Integer.parseInt(pagenum);
        }
        
        if (query == null || query.length() == 0) {
            response.redirect("/");
            return "";
        }
        SearchEngine engine = new SearchEngine(query, FrontController.getDocRDSController(), FrontController.getDocS3Controller(), FrontController.getPageRankController(), FrontController.getIndexerController());
        try {
            File myObj = new File(FrontController.getPagepath() + "resultPage.html");
            Scanner myReader = new Scanner(myObj);
            
            int[] resultPageNum = new int[1]; 
            //inserting dynamic contents to the page
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                if (data.strip().equals("^&*for_search^&*")) {
                    data = engine.searchQuery(pn, resultPageNum);
                }
                else if (data.strip().equals("^&*for_wheather^&*")) {
                    WeatherDemo check = new WeatherDemo(query);
                    data = check.insertWetherInfo();
                    if(data == null) {
                    	data = "";
                    }
                } else if(data.strip().equals("^&*for_page_number^&*")) {
                	System.out.println("the num of page is: " + resultPageNum[0]);
                	System.out.println("target page is: " + pn);
                	StringBuilder sb = new StringBuilder();
                	sb.append("       <div class=\"serp__pagination\">\r\n"
                			+ "          <ul>\r\n");
                			if(pn == 1) {
                				sb.append("<li><a class=\"serp__disabled\"><<</a></li>\r\n");
                				sb.append("<li><a class=\"serp__disabled\"><</a></li>\r\n");
                			} else {
                				sb.append("<li><a class=\"serp__active\" href=\"\\resultPage.html?query=" +query+ "&pageNum=" + 1 +"\"><<</a></li>");
                				sb.append("<li><a class=\"serp__active\" href=\"\\resultPage.html?query=" +query+ "&pageNum=" + (pn - 1) +"\"><</a></li>");
                			}
                	int startPageNum = pn - 2 < 1 ? 1 : pn - 2;
                	int endPageNum = 0;
                	if(startPageNum == 1) {
                		endPageNum = Math.min(5, resultPageNum[0]);
                	} else {// start = pn - 2
                		endPageNum = Math.min(pn + 2, resultPageNum[0]);
                	}
                	for(int i = startPageNum; i <= endPageNum; i++) {
                		if(i == pn) {
                			sb.append("<li><a class=\"serp__pagination-active\" href=\""+ "\\resultPage.html?query=" +query+ "&pageNum=" + i +"\">" + i + "</a></li>\r\n");
                		} else {
                			sb.append("<li><a href=\""+ "\\resultPage.html?query=" +query+ "&pageNum=" + i +"\">" + i + "</a></li>\r\n");
                		}
                	}
                	if(pn == resultPageNum[0]) {
                		sb.append("<li><a class=\"serp__disabled\">></a></li>\r\n");
                		sb.append("<li><a class=\"serp__disabled\">>></a></li>\r\n");
                	} else {
                		sb.append("<li><a class=\"serp__active\" href=\""+ "\\resultPage.html?query=" +query+ "&pageNum=" + (pn + 1) +"\">></a></li>\r\n");
                		sb.append("<li><a class=\"serp__active\" href=\""+ "\\resultPage.html?query=" +query+ "&pageNum=" + resultPageNum[0] +"\">>></a></li>\r\n");
                	}
                	sb.append("</ul>\r\n</div>");
                	data = sb.toString();
                } else if(data.strip().equals("^&*for_input^&*")) {
                	data = "<input name=\"query\" type=\"search\" class=\"serp__query\" maxlength=\"512\" autocomplete=\"off\" title=\"Search\" aria-label=\"Search\" dir=\"ltr\" spellcheck=\"false\" autofocus=\"autofocus\" placeholder=\"" + query + "\"></input>";
                }
                res += data;
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        return res;
	}

}
