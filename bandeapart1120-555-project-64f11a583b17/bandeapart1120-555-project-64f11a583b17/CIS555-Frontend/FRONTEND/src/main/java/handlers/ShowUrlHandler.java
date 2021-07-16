package handlers;
import java.io.File;
import java.io.FileNotFoundException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Scanner;

import front.FrontController;
import front.URLInfo;
import spark.Request;
import spark.Response;
import spark.Route;
import storage.DocRDSEntity;
public class ShowUrlHandler implements Route{

	@Override
	public Object handle(Request request, Response response) throws Exception {
		
		
		 String url = request.queryParams("url");
         URLInfo urlInfo = new URLInfo(url);
         String urlId = urlInfo.toUrlId();
         List<DocRDSEntity> docIndexList = FrontController.getDocRDSController().queryDocByUrlId(urlId);
         if (docIndexList.size() == 0) {
             return "URL " + url + " cannot be found!";
         }

         DocRDSEntity docIndexEntity = docIndexList.get(0);
         StringBuilder sb = new StringBuilder();
 		String css = 
 				"<style type=\"text/css\">\r\n"+
 					"body {"+
 					"background:url(\"https://source.unsplash.com/X4l3CjcDvic\");\r\n"+
 					"background-attachment:fixed;} "
 				+".word {"
// 				+"padding-left: 30px; padding-right: 30px;"
 				+ "font:20px Lobster;"
 				+ "}"
 				+ "</style>";
 				String headerPage = "<head>\r\n" +
 		                "	<meta charset=\"utf-8\">\r\n" +
 		                "	<title>Crawler List</title>\r\n" +
 		                "  <link href='https://fonts.googleapis.com/css?family=Monoton' rel='stylesheet' type='text/css'>\r\n"
 		                + "  <link href='https://fonts.googleapis.com/css?family=Lobster' rel='stylesheet' type='text/css'>\r\n"
 		                + "  <!--Font Awesome-->\r\n"
 		                + "  <link href=\"//maxcdn.bootstrapcdn.com/font-awesome/4.2.0/css/font-awesome.min.css\" rel=\"stylesheet\">\r\n"
 		                + "  <link rel=\"stylesheet\" type=\"text/css\" href=\"css/homePage.css\">" +
 		                css +
 		                "</head>\r\n";
 				
 				sb.append(headerPage);
         sb.append("<div><span class = \"word\">URL: </span>" + docIndexEntity.getUrl() + "</div>");
         sb.append("<div><span class = \"word\">Last Crawled Ts: </span>" + LocalDateTime.ofInstant(Instant.ofEpochMilli(docIndexEntity.getLastCrawledTs()), ZoneOffset.systemDefault()) + "</div>");
         sb.append("<iframe src=\"" + "/crawler/cached/?urlId=" + urlId + "\" style=\"height:100%;width:100%\"></iframe>");
         return sb.toString();
	}

}
