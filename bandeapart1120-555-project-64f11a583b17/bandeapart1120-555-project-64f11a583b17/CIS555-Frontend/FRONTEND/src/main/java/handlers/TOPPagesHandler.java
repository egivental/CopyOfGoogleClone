package handlers;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Scanner;

import javax.imageio.ImageIO;

import front.FrontController;
import spark.Request;
import spark.Response;
import spark.Route;
public class TOPPagesHandler implements Route{

	@Override
	public Object handle(Request request, Response response) throws Exception {
		
		String css = 
		"<style type=\"text/css\">\r\n"+
//		".top {\r\n"
//		+ "  background:url(\"https://source.unsplash.com/WLUHO9A_xik/1600x900\");\r\n"
//		+ "  min-height: 100%;\r\n"
//		+ "  width: 100%;\r\n"
//		+ "  font:16px Lobster;		"
//		+ "}"
		"body {"+
		"background:url(\"https://source.unsplash.com/X4l3CjcDvic\");\r\n"+
		"background-attachment:fixed;} "
//		+"display: flex;"
//		+"justify-content: center; align-items: center;\r\n"
		+"h2 {"
//		+"padding-left: 30px; padding-right: 30px;"
		+ "font:40px Lobster;"
		+ "}"
		+".top {"
		+"background-attachment:fixed;"
		+"justify-content: center; align-items: center;\r\n"
		+ " }"
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
		
        StringBuffer sb = new StringBuffer();
        sb.append("<div class = \"top\">");
        sb.append("<h2>Top 100 Freqeuent Domains</h2>\n\n\n\n");
        sb.append("<table style=\"width:auto\">");
        sb.append("<tr><th>Url</th> <th>Freq</th></tr>");
        for (Map.Entry<String, Integer> entry: FrontController.getDocRDSController().topKFrequentDomain(100)) {
            String domain = entry.getKey();
            int freq = entry.getValue();
            sb.append("<tr>");
            sb.append("<td>" + "<a href=\"/crawler/domain/" + domain + "/\">" + domain + "</a>" +  "</td>");
            sb.append("<td>" + freq + "</td>");
            sb.append("</tr>");
        }
        sb.append("</table>");
        sb.append("</div>");
        String topPage = sb.toString();
        

        return headerPage + "<body>" + topPage + "</body>";
	}

}
