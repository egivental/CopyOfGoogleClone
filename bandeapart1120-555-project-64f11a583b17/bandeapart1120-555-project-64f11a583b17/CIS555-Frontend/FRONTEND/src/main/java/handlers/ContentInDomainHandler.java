package handlers;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import front.FrontController;
import spark.Request;
import spark.Response;
import spark.Route;
public class ContentInDomainHandler implements Route{

	@Override
	public Object handle(Request request, Response response) throws Exception {
        String[] splat = request.splat();
        String domain = splat[0];
        StringBuilder sb = new StringBuilder();
		String css = 
		"<style type=\"text/css\">\r\n"+
			"body {"+
			"background:url(\"https://source.unsplash.com/X4l3CjcDvic\");\r\n"+
			"background-attachment:fixed;} "
		+"h2 {"
//		+"padding-left: 30px; padding-right: 30px;"
		+ "font:40px Lobster;"
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
        sb.append("<h2>Urls with domain " + domain + " </h2>");
        for (String url: FrontController.getDocRDSController().queryUrlsUnderDomain(domain, 1000)) {
            sb.append("<div><a href=\"/crawler/showurl/?url=" + url + "\">" +url+ "</a>" +  "</div>");
            }
            return sb.toString();
	}

}
