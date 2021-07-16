package handlers;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import front.FrontController;
import spark.Request;
import spark.Response;
import spark.Route;
public class HomePageHandler implements Route{

	@Override
	public Object handle(Request request, Response response) throws Exception {
        StringBuilder res = new StringBuilder();
        try {
        	File homePage = new File(FrontController.getPagepath() + "homePage.html");
            Scanner myReader = new Scanner(new File(FrontController.getPagepath() + "homePage.html"));
            while (myReader.hasNextLine()) {
                String line = myReader.nextLine();
                res.append(line);
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("index.html file problem");
            e.printStackTrace();
        }
        return res.toString();
	}
}
