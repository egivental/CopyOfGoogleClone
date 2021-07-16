package handlers;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import front.FrontController;
import spark.Request;
import spark.Response;
import spark.Route;
public class CSSHandler implements Route{

	@Override
	public Object handle(Request request, Response response) throws Exception {
		System.out.println("in the CSS handler!!");
        StringBuilder res = new StringBuilder();
        String name = request.params("name");
        try {
            Scanner myReader = new Scanner(new File(FrontController.getPagepath() + "css/" + name));
            while (myReader.hasNextLine()) {
                String line = myReader.nextLine();
                res.append(line);
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("css file problem");
            e.printStackTrace();
        }
        response.type("text/css");
        return res;
	}

}
