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
import spark.Request;
import spark.Response;
import spark.Route;
public class IMGHandler implements Route{

	@Override
	public Object handle(Request request, Response response) throws Exception {
		System.out.println("IMG get!!");
        String name = request.params("name");
        try {
        	File file = new File(FrontController.getPagepath() + "img/" + name);
            Path source = Paths.get(FrontController.getPagepath() + "img/" + name);
            response.header("Content-Type", Files.probeContentType(source));
            response.raw().setContentType(Files.probeContentType(source));
            try (OutputStream out = response.raw().getOutputStream()) {
                ImageIO.write(ImageIO.read(file), "png", out);
            }
        } catch (FileNotFoundException e) {
            System.out.println("image file problem");
            e.printStackTrace();
        }
        return "";
	}

}
