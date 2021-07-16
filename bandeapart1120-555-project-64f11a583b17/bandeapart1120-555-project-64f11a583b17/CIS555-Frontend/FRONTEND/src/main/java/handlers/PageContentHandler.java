package handlers;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Scanner;

import front.FrontController;
import spark.Request;
import spark.Response;
import spark.Route;
import storage.DocRDSEntity;
import storage.DocS3Block;
import storage.DocS3Entity;
public class PageContentHandler implements Route{

	@Override
	public Object handle(Request request, Response response) throws Exception {
		String urlId = request.queryParams("urlId");
        List<DocRDSEntity> docIndexList = FrontController.getDocRDSController().queryDocByUrlId(urlId);
        if (docIndexList.size() == 0) {
            return "urlId " + urlId + " cannot be found!";
        }

        DocRDSEntity docIndexEntity = docIndexList.get(0);
        DocS3Entity docContentEntity = FrontController.getDocS3Controller().querySingleDoc(docIndexEntity.getDocBlockName(), docIndexEntity.getDocBlockIndex());

        if (docContentEntity.getContentType() == DocS3Block.DOCTYPE_HTML) {
            response.header("Content-Type", "text/html");
        } else if (docContentEntity.getContentType() == DocS3Block.DOCTYPE_PDF) {
            response.header("Content-Type", "application/pdf");
        }

        return docContentEntity.getContentBytes();
	}

}
