package storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class CallingExamples {

	public static void main(String[] args) {
		try {
			iterateOverBlocksFromS3();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void iterateOverBlocksFromS3() throws FileNotFoundException, IOException {
		String storage = "/home/cis455/Documents/555-proj/storage/inputs";
		int i = 0;
		List<String> blockNames = DocS3Controller.listFilesInS3();
		try (DocS3Controller docS3Controller = new DocS3Controller()) {
			for (String blockName: blockNames) {
				System.out.println(blockNames.size());
				DocS3Block docS3Block = docS3Controller.getEntireDocBlock(blockName);
				Iterator<DocS3Entity> it = docS3Block.iterator();
				//System.out.println(docS3Block.getEntityCount());
				while (it.hasNext()) {
					DocS3Entity entity = it.next();
					System.out.println(DocS3Entity.toHexString(entity.getDocId()));
					File f = new File(storage + "/" + DocS3Entity.toHexString(entity.getDocId()));
					f.createNewFile();
					try (FileOutputStream outputStream = new FileOutputStream(f)) {
					    outputStream.write(entity.getContentBytes());
					}
				}
				if (i++ > 1) {
					break;
				}
			}
		}
	}

}
