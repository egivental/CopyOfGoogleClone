package storage;

import java.util.Iterator;
import java.util.List;

public class CallingExamples {

	public static void main(String[] args) {
		iterateOverBlocksFromS3();
	}
	
	public static void iterateOverBlocksFromS3() {
		int i = 0;
		List<String> blockNames = DocS3Controller.listFilesInS3();
		try (DocS3Controller docS3Controller = new DocS3Controller()) {
			for (String blockName: blockNames) {
				DocS3Block docS3Block = docS3Controller.getEntireDocBlock(blockName);
				Iterator<DocS3Entity> it = docS3Block.iterator();
				System.out.println(docS3Block.getEntityCount());
				while (it.hasNext()) {
					DocS3Entity entity = it.next();
					System.out.println(DocS3Entity.toHexString(entity.getUrlId()));
				}
				if (i++ > 1) {
					break;
				}
			}
		}
	}

}
