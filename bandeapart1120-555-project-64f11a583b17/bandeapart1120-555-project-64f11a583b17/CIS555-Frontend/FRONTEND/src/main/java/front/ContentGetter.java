package front;

import storage.*;

import java.util.List;
import java.util.Map;
import java.util.Queue;

public class ContentGetter extends Thread{

    Queue<String> docids;
    Map<String, Integer> pos;
    String[] contents;
    DocRDSController docRDSController;
    DocS3Controller docS3Controller;

    //thread function to get url contents
    public ContentGetter(Queue<String> docids, Map<String, Integer> pos, String[] contents, DocRDSController docRDSController, DocS3Controller docS3Controller) {
        this.docids = docids;
        this.pos = pos;
        this.contents = contents;
        this.docRDSController = docRDSController;
        this.docS3Controller = docS3Controller;
    }

    public void run() {
        while (true) {
            String docid = null;
            synchronized (this.docids) {
                docid = this.docids.poll();
            }
            if (docid == null) {
                break;
            }
            List<DocRDSEntity> docIndexList = null;
            try {
//                docIndexList = docRDSController.queryDocByUrlId(docid);
                docIndexList = docRDSController.queryDocByDocId(docid);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            if (docIndexList.size() == 0) {
                System.out.println("docId " + docid + " cannot be found.");
                continue;
            }
            DocRDSEntity docRDSEntity = docIndexList.get(0);
            DocS3Entity docS3Entity = docS3Controller.querySingleDoc(docRDSEntity.getDocBlockName(), docRDSEntity.getDocBlockIndex());
            String content = new String(docS3Entity.getContentBytes());
//            synchronized (this.contents) {
                this.contents[this.pos.get(docid)] = content;
//            }
        }
    }
}
