package com.cis555.pagerank.mapreduce;

import com.cis555.pagerank.configs.PageRankConfig;
import com.cis555.pagerank.utils.PageRankUtil;
import com.cis555.pagerank.storage.doc.DocRDSController;
import com.cis555.pagerank.storage.doc.DocS3Block;
import com.cis555.pagerank.storage.doc.DocS3Controller;
import com.cis555.pagerank.storage.doc.DocS3Entity;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PreprocessMapper extends Mapper<Object, Text, Text, Text> {

    static Logger logger = LoggerFactory.getLogger(PreprocessMapper.class);

    DocRDSController docRDSController = new DocRDSController();
    DocS3Controller docS3Controller = new DocS3Controller();

    @Override
    public void map (Object key, Text value, Context context) throws IOException,InterruptedException {

        String blockName = value.toString();
        logger.info("Process block: {}", blockName);

        try {

                DocS3Block docS3Block = docS3Controller.getEntireDocBlock(blockName);
                Iterator<DocS3Entity> it = docS3Block.iterator();

                System.out.println(docS3Block.getEntityCount());

                List<String> fromIds = new ArrayList<>();

                while (it.hasNext()) {
                    DocS3Entity entity = it.next();
                    // content is html
                    if (entity.getContentType() == 0) {
                        String id = DocS3Entity.toHexString(entity.getUrlId());
                        fromIds.add(id);
                    }
                }

                it = docS3Block.iterator();

                 List<String> fromUrls = docRDSController.queryUrlsByUrlIds(fromIds);

                 int i = 0;

                 while (it.hasNext()) {
                     DocS3Entity entity = it.next();

                     if (entity.getContentType() == 0) {
                         String fromUrl = fromUrls.get(i);
                         List<String> ourUrls = PageRankUtil.getOutLinks(entity.getContentBytes(), fromUrl);
                         String outlinks = PageRankUtil.getOutLinksString(ourUrls);
                         context.write(new Text(fromUrl), new Text(outlinks));
                         i++;
                     }
                 }


            } catch (Exception exception) {
                logger.error(exception.toString());
                exception.printStackTrace();
            }

        }



}
