package com.cis555.pagerank.mapreduce;

import com.cis555.pagerank.configs.PageRankConfig;
import com.cis555.pagerank.utils.PageRankUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InitializationMapper extends Mapper<Object, Text, Text, Text> {
    static Logger logger = LoggerFactory.getLogger(InitializationMapper.class);

    @Override
    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        String[] strs = line.split("\t");

        if (strs.length > 1) {
            String fromUrl = strs[0];


            String[] outUrls = strs[1].split("\\|");
            List<String> outlinks = new ArrayList<>();
            if (!PageRankConfig.DUMMY_OUTLINKS.equals(outUrls[0])) {
                outlinks = Arrays.asList(Arrays.copyOfRange(outUrls, 0, outUrls.length - 1));
            }

            double pageRank = 1.0 / Integer.parseInt(context.getConfiguration().get(PageRankConfig.NODE_COUNTER));

            if (!outlinks.isEmpty()) {
                context.write(new Text(fromUrl), new Text(PageRankUtil.getOutLinksString(outlinks) + "|" + pageRank));
            }
        }


//        logger.info("from:{}, out:{}, pagerank:{}", fromUrl, outlinks.toString(), pageRank);


    }
}
