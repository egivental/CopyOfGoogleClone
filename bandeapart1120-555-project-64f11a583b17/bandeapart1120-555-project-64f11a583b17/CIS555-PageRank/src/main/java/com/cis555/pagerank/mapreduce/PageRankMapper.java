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

public class PageRankMapper extends Mapper<Object, Text, Text, Text> {

    static Logger logger = LoggerFactory.getLogger(PageRankMapper.class);

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        String[] strs = line.split("\t");

        String fromUrl = strs[0];
        String[] outUrls = strs[1].split("\\|");
        double pageRank = Double.parseDouble(outUrls[outUrls.length - 1]);

        String oldDelta = context.getConfiguration().get(PageRankConfig.DELTA);
        double oldDeltaValue = (Double.parseDouble(oldDelta)) / 100000;

        double newPageRank = pageRank + (1 - PageRankConfig.DAMPING_FACTOR)
                * (oldDeltaValue / Integer.parseInt(context.getConfiguration().get(PageRankConfig.NODE_COUNTER)));


        List<String> outlinks = new ArrayList<>();
        if (!PageRankConfig.DUMMY_OUTLINKS.equals(outUrls[0])) {
            outlinks = Arrays.asList(Arrays.copyOfRange(outUrls, 0, outUrls.length - 1));
        }

        context.write(new Text(fromUrl), new Text(PageRankUtil.getOutLinksString(outlinks) + "|" + newPageRank));
//        logger.info("from:{}, out:{}, pagerank:{}", fromUrl, outlinks.toString(), newPageRank);

        if (!outlinks.isEmpty()) {
            double p = newPageRank / outlinks.size();
            for (String url : outlinks) {
                context.write(new Text(url), new Text(String.valueOf(p)));
            }
        } else {
            context.getCounter(PageRankConfig.COUNTERS.deltaCounter).increment((long) pageRank * 100000);
        }

    }


}
