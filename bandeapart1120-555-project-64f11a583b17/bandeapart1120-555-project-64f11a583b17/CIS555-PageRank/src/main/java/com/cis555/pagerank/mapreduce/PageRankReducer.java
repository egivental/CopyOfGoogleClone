package com.cis555.pagerank.mapreduce;

import com.cis555.pagerank.configs.PageRankConfig;
import com.cis555.pagerank.utils.PageRankUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
    static Logger logger = LoggerFactory.getLogger(PageRankReducer.class);

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String url = key.toString();
        List<String> outLinks = new ArrayList<>();


        double sum = 0;
        boolean isCrawled = false;

        for (Text value : values) {
            String str = value.toString();
            if (str.contains("|")) {
                String[] outUrls = str.split("\\|");

                if (!PageRankConfig.DUMMY_OUTLINKS.equals(outUrls[0])) {
                    isCrawled = true;
                    outLinks = Arrays.asList(Arrays.copyOfRange(outUrls, 0, outUrls.length - 1));
                }

            } else {
                sum += Double.parseDouble(str);
            }
        }

        int n = Integer.parseInt(context.getConfiguration().get(PageRankConfig.NODE_COUNTER));
        double damping = PageRankConfig.DAMPING_FACTOR;

        double newPageRank = (damping / n) + (1 - damping) * sum;


        if (isCrawled) {
            context.write(new Text(url), new Text(PageRankUtil.getOutLinksString(outLinks) + "|" + newPageRank));
        }
//

    }
}
