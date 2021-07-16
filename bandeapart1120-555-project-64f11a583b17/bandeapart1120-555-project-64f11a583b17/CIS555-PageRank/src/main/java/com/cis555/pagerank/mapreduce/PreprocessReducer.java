package com.cis555.pagerank.mapreduce;

import com.cis555.pagerank.configs.PageRankConfig;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PreprocessReducer extends Reducer<Text, Text, Text, Text> {
    static Logger logger = LoggerFactory.getLogger(PreprocessReducer.class);

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        for (Text value : values) {
            if (!PageRankConfig.DUMMY_OUTLINKS.equals(value.toString())) {
                sb.append(value.toString()).append("|");
            }
        }

        if (sb.length() == 0) {
            sb.append(PageRankConfig.DUMMY_OUTLINKS).append("|");
        }

        sb.append("0");
        context.write(key, new Text(sb.toString()));
//        logger.info("Reduce key: {}, adlist: {}", key.toString(), sb.toString());
        context.getCounter(PageRankConfig.COUNTERS.nodeCounter).increment(1);

    }
}
