package com.cis555.pagerank.mapreduce;

import com.cis555.pagerank.configs.PageRankConfig;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class WriterMapper extends Mapper<Object, Text, IntWritable, Text> {
    static Logger logger = LoggerFactory.getLogger(WriterMapper.class);

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] strs = line.split("\t");
        String url = strs[0];

        String[] outUrls = strs[1].split("\\|");
        String pageRank = outUrls[outUrls.length - 1];


        int randomId = (int) (Math.random() * PageRankConfig.SPLIT);




        context.write(new IntWritable(randomId), new Text(url + "|" + pageRank));

    }
}
