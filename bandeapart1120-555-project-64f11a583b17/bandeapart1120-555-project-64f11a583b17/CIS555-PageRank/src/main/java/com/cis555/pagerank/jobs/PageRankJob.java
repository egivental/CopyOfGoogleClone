package com.cis555.pagerank.jobs;

import com.cis555.pagerank.PageRank;
import com.cis555.pagerank.mapreduce.PageRankMapper;
import com.cis555.pagerank.mapreduce.PageRankReducer;
import com.cis555.pagerank.mapreduce.PreprocessMapper;
import com.cis555.pagerank.mapreduce.PreprocessReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class PageRankJob implements MyJob{

    Job job;

    public PageRankJob() throws IOException {
        this.job = Job.getInstance(PageRank.conf, "pagerank job");
    }

    @Override
    public boolean run(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(PageRankJob.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
//        job.setMapperClass(PageRankMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(MultithreadedMapper.class);
        MultithreadedMapper.setMapperClass(job, PageRankMapper.class);
        MultithreadedMapper.setNumberOfThreads(job, 10);


        // output
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankReducer.class);
        // TODO: whether add reduce
        return job.waitForCompletion(true);

    }

    @Override
    public Job getJob() {
        return this.job;
    }
}
