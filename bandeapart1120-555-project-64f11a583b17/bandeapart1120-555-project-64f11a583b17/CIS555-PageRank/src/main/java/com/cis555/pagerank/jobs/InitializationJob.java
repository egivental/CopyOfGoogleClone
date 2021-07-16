package com.cis555.pagerank.jobs;

import com.cis555.pagerank.PageRank;
import com.cis555.pagerank.mapreduce.InitializationMapper;
import com.cis555.pagerank.mapreduce.PreprocessMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class InitializationJob implements MyJob {

    Job job;

    public InitializationJob() throws IOException {
        this.job = Job.getInstance(PageRank.conf, "initialization job");
    }

    @Override
    public boolean run(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(InitializationJob.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        job.setInputFormatClass(TextInputFormat.class);
//        job.setMapperClass(InitializationMapper.class);
        job.setMapperClass(MultithreadedMapper.class);
        MultithreadedMapper.setMapperClass(job, InitializationMapper.class);
        MultithreadedMapper.setNumberOfThreads(job, 10);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true);
    }

    @Override
    public Job getJob() {
        return this.job;
    }
}
