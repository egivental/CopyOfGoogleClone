package com.cis555.pagerank.jobs;

import com.cis555.pagerank.PageRank;
import com.cis555.pagerank.mapreduce.PreprocessMapper;
import com.cis555.pagerank.mapreduce.WriterMapper;
import com.cis555.pagerank.mapreduce.WriterReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class WriterJob implements MyJob{

    Job job;

    public WriterJob() throws IOException {
        this.job = Job.getInstance(PageRank.conf, "writer job");
    }

    @Override
    public boolean run(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(WriterJob.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(MultithreadedMapper.class);
        MultithreadedMapper.setMapperClass(job, WriterMapper.class);
        MultithreadedMapper.setNumberOfThreads(job, 10);


        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(WriterReducer.class);

        return job.waitForCompletion(true);

    }

    @Override
    public Job getJob() {
        return this.job;
    }
}
