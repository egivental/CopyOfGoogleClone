package com.cis555.pagerank.jobs;

import com.cis555.pagerank.PageRank;
import com.cis555.pagerank.mapreduce.PreprocessMapper;
import com.cis555.pagerank.mapreduce.PreprocessReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PreprocessJob implements MyJob{

    Job job;

    public PreprocessJob() throws IOException {
        this.job = Job.getInstance(PageRank.conf, "Preprocess Job");
    }

    @Override
    public boolean run(String inputPath, String outputPath) throws
            IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(PreprocessJob.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(MultithreadedMapper.class);
        MultithreadedMapper.setMapperClass(job, PreprocessMapper.class);
        MultithreadedMapper.setNumberOfThreads(job, 10);





        // output
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PreprocessReducer.class);
        // TODO: whether add reduce
        return job.waitForCompletion(true);



    }

    @Override
    public Job getJob() {
        return this.job;
    }


}
