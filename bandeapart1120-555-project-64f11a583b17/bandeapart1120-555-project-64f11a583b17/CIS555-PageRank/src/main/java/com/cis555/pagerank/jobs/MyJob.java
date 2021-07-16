package com.cis555.pagerank.jobs;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public interface MyJob {
    /**
     * Runs the job.
     * @param inputPath The input path for the job.
     * @param outputPath The output path for the job.
     * @throws IOException
     */
    public boolean run(String inputPath, String outputPath)
            throws IOException, ClassNotFoundException, InterruptedException;

    public Job getJob();
}
