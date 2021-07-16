package com.cis555.pagerank;

import com.cis555.pagerank.configs.PageRankConfig;
import com.cis555.pagerank.jobs.*;
import com.cis555.pagerank.utils.PageRankUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

public class PageRank {

    public static final String PRE_INPUT = "./PRE_INPUT";
    public static final String PR_INPUT = "./PR_INPUT/";
    public static final String PR_OUTPUT = "./PR_OUTPUT";

    public static final Configuration conf = new Configuration();

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        deleteDirectory(new File(PR_INPUT));
        deleteDirectory(new File(PR_OUTPUT));
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        double previousDelta = 0.00;


        // get all block names from document S3
        PageRankUtil.getAllDocBlockNames();


        // preprocess document from S3 to get link - outlinks
        MyJob preprocessJob = new PreprocessJob();
         boolean isComplete = preprocessJob.run(PRE_INPUT, PR_INPUT + "prepare");

        conf.set(PageRankConfig.NODE_COUNTER, String.valueOf(preprocessJob.getJob().getCounters()
                .findCounter(PageRankConfig.COUNTERS.nodeCounter).getValue()));

        if (!isComplete) {
            System.exit(1);
        }

        System.out.println("COUNTER: " + preprocessJob.getJob().getCounters()
                .findCounter(PageRankConfig.COUNTERS.nodeCounter).getValue());


        // initialize pagerank for each link to 1/n
        MyJob initializationJob = new InitializationJob();

        isComplete = initializationJob.run(PR_INPUT + "prepare", PR_INPUT + "0");

        if (!isComplete) {
            System.exit(1);
        }

        String inputPath = PR_INPUT + "0";

        // pagerank iterations job
        for (int i = 0; i < PageRankConfig.ITERATIONS; i++) {
            conf.set(PageRankConfig.DELTA, String.valueOf(previousDelta));
            MyJob pagerankJob = new PageRankJob();
            String outputPath = PR_INPUT + (i + 1);
            isComplete = pagerankJob.run(inputPath, outputPath);

            if (!isComplete) {
                System.exit(1);
            }

            previousDelta = (double) pagerankJob.getJob().getCounters().findCounter(PageRankConfig.COUNTERS.deltaCounter).getValue();
            pagerankJob.getJob().getCounters().findCounter(PageRankConfig.COUNTERS.deltaCounter).setValue(0);
            inputPath = outputPath;
        }


        MyJob writerJob = new WriterJob();
        isComplete = writerJob.run(inputPath, PR_OUTPUT);

        if (!isComplete) {
            System.exit(1);
        }



        System.out.println("PageRank Finished!");




    }

    public static void deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        directoryToBeDeleted.delete();
    }
}
