package com.cis555.pagerank.configs;

public class PageRankConfig {

    public static final String DUMMY_OUTLINKS = "dummyLinks";
    public static final String NODE_COUNTER = "nodeCounter";
    public static final String DELTA = "delta";
    public static final String PAGERANK_TABLE = "PAGERANK_TABLE";
    public static final String IS_CRAWLED = "isCrawled";
    public static final int SPLIT = 3000;
    public static final int ITERATIONS = 20;





    public static final double DAMPING_FACTOR = 0.85;



    public static enum COUNTERS{
        nodeCounter,
        deltaCounter;
    }
}
