package com.cis555.search.crawler;

import java.util.ArrayList;
import java.util.List;


public class CrawlerMain {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            //Args0=threadCount	Args1=nodeName Args2=Master
            //100 N01 localhost
            System.err.println("Error input");
            System.exit(1);
        }
        int count = Integer.parseInt(args[0]);
        String node = args[1];
        String master = null;

        if (args.length > 2) {
            master = args[2];
        }

        String remoteHost = "3.84.146.60";
        List<String> seeds = new ArrayList<>();
        seeds.add("https://sites.google.com/seas.upenn.edu/cis-519-applied-ml/");
        seeds.add("https://15445.courses.cs.cmu.edu/fall2021/");
        seeds.add("https://en.wikipedia.org/wiki/Singapore");
        seeds.add("https://en.wikipedia.org/wiki/Nuclear");
        seeds.add("https://en.wikipedia.org/wiki/COVID-19");
        seeds.add("https://en.wikipedia.org/wiki/Lehigh_University");

        Crawler crawler = new Crawler(count, remoteHost, seeds, node, master);
        crawler.start();
    }
}
