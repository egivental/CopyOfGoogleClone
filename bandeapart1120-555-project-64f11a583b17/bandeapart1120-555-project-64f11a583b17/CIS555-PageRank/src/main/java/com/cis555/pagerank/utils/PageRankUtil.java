package com.cis555.pagerank.utils;

import com.cis555.pagerank.configs.PageRankConfig;
import com.cis555.pagerank.storage.doc.DocS3Controller;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PageRankUtil {

    public static void main(String[] args) {
       getAllDocBlockNames();
    }

    static Logger logger = LoggerFactory.getLogger(PageRankUtil.class);

    public static void getAllDocBlockNames() {
        List<String> blockNames = DocS3Controller.listFilesInS3();

        try {
            PrintWriter writer = new PrintWriter("./PRE_INPUT/DocBlocks.txt", "UTF-8");
            for (String blockName : blockNames) {
                writer.println(blockName);
            }

            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<String> getOutLinks(byte[] content, String url) {
        Set<String> outlinks = new HashSet<>();

        Parser parser = Parser.htmlParser();
        Document doc = parser.parseInput(new String(content), url);

        Elements links = doc.select("a[href]");

        for (Element link : links) {
            String newUrl = link.attr("abs:href");
            outlinks.add(newUrl);
        }

        return new ArrayList<>(outlinks);

    }

    public static String getOutLinksString(List<String> outlinks) {
        if (outlinks.isEmpty()) {
            return PageRankConfig.DUMMY_OUTLINKS;
        }

        StringBuilder sb = new StringBuilder();
        for (String s : outlinks) {
            sb.append(s).append("|");
        }

        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public static String toUrlId(String url) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            md.update(url.getBytes());
            byte[] sha1Value = md.digest();
            String ret = String.format("%1$40s", (new BigInteger(1, sha1Value)).toString(16)).replace(' ', '0');
            return ret;
        } catch (NoSuchAlgorithmException e) {
        }
        return null;
    }

}
