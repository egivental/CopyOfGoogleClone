package com.cis555.search.crawler;

import com.cis555.search.crawler.info.URLInfo;
import com.cis555.search.crawler.remote.RemoteWorker;
import com.cis555.search.enums.Constants;
import com.cis555.search.storage.DocRDSController;
import com.cis555.search.storage.DocS3Contoller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of crawler
 */
public class Crawler {
    private static Logger logger = LoggerFactory.getLogger(Crawler.class);

    private final int threadCount;
    private final List<CrawlerWorker> threads = new ArrayList<CrawlerWorker>();
    private final TaskDispatcher dispatcher;
    private final InetAddress monitorInetAddr;
    private final RemoteWorker remoteController;
    private DatagramSocket socket;
    private final String crawlerIdentifier;
    public static final String USER_AGENT = "cis455crawler";

    private final DocRDSController docRDSController;
    private final List<DocS3Contoller> docS3Controllers = new ArrayList<>();

    private final List<Long> HTMLCount; // the number of HTML pages scanned for links
    private final List<Long> downloadedBytes; // the amount of data downloaded
    private InetSocketAddress masterAddrObj = null;

    public Crawler(int threadCount, String monitorHostname, List<String> seedUrls, String crawlerIdentifier, String masterAddr) {
        this.threadCount = threadCount;
        this.crawlerIdentifier = crawlerIdentifier;

        InetAddress host = null;
        try (DatagramSocket s = new DatagramSocket()) {
            this.socket = s;
            host = InetAddress.getByName(monitorHostname);
        } catch (SocketException | UnknownHostException e1) {
            logger.error("Error when creating udp socket or solving host");
        }
        this.monitorInetAddr = host;

        // Create Remote Controller
        try {
            InetAddress inetAddr = InetAddress.getByName(masterAddr);
            masterAddrObj = new InetSocketAddress(inetAddr, 21555);
            logger.info("Master address is {}, remote controller is enabled.", inetAddr);
        } catch (UnknownHostException e) {
            logger.warn("Unknown host {}, remote controller is disabled.", masterAddr);
        }
        this.remoteController = new RemoteWorker(masterAddrObj, this);

        this.docRDSController = new DocRDSController();
        assignS3Thread();

        this.HTMLCount = new ArrayList<>(Collections.nCopies(threadCount, 0L));
        this.downloadedBytes = new ArrayList<>(Collections.nCopies(threadCount, 0L));
        this.dispatcher = new TaskDispatcher(this);

        for (int i = 0; i < threadCount; ++i) {
            threads.add(new CrawlerWorker(i, this));
        }
        seedUrls.forEach(e -> {
            this.dispatcher.addTask(new CrawlerTask(new URLInfo(e)));
        });
    }

    private void assignS3Thread() {
        for (int i = 0; i < (int) Math.ceil(threadCount / 10.0); ++i) {
            String tId = "" + Constants.CHAR_SET.value().charAt((i / 16) % 16) + Constants.CHAR_SET.value().charAt(i % 16);
            String temp = String.format("%s-T%s", crawlerIdentifier, tId);
            this.docS3Controllers.add(new DocS3Contoller(temp));
        }
    }

    private void stopNetworkEmit() {
        logger.info("Stop current crawler.");
        this.dispatcher.stop();
        this.threads.forEach(e -> e.stop());
        logger.info("Waiting for threads to be interrupted");
        try {
            Thread.sleep(15 * 1000);
        } catch (InterruptedException e) {
            logger.info("Wait 15 seconds for all the threads to end.");
        }
        this.remoteController.stop();
    }

    private void stopStorage() {
        logger.info("Handling storage closing...");
        this.docS3Controllers.forEach(e -> e.close());
        this.dispatcher.saveProgress();
        logger.info("Storage closed successfully!");
    }

    public synchronized final void start() {
        List<Thread> allThreads = new ArrayList<Thread>();
        this.dispatcher.start();
        for (CrawlerWorker t : this.threads) {
            Thread rawT = new Thread(t);
            allThreads.add(rawT);
            rawT.start();
        }
        this.remoteController.initalize();
        try {
            this.wait();
        } catch (InterruptedException e) {
        }
        stopNetworkEmit();
        stopStorage();
        allThreads.forEach(e -> e.interrupt());
        logger.info("Crawler quits.");
    }

    public final int getThreadCount() {
        return threadCount;
    }

    public final TaskDispatcher getDispatcher() {
        return dispatcher;
    }

    public final InetAddress getMonitorInetAddr() {
        return monitorInetAddr;
    }

    public final DatagramSocket getSocket() {
        return socket;
    }

    public final List<CrawlerWorker> getThreads() {
        return threads;
    }

    public final void incrementHTMLCount(int threadId) {
        HTMLCount.set(threadId, HTMLCount.get(threadId) + 1);
    }

    public final void incrementDownloadedBytes(int threadId, long bytes) {
        downloadedBytes.set(threadId, downloadedBytes.get(threadId) + bytes);
    }

    public final DocRDSController getDocRDSController() {
        return docRDSController;
    }

    public final DocS3Contoller getDocumentContentController(int threadId) {
        return docS3Controllers.get(threadId / 10);
    }

    public final List<Long> getHTMLCounts() {
        return HTMLCount;
    }

    public final String getCrawlerIdentifier() {
        return crawlerIdentifier;
    }

    public final RemoteWorker getRemoteController() {
        return remoteController;
    }

}