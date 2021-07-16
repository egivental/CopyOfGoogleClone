package com.cis555.search.crawler;

import com.cis555.search.util.LRUHashMap;
import com.cis555.search.crawler.info.RobotsTxt;
import com.cis555.search.crawler.info.URLInfo;
import com.cis555.search.storage.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;

import static com.cis555.search.crawler.CrawlerWorkerHelper.*;

class CrawlerWorker implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(CrawlerWorker.class);
    private final int workerId;
    private final BlockingQueue<CrawlerTask> queue = new DelayQueue<CrawlerTask>();
    private final LRUHashMap<String, Long> lastVisitedMap = new LRUHashMap<String, Long>(256);
    private final LRUHashMap<String, RobotsTxt> robotsTxtMap = new LRUHashMap<String, RobotsTxt>(256);

    private boolean isContinue = true;

    private final Crawler crawler;
    private final TaskDispatcher dispatcher;
    private final DocRDSController docRDSController;
    private final DocS3Contoller docS3Contoller;

    public CrawlerWorker(int workerId, Crawler crawler) {
        this.workerId = workerId;
        this.crawler = crawler;
        this.dispatcher = crawler.getDispatcher();
        this.docRDSController = crawler.getDocRDSController();
        this.docS3Contoller = crawler.getDocumentContentController(workerId);
    }

    private HttpURLConnection send(CrawlerTask task, String method, boolean isToStore) {
        HttpURLConnection con = null;
        try {
            URLInfo url = task.getUrl();
            sendHeartbeatPacket(url, crawler.getMonitorInetAddr(), crawler.getSocket());

            // 1. Get connection
            con = connect(url, method);
            int responseCode = con.getResponseCode();
            if (con == null) {
                return null;
            }
            if (responseCode < 0) {
                closeInConnected(con);
                return null;
            }

            // 2. Redirection, handle all 300-level code
            while (responseCode / 100 == 3) {
                String redirection = con.getHeaderField("Location");
                if (redirection == null) {
                    closeInConnected(con);
                    return null;
                }
                task = task.newRedirectedTask(redirection);
                URLInfo redirectedURL = task.getUrl();

                if (task.getCounter() > 5) {
                    closeInConnected(con);
                    return null;
                }

                if (!isToStore) {
                    if (!redirection.endsWith("robots.txt")) {
                        closeInConnected(con);
                        return null;
                    }
                    con = connect(task.getUrl(), method);
                    if (con == null || con.getResponseCode() < 0) {
                        closeInConnected(con);
                        return null;
                    }
                    continue;
                } else {
                    if (isURLInterested(redirectedURL)) {
                        try {
                            if (docRDSController.isUrlSeen(redirectedURL.toUrlId())) {
                                break;
                            }
                        } catch (SQLException e) {
                            logger.error("SQL Exception");
                        }
                        dispatcher.addTask(task);
                    }
                }
                closeInConnected(con);
                return null;
            }
            // 3. If not redirect, just send the task
            return con;
        } catch (IOException e) {
//            logger.error("Catch IOException");
            if (con != null) {
                con.disconnect();
            }
        }
        return null;
    }

    private void closeInConnected(HttpURLConnection con) {
        if (con != null) {
            con.disconnect();
        }
    }

    private RobotsTxt processRobotsTxt(HttpURLConnection con, URLInfo robotsTxtURL) {
        RobotsTxt robotsTxt = null;
        try {
            if (con != null && con.getResponseCode() == HttpURLConnection.HTTP_OK) {
                robotsTxt = new RobotsTxt(new String(getRawContent(con)), Crawler.USER_AGENT);
            }
        } catch (IOException e) {
            logger.error("IO exception");
        }

        if (robotsTxt == null || robotsTxt.isUninitialized()) {
            robotsTxt = RobotsTxt.dummy;
        }
        closeInConnected(con);
        return robotsTxt;
    }

    private boolean addDocument(URLInfo url, long lastModifiedTs, short contentType, byte[] contentBytes) {
        try {
            boolean isSeen = docRDSController.isUrlSeen(url.toUrlId());
            if (isSeen) {
                return true;
            }

            byte[] docId = byte2SHA1(contentBytes);
            String docIdStr = DocS3Entity.toHexString(docId);
            List<DocRDSEntity> documentIndexList = docRDSController.queryDocByDocId(docIdStr);

            String urlStr = url.toString();
            byte[] urlId = byte2SHA1(urlStr.getBytes());
            String urlIdStr = DocS3Entity.toHexString(urlId);

            int docBlockIndex;
            String docBlockName;
            if (documentIndexList.size() == 0) {
                List<Object> result = docS3Contoller.addDoc(urlStr, contentType, contentBytes, docId, urlId);
                docBlockIndex = (int) result.get(0);
                docBlockName = (String) result.get(1);
            } else {
                DocRDSEntity docRDSEntity = documentIndexList.get(0);
                docBlockIndex = docRDSEntity.getDocBlockIndex();
                docBlockName = docRDSEntity.getDocBlockName();
            }

            DocRDSEntity docRDSEntity = new DocRDSEntity(urlStr, urlIdStr, docIdStr, docBlockName, docBlockIndex, System.currentTimeMillis(), lastModifiedTs);
            isSeen = !docRDSController.addDoc(docRDSEntity);
            return isSeen;
        } catch (SQLException e) {
            logger.error("Failed when adding document.", e);
        }
        return false;
    }

    @Override
    public void run() {
        try {
            while (isContinue) {
                try {
                    CrawlerTask task = queue.take();
                    final URLInfo url = task.getUrl();
                    RobotsTxt robotsTxt = robotsTxtMap.get(url.getHost());

                    if (robotsTxt == null) {
                        URLInfo robotsTxtURL = url.withNewFilePath("/robots.txt");
                        CrawlerTask newTask = new CrawlerTask(url.withNewFilePath("/robots.txt"), 0L);
                        HttpURLConnection con = send(newTask, "GET", false);
                        robotsTxt = processRobotsTxt(con, robotsTxtURL);
                        robotsTxtMap.put(url.getHost(), robotsTxt);
                    }
                    int delay = robotsTxt.getDelay(Crawler.USER_AGENT, 1) * 500;
                    //Ignore all the sites with a crawl delay more than 1 minute
                    if (delay >= 60 * 1000) {
                        continue;
                    }

                    long curr = System.currentTimeMillis();
                    long lastTimestamp = lastVisitedMap.getOrDefault(url.getHost(), 0L);
                    if (curr - lastTimestamp < delay) {
                        task.setDelay(lastTimestamp + delay);
                        dispatcher.addTask(task);
                        continue;
                    }

                    if (!robotsTxt.isURLAllowed(url, Crawler.USER_AGENT)) {
                        continue;
                    }

                    HttpURLConnection con;
                    /**
                     * Sending HEAD request
                     */
                    con = send(task, "HEAD", true);
                    if (con == null) {
                        continue;
                    }
                    int responseCode = con.getResponseCode();
                    String language = con.getHeaderField("content-language");
                    int size = con.getContentLength();
                    lastVisitedMap.put(url.getHost(), System.currentTimeMillis());
                    if (responseCode == HttpURLConnection.HTTP_OK) {
                        // Filter by type, language and size
                        if (!isTypeInterested(con.getContentType()) ||
                                !isContentLanguageInterested(language) ||
                                !isContentSizeinterested(size)) {
                            con.disconnect();
                            continue;
                        }
                    } else {
                        con.disconnect();
                        continue;
                    }
                    closeInConnected(con);
                    /**
                     * Sending GET request
                     */
                    con = send(task, "GET", true);
                    if (con != null && con.getResponseCode() == 200) {
                        byte[] rawContent = getRawContent(con);
                        // Skip if downloading failed
                        if (rawContent == null) {
                            con.disconnect();
                            continue;
                        }

                        // Use Jsoup to parse the content and url
                        String urlRaw = url.toString();
                        Document doc = Jsoup.parse(new String(rawContent), urlRaw);

                        // Only process html files
                        short documentDBType = DocS3Block.DOCTYPE_HTML;

                        // Filter out non-English page
                        language = doc.select("html").first().attr("lang");
                        if (!isDocumentLanguageInterested(language)) {
                            con.disconnect();
                            continue;
                        }

                        // skip all the meta[name=robots]
                        Element metaRobotsElement = doc.select("meta[name=robots]").first();
                        if (!isMetaRobotsElementInterested(metaRobotsElement)) {
                            con.disconnect();
                            continue;
                        }
                        crawler.incrementHTMLCount(getWorkerId());
                        crawler.incrementDownloadedBytes(getWorkerId(), rawContent.length);

                        // Synchronize to DB
                        boolean isURLExistInDB = this.addDocument(url, con.getLastModified(), documentDBType, rawContent);
                        if (isURLExistInDB) {
                            con.disconnect();
                            continue;
                        }

                        // Extract URLs for HTML documents
                        Set<String> urlsInPage = new LinkedHashSet<>();
                        for (Element link : doc.select("a[href]")) {
                            if (!isLinkRefInterested(link)) {
                                continue;
                            }
                            // Get Absolute Full URL
                            URLInfo newUrl = new URLInfo(link.absUrl("href"));
                            String newUrlStr = newUrl.toString();

                            // Skip self page (caused by anchor)
                            if (urlRaw.equals(newUrlStr)) {
                                continue;
                            }

                            // URL Filter - interested in HTML
                            if (isURLInterested(newUrl)) {
                                if (!urlsInPage.add(newUrlStr) || docRDSController.isUrlSeen(newUrl.toUrlId())) {
                                    continue;
                                }
                                dispatcher.addTask(new CrawlerTask(newUrl));
                            }
                        }
                        con.disconnect();
                    }
                } catch (IOException e) {
                    logger.error("Run loop", e);
                } catch (InterruptedException e) {
                    throw e;
                } catch (Exception e) {
                    logger.error("Run loop", e);
                }
            }
        } catch (InterruptedException e) {

        }

    }

    public final int getWorkerId() {
        return workerId;
    }

    public final void putIntoQueue(CrawlerTask task) throws InterruptedException {
        this.queue.put(task);
    }

    public final void stop() {
        this.isContinue = false;
    }

    public final int getQueueSize() {
        return this.queue.size();
    }

    public final int getServerCount() {
        return this.robotsTxtMap.size();
    }

    public final Iterator<CrawlerTask> getTaskIterator() {
        return queue.iterator();
    }

    private HttpURLConnection connect(URLInfo url, String method) throws IOException {
        try {
            HttpURLConnection con = url.openConnection();
            con.setConnectTimeout(5000);
            con.setReadTimeout(5000);
            con.setRequestMethod(method);
            con.setInstanceFollowRedirects(false);
            con.addRequestProperty("Accept-Language", "en-US,en;q=0.8");
            con.addRequestProperty("Accept-Encoding", "gzip");
            con.addRequestProperty("User-Agent", Crawler.USER_AGENT);
            return con;
        } catch (ProtocolException | NullPointerException e) {
//            logger.error("Error when creating connection.");
        }
        return null;
    }

}