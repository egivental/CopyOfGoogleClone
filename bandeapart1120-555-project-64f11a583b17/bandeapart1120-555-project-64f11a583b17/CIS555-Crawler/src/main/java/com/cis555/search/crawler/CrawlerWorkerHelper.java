package com.cis555.search.crawler;

import com.cis555.search.crawler.info.URLInfo;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.GZIPInputStream;

public class CrawlerWorkerHelper {

    private static Logger logger = LoggerFactory.getLogger(CrawlerWorker.class);

    public static byte[] getRawContent(HttpURLConnection con) throws IOException {
        InputStream input = con.getInputStream();
        if ("gzip".equals(con.getContentEncoding())) {
            input = new GZIPInputStream(input);
        }
        byte[] res = null;
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            try {
                byte[] buffer = new byte[4096];
                int read = 0;
                while ((read = input.read(buffer)) != -1) {
                    //keep writing until reach EOF
                    output.write(buffer, 0, read);
                }
                res = output.toByteArray();
            } catch (SocketTimeoutException e) {
                logger.error("Socket time out");
            } finally {
                try {
                    input.close();
                } catch (Exception e) {
                    logger.error("Failed to close input connection");
                }
            }
        }
        return res;
    }

    /**
     * byte to SHA1
     */
    public static byte[] byte2SHA1(byte[] rawBytes) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            md.update(rawBytes);
            return md.digest();
        } catch (NoSuchAlgorithmException e) {
            logger.error("No such algorithm exception");
        }
        return null;
    }

    /**
     * Sending out heartbeat packet
     */
    public static void sendHeartbeatPacket(URLInfo url, InetAddress monitorInetAddress, DatagramSocket udpSocket) {
        if (monitorInetAddress == null || udpSocket == null) {
            return;
        }
        byte[] data = ("yk;" + url).getBytes();
        try {
            DatagramPacket packet = new DatagramPacket(data, data.length, monitorInetAddress, 10455);
            udpSocket.send(packet);
        } catch (IOException e) {
            logger.error("Error when reporting to the web server");
        }
    }

    public static boolean isURLInterested(URLInfo url) {
        // 1. Skip all invalid urls
        if (!url.isValid()) {
            return false;
        }
        String extension = url.getFileExtension();
        String rawUrl = url.toString();
        // 2. Only look at html files and skip extremely long urls
        if (!(extension.equals("") || extension.equals("htm") || extension.equals("html")) || rawUrl.length() > 1024) {
            return false;
        }
        return true;
    }

    public static boolean isTypeInterested(String type) {
        if (type == null) {
            return false;
        }
        int pos = type.indexOf(';');
        if (pos >= 0) {
            type = type.substring(0, pos);
        }
        return type.startsWith("text/html");
    }

    public static boolean isContentSizeinterested(int docSize) {
        return docSize == -1 || docSize >= 0 && docSize <= 10 * 1024 * 1024;
    }

    public static boolean isContentLanguageInterested(String contentLang) {
        return contentLang == null || contentLang.contains("en");
    }

    public static boolean isDocumentLanguageInterested(String docLang) {
        return docLang.length() == 0 || docLang.length() > 0 && docLang.toLowerCase().contains("en");
    }

    public static boolean isMetaRobotsElementInterested(Element metaRobotsElement) {
        if (metaRobotsElement == null) {
            return true;
        }

        String metaRobots = metaRobotsElement.attr("content").toLowerCase();
        return (!metaRobots.contains("nofollow") &&
                !metaRobots.contains("noindex") &&
                !metaRobots.contains("none"));

    }

    public static boolean isLinkRefInterested(Element link) {
        String ref = link.attr("ref").toLowerCase();
        return !ref.contains("nofollow") &&
                !ref.contains("ugc") &&
                !ref.contains("sponsored");
    }
}
