package com.cis555.search.crawler.info;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

public class URLInfo implements Serializable {

    private static final long serialVersionUID = -2855666167040194246L;
    private String protocol = null;
    private String host = null;
    private int portNo = -1;
    private String filePath = null;
    private boolean isSecure = false;

    public URLInfo(String protocol, String host, int portNo, String filePath) {
        this.protocol = protocol;
        this.host = host;
        this.portNo = portNo;
        this.filePath = filePath;
    }

    public URLInfo(String docURL) {
        //Need to parse docURL
        if (docURL == null || docURL.equals(""))
            return;
        docURL = docURL.trim();
        if (docURL.startsWith("http://")) {
            protocol = "http";
            portNo = 80;
            if (docURL.length() <= 7) {
                return;
            }
            docURL = docURL.substring(7);
        } else if (docURL.startsWith("https://")) {
            isSecure = true;
            protocol = "https";
            portNo = 443;
            if (docURL.length() <= 8) {
                return;
            }
            docURL = docURL.substring(8);
        } else {
            return;
        }
        int i = 0;
        while (i < docURL.length()) {
            char c = docURL.charAt(i);
            if (c == '/' || c == '?') {
                break;
            }
            i++;
        }
        String address = docURL.substring(0, i);
        if (i == docURL.length()) {
            filePath = "/";
        } else if (docURL.charAt(i) == '/') {
            filePath = docURL.substring(i);
        } else {
            filePath = "/" + docURL.substring(i);
        }

        int pos = filePath.indexOf('#');
        if (pos >= 0) {
            filePath = filePath.substring(0, pos);
        }

        if (address.equals("/") || address.equals(""))
            return;
        if (address.indexOf(':') != -1) {
            String[] comp = address.split(":", 2);
            host = comp[0].trim();
            try {
                portNo = Integer.parseInt(comp[1].trim());
            } catch (NumberFormatException nfe) {
                System.err.println("Number format exception");
            }
        } else {
            host = address;
        }
    }

    public URLInfo withNewFilePath(String filePath) {
        return new URLInfo(this.protocol, this.host, this.portNo, filePath);
    }

    public String getHost() {
        return host;
    }

    public String getFilePath() {
        return filePath;
    }

    public final String getFileName() {
        int pos = filePath.indexOf('?');
        return pos != -1 ? filePath = filePath.substring(0, pos) :
                filePath.substring(filePath.lastIndexOf('/') + 1);
    }

    public final String getFileExtension() {
        String temp = this.getFileName();
        int pos = temp.lastIndexOf('.');
        return pos == -1 ? "" : temp.substring(pos + 1);
    }

    public String toString() {
        if ("http".equals(this.protocol) && this.portNo == 80 || "https".equals(this.protocol) && this.portNo == 443) {
            //If http or https
            return this.protocol + "://" + this.host + this.filePath;
        }
        return this.protocol + "://" + this.host + ":" + this.portNo + this.filePath;
    }

    public String toRootURLString() {
        if ("http".equals(this.protocol) && this.portNo == 80 || "https".equals(this.protocol) && this.portNo == 443) {
            return this.protocol + "://" + this.host;
        }
        return this.protocol + "://" + this.host + ":" + this.portNo;
    }

    public HttpURLConnection openConnection() throws IOException {
        if (!isValid()) {
            return null;
        }
        return (HttpURLConnection) (new URL(this.toString())).openConnection();
    }

    public String toUrlId() {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            md.update(this.toString().getBytes());
            String ret = String.format("%1$40s", (new BigInteger(1, md.digest())).toString(16)).replace(' ', '0');
            return ret;
        } catch (NoSuchAlgorithmException e) {
            System.err.println("NoSuchAlgorithmException here");
        }
        return null;
    }

    public boolean isValid() {
        boolean pt = this.portNo > 0 && this.portNo < 65536;
        return pt && this.filePath != null && this.protocol != null && this.host != null && this.filePath != null;
    }

    public final Map<String, String> getQueryParams() {
        Map<String, String> queryParams = new HashMap<String, String>();
        int i = filePath.indexOf('?');
        String[] entries = filePath.substring(i + 1).split("&");
        if (i != -1) {
            for (String entryStr : entries) {
                String[] entry = entryStr.split("=");
                if(entry.length != 2){
                    System.err.println("Error when parsing query parameters");
                    continue;
                }
                queryParams.put(entry[0], entry[1]);
            }
        }
        return queryParams;
    }
}
