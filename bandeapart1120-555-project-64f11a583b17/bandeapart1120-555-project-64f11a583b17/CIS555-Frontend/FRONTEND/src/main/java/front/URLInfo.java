package front;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

public class URLInfo implements Serializable{

    /** (MS1, MS2) Holds information about a URL.
     */

        private static final long serialVersionUID = -2855666167040194246L;

        private String protocol = null;
        private String hostName = null;
        private int portNo = -1;
        private String filePath = null;

        /**
         * Constructor called with raw URL as input - parses URL to obtain host name and file path
         */
        public URLInfo(String docURL){
            if(docURL == null || docURL.equals(""))
                return;
            docURL = docURL.trim();

            // Modified by ZZA
            // -----begin-----
            if (docURL.startsWith("http://")) {
                protocol = "http";
                portNo = 80;
                if (docURL.length() <= 7) {
                    return;
                }
                docURL = docURL.substring(7);
            } else if (docURL.startsWith("https://")) {
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
            while(i < docURL.length()){
                char c = docURL.charAt(i);
                if(c == '/' || c == '?')
                    break;
                i++;
            }
            String address = docURL.substring(0,i);
            if(i == docURL.length()) {
                filePath = "/";
            } else if (docURL.charAt(i) == '/'){
                filePath = docURL.substring(i); //starts with '/'
            } else {
                filePath = "/" + docURL.substring(i);
            }


            // ZZA Added - Remove Anchor in url
            int pos = filePath.indexOf('#');
            if (pos >= 0) {
                filePath = filePath.substring(0, pos);
            }

            if(address.equals("/") || address.equals(""))
                return;
            if(address.indexOf(':') != -1){
                String[] comp = address.split(":",2);
                hostName = comp[0].trim();
                try{
                    portNo = Integer.parseInt(comp[1].trim());
                }catch(NumberFormatException nfe){
//				portNo = 80; // ZZA
                }
            }else{
                hostName = address;
//			portNo = 80; // ZZA
            }

        }

//	public URLInfo(String hostName, String filePath){
//		this.hostName = hostName;
//		this.filePath = filePath;
//		this.portNo = 80;
//	}
//
//	public URLInfo(String hostName,int portNo,String filePath){
//		this.hostName = hostName;
//		this.portNo = portNo;
//		this.filePath = filePath;
//	}

        public URLInfo(String protocol, String hostName, int portNo, String filePath){
            this.protocol = protocol;
            this.hostName = hostName;
            this.portNo = portNo;
            this.filePath = filePath;
        }

        public URLInfo(String hostName, int portNo, String filePath) {
            this("http", hostName, portNo, filePath);
        }

        public URLInfo(String hostName, String filePath) {
            this("http", hostName, 80, filePath);
        }


        public URLInfo withNewFilePath(String filePath) {
            return new URLInfo(getProtocol(), getHostName(), getPortNo(), filePath);
        }

        public URLInfo withRelativePath(String filePath) {

            // Relative path
            if (!filePath.startsWith("/")) {
                filePath = getFilePath().substring(0, getFilePath().lastIndexOf("/") + 1) + filePath;
            }
            return new URLInfo(getProtocol(), getHostName(), getPortNo(), filePath);
        }

        public String getHostName(){
            return hostName;
        }

        public void setHostName(String s){
            hostName = s;
        }

        public int getPortNo(){
            return portNo;
        }

        public void setPortNo(int p){
            portNo = p;
        }

        public String getFilePath(){
            return filePath;
        }

        public void setFilePath(String fp){
            filePath = fp;
        }

        public final String getProtocol() {
            return protocol;
        }

        public final void setProtocol(String protocol) {
            this.protocol = protocol;
        }

        public final String getFileName() {
            int pos = filePath.indexOf('?');
            if (pos != -1) {
                filePath = filePath.substring(0, pos);
            }
            return filePath.substring(filePath.lastIndexOf('/')+1);
        }

        public final String getFileExtension() {
            String fileName = this.getFileName();
            int pos = fileName.lastIndexOf('.');
            if (pos == -1) {
                return "";
            } else {
                return fileName.substring(pos+1);
            }
        }

        public String toString() {
            if ("http".equals(this.protocol) && this.portNo == 80
                    || "https".equals(this.protocol) && this.portNo == 443) {
                return this.protocol + "://" + this.hostName + this.filePath;
            }
            return this.protocol + "://" + this.hostName + ":" + this.portNo + this.filePath;
        }

        public String toRootURLString() {
            if ("http".equals(this.protocol) && this.portNo == 80
                    || "https".equals(this.protocol) && this.portNo == 443) {
                return this.protocol + "://" + this.hostName;
            }
            return this.protocol + "://" + this.hostName + ":" + this.portNo;
        }

        public String toEncodedUrlString() {
            String decodedUrl = this.toString();
            try {
                return URLEncoder.encode(decodedUrl, "UTF-8");
            } catch (UnsupportedEncodingException e) {
            }
            return null;
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
                byte[] sha1Value = md.digest();
                String ret = String.format("%1$40s", (new BigInteger(1, sha1Value)).toString(16)).replace(' ', '0');
                return ret;
            } catch (NoSuchAlgorithmException e) {
            }
            return null;
        }

        public boolean isValid() {
            return this.filePath != null &&
                    this.portNo > 0 && this.portNo < 65536 &&
                    this.protocol != null &&
                    this.hostName != null &&
                    this.filePath != null;
        }

        public final HashMap<String, String> getQueryParams() {
            HashMap<String, String> queryParams = new HashMap<String, String>();

            int i = filePath.indexOf('?');
            if (i != -1) {
                for (String entryStr: filePath.substring(i+1).split("&")) {
                    String[] entry = entryStr.split("=", 2);
                    if (entry.length == 2) {
                        queryParams.put(entry[0], entry[1]);
                    } else {
                        queryParams.put(entry[0], null);
                    }
                }
            }// TODO: need testing

            return queryParams;
        }
}
