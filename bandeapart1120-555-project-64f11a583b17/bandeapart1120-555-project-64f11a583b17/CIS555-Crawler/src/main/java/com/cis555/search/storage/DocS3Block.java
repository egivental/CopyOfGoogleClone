package com.cis555.search.storage;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Chunk stored in S3. Contains raw bytes and need to use the helper methods for parsing
 */
public class DocS3Block {

    private List<DocS3Entity> docList;
    private int totalBytes = 4;
    public static short DOCTYPE_HTML = 0;
    private static final int headerLength = 20 + 20 + 4 + 4 + 2;
    private final String blockName;
    private static final int DEFAULT_BUFFER_SIZE = 8192;
    private static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;

    public DocS3Block(String blockName) {
        this.docList = new ArrayList<>();
        if (blockName.length() > 20) {
            throw new RuntimeException("Invalid block name");
        }
        this.blockName = blockName;
    }

    public synchronized int addDoc(String url, short contentType, byte[] contentBytes, byte[] docId, byte[] urlId) {
        int contentLength = contentBytes.length;
        DocS3Entity docS3Entity = new DocS3Entity(docId, contentLength, urlId, contentType, contentBytes);
        docList.add(docS3Entity);
        totalBytes += headerLength + contentLength;
        return docList.size() - 1;
    }

    public synchronized final int getTotalBytes() {
        return totalBytes;
    }

    /**
     * 1. Write the count of document. 4 bytes
     * 2. Calculate the offset for header information, each entry has a length of headerLength
     */
    public synchronized void saveToFile(String storageDir) {
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(Paths.get(storageDir, blockName).toFile())))) {
            out.writeInt(docList.size());
            int offset = 4 + docList.size() * headerLength;

            for (DocS3Entity docS3Entity : docList) {
                out.write(docS3Entity.getDocId());
                out.writeInt(docS3Entity.getContentLength());
                out.writeShort(docS3Entity.getContentType());
                out.write(docS3Entity.getUrlId());
                out.writeInt(offset);
                offset += docS3Entity.getContentLength();
            }
            for (DocS3Entity docS3Entity : docList) {
                out.write(docS3Entity.getContentBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void loadFromFile(String storageDir) {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(Paths.get(storageDir, blockName).toFile())))) {
            int docCount = in.readInt();
            for (int i = 0; i < docCount; ++i) {
                byte[] docId = readNBytes(20, in);
                int contentLength = in.readInt();
                short contentType = in.readShort();
                byte[] urlId = readNBytes(20, in);
                in.readInt();
                docList.add(new DocS3Entity(docId, contentLength, urlId, contentType, null));
            }
            for (DocS3Entity docS3Entity : docList) {
                byte[] contentBytes = readNBytes(docS3Entity.getContentLength(), in);
                docS3Entity.setContentBytes(contentBytes);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static int loadHeader(byte[] headerByte, DocS3Entity docS3Entity) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(headerByte))) {
            docS3Entity.setDocId(readNBytes(20, in));
            docS3Entity.setContentLength(in.readInt());
            docS3Entity.setContentType(in.readShort());
            docS3Entity.setUrlId(readNBytes(20, in));
            return in.readInt();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public final String getBlockName() {
        return blockName;
    }

    public final static int getHeaderLength() {
        return headerLength;
    }

    public final int getEntityCount() {
        return docList.size();
    }

    public synchronized final void reset() {
        this.docList.clear();
        this.totalBytes = 4;
    }

    private static final byte[] readNBytes(int len, final InputStream in) throws IOException {
        if (len < 0) {
            throw new IllegalArgumentException("len < 0");
        }
        List<byte[]> bufs = null;
        byte[] result = null;
        int total = 0;
        int remaining = len;
        int n;
        do {
            byte[] buf = new byte[Math.min(remaining, DEFAULT_BUFFER_SIZE)];
            int nread = 0;
            while ((n = in.read(buf, nread, Math.min(buf.length - nread, remaining))) > 0) {
                nread += n;
                remaining -= n;
            }
            if (nread > 0) {
                if (MAX_BUFFER_SIZE - total < nread) {
                    throw new OutOfMemoryError("OOM when reading into buffer");
                }
                total += nread;
                if (result == null) {
                    result = buf;
                } else {
                    if (bufs == null) {
                        bufs = new ArrayList<>();
                        bufs.add(result);
                    }
                    bufs.add(buf);
                }
            }
        } while (n >= 0 && remaining > 0);
        if (bufs == null) {
            if (result == null) {
                return new byte[0];
            }
            return result.length == total ? result : Arrays.copyOf(result, total);
        }
        result = new byte[total];
        int offset = 0;
        remaining = total;
        for (byte[] b : bufs) {
            int count = Math.min(b.length, remaining);
            System.arraycopy(b, 0, result, offset, count);
            offset += count;
            remaining -= count;
        }
        return result;
    }
}