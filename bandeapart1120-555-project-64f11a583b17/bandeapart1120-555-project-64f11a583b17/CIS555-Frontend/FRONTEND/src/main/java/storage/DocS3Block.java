package storage;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Chunk stored in S3. Contains raw bytes and need to use the helper methods for parsing
 */
public class DocS3Block {

    private final List<DocS3Entity> documentList = new ArrayList<DocS3Entity>();

    public static short DOCTYPE_HTML = 0;
    public static short DOCTYPE_PDF = 1;                    //Reserve for extra credit(if possible)

    private int totalBytes = 4;                                //The leading 4 bytes is for docCount
    private static final int headerLength = 20 + 20 + 4 + 4 + 2;    //Header size

    private final String blockName;
    private static final int DEFAULT_BUFFER_SIZE = 8192;
    private static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;

    public DocS3Block(String blockName) {
        if (blockName.length() > 20) {
            throw new RuntimeException("Invalid block name");
        }
        this.blockName = blockName;
    }

    public synchronized int addDoc(String url, short contentType, byte[] contentBytes, byte[] docId, byte[] urlId) {
        int contentLength = contentBytes.length;
        DocS3Entity docS3Entity = new DocS3Entity(docId, contentLength, urlId, contentType, contentBytes);
        documentList.add(docS3Entity);
        totalBytes += headerLength + contentLength;
        int blockIndex = documentList.size() - 1;
        return blockIndex;
    }

    public synchronized final int getTotalBytes() {
        return this.totalBytes;
    }

    /**
     * 1. Write the count of document. 4 bytes
     * 2. Calculate the offset for header information, each entry has a length of headerLength
     */
    public synchronized void saveToFile(String storageDir) {
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(Paths.get(storageDir, blockName).toFile())));) {
            out.writeInt(documentList.size());//4 byte for the count of document
            int offset = 4 + documentList.size() * headerLength; // raw data begins at
            for (DocS3Entity docS3Entity : documentList) {
                out.write(docS3Entity.getDocId());            // 20
                out.writeInt(docS3Entity.getContentLength()); // 4
                out.writeShort(docS3Entity.getContentType()); // 2
                out.write(docS3Entity.getUrlId());            // 20
                out.writeInt(offset);                         // 4
                offset += docS3Entity.getContentLength();
            }
            for (DocS3Entity docS3Entity : documentList) {
                out.write(docS3Entity.getContentBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void loadFromFile(String storageDir) {
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(Paths.get(storageDir, blockName).toFile())));) {
            int docCount = in.readInt();    //Doc count within this block
            for (int i = 0; i < docCount; ++i) {
                byte[] docId = readNBytes(20, in);
                int contentLength = in.readInt();
                short contentType = in.readShort();
                byte[] urlId = readNBytes(20, in);
                int offset = in.readInt();
                DocS3Entity docS3Entity = new DocS3Entity(docId, contentLength, urlId, contentType);
                docS3Entity.setOffset(offset);
                documentList.add(docS3Entity);
            }
            for (DocS3Entity docS3Entity : documentList) {
                docS3Entity.setContentBytes(readNBytes(docS3Entity.getContentLength(), in));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static int loadHeader(byte[] headerByte, DocS3Entity docS3Entity) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(headerByte));) {
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


    public final Iterator<DocS3Entity> iterator() {
        return documentList.iterator();
    }

    public final String getBlockName() {
        return blockName;
    }

    public final static int getHeaderLength() {
        return headerLength;
    }

    public final int getEntityCount() {
        return documentList.size();
    }

    public synchronized final void reset() {
        this.documentList.clear();
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
            while ((n = in.read(buf, nread,
                    Math.min(buf.length - nread, remaining))) > 0) {
                nread += n;
                remaining -= n;
            }
            if (nread > 0) {
                if (MAX_BUFFER_SIZE - total < nread) {
                    throw new OutOfMemoryError("Required array size too large");
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
        //read all the bytes first
        if (bufs == null) {
            if (result == null) {
                return new byte[0];
            }
            return result.length == total ?
                    result : Arrays.copyOf(result, total);
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