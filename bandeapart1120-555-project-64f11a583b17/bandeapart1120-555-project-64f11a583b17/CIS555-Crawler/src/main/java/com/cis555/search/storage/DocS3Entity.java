package com.cis555.search.storage;

import java.math.BigInteger;

/**
 * Entity class for S3 storage.
 * See document for more information.
 */
public class DocS3Entity {
    private byte[] docId;
    private int contentLength;
    private byte[] urlId;
    private short contentType;
    private byte[] contentBytes;

    public DocS3Entity(byte[] docId, int contentLength, byte[] urlId, short contentType,
                       byte[] contentBytes) {
        this.docId = docId;
        this.contentLength = contentLength;
        this.urlId = urlId;
        this.contentType = contentType;
        this.contentBytes = contentBytes;
    }

    public final byte[] getDocId() {
        return docId;
    }

    public final void setDocId(byte[] docId) {
        this.docId = docId;
    }

    public final int getContentLength() {
        return contentLength;
    }

    public final void setContentLength(int contentLength) {
        this.contentLength = contentLength;
    }

    public final byte[] getUrlId() {
        return urlId;
    }

    public final void setUrlId(byte[] urlId) {
        this.urlId = urlId;
    }

    public final short getContentType() {
        return contentType;
    }

    public final void setContentType(short contentType) {
        this.contentType = contentType;
    }

    public final byte[] getContentBytes() {
        return contentBytes;
    }

    public final void setContentBytes(byte[] contentBytes) {
        this.contentBytes = contentBytes;
    }

    public static String toHexString(byte[] data) {
        return String.format("%1$40s", new BigInteger(1, data).toString(16)).replace(' ', '0');
    }

    @Override
    public String toString() {
        return "docId: " + toHexString(docId) + " length: " + contentLength + " urlId: " + toHexString(urlId) + " type: " + contentType;
    }
}
