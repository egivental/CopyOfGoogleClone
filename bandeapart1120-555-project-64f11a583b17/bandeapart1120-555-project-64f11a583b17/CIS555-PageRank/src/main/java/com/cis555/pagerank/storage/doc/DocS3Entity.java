package com.cis555.pagerank.storage.doc;

import java.math.BigInteger;

/*
 * Data class used by DocS3Controller.
 * */
public class DocS3Entity {
    private byte[] docId; // hash value, 20 bytes
    private int contentLength; // 4 bytes
    private byte[] urlId; // hash value, 20 bytes
    private short contentType; // 2 bytes
    private byte[] contentBytes; // payload
    private int offset = 0;

    public DocS3Entity() {
    }

    public DocS3Entity(byte[] docId, int contentLength, byte[] urlId, short contentType) {
        this.docId = docId;
        this.contentLength = contentLength;
        this.urlId = urlId;
        this.contentType = contentType;
    }

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
        //Java is using SHA-1 hashing. There will be 160 bits and an array with length 40 can hold it
        String res = String.format("%1$40s", new BigInteger(1, data).toString(16));
        return res.replaceAll(" ", "0");
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "docId: " + toHexString(docId) + " length: " + contentLength + " urlId: " + toHexString(urlId) + " type: " + contentType;
    }

    public static void main(String[] args) {
        String test = "test";
        String res = DocS3Entity.toHexString(test.getBytes());
        System.out.println(res);
    }
}
