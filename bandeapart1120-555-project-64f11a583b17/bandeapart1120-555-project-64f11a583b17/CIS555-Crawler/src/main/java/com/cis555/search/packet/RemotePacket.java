package com.cis555.search.packet;

import java.io.Serializable;

public class RemotePacket implements Serializable {
    public String source;
    public Object payload;
    public String etag;

    public RemotePacket(String source, Serializable payload, String etag) {
        this.source = source;
        this.payload = payload;
        this.etag = etag;
    }

}