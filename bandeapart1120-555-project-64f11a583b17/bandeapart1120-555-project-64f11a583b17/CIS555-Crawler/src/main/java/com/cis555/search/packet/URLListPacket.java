package com.cis555.search.packet;

import java.io.Serializable;

public class URLListPacket implements Serializable {

    public final String[] urls;

    public URLListPacket(String[] urls) {
        this.urls = urls;
    }

}
