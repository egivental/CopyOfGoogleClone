package com.cis555.search.packet;

import java.io.Serializable;

public class HeartBeatPacket implements Serializable {

    public String status;

    public HeartBeatPacket(String status) {
        this.status = status;
    }

}
