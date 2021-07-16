package com.cis555.search.crawler.remote;

import com.cis555.search.packet.StatPacket;

import java.net.SocketAddress;

public class WorkerInfo {
    public final String name;
    public final SocketAddress addr;
    public long lastTimestamp;
    public String status;
    public StatPacket statistics;

    public WorkerInfo(String name, SocketAddress addr) {
        this.name = name;
        this.addr = addr;
    }
}
