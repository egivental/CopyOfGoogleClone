package com.cis555.search.packet;

import java.io.Serializable;
import java.util.List;

public class MasterBroadcastPacket implements Serializable {
    final List<String> names;
    final List<String> addrs;

    public MasterBroadcastPacket(List<String> names, List<String> addrs) {
        this.names = names;
        this.addrs = addrs;
    }

    public List<String> getNames() {
        return names;
    }

    public List<String> getAddrs() {
        return addrs;
    }
}
