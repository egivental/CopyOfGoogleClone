package com.cis555.search.packet;

import java.io.Serializable;
import java.util.List;

public class StatPacket implements Serializable {
    public final List<Long> counts;

    public StatPacket(List<Long> counts) {
        this.counts = counts;
    }


}