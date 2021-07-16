package com.cis555.search.util;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Comparator;

public class KeyComparator implements Comparator<byte[]>, Serializable {
    @Override
    public int compare(byte[] key1, byte[] key2) {
        BigInteger k1 = new BigInteger(key1);
        BigInteger k2 = new BigInteger(key2);
        return k1.compareTo(k2);
    }

}

