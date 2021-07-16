package com.cis555.search.util;

import java.io.*;

public class ObjectSerializer {

    public static byte[] toBinary(Serializable obj) {
        byte[] ret = null;
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
                objectOutputStream.writeObject(obj);
                objectOutputStream.flush();
                ret = byteArrayOutputStream.toByteArray();
            }
        } catch (IOException e) {
        }
        return ret;
    }

    public static Object fromBinary(byte[] bytes) {
        Object ret = null;
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes)) {
            try (ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
                ret = objectInputStream.readObject();
            }
        } catch (IOException | ClassNotFoundException e) {
        }
        return ret;
    }
}
