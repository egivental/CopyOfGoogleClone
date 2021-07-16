package com.cis555.search.util;

import com.sleepycat.je.*;

import java.io.File;
import java.math.BigInteger;

/**
 * Implemented based on BDB.
 * Used for local persistence layer.
 */
public class PersistentStorage {

    private Environment environment;
    private Database database;
    private int size;
    private String name;
    private int counter;

    public PersistentStorage(final String name) {
        this("./PersistentStorage/", name, 2000);
    }

    public PersistentStorage(final String dbRoot, final String name, final int size) {
        new File(dbRoot).mkdirs();
        setupEnvironment(dbRoot);
        DatabaseConfig dbConfig = databaseConfig();
        this.database = environment.openDatabase(null, name, dbConfig);
        this.name = name;
        this.size = size;
        this.counter = 0;
    }

    public void sync() {
        database.sync();
        counter = 0;
    }

    private void setupEnvironment(String dbRoot) {
        final EnvironmentConfig dbEnvConfig = new EnvironmentConfig();
        dbEnvConfig.setTransactional(false);
        dbEnvConfig.setAllowCreate(true);
        this.environment = new Environment(new File(dbRoot), dbEnvConfig);
    }

    private DatabaseConfig databaseConfig() {
        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setTransactional(false);
        databaseConfig.setAllowCreate(true);
        databaseConfig.setDeferredWrite(true);
        databaseConfig.setBtreeComparator(new KeyComparator());
        return databaseConfig;
    }

    /**
     * Fetch an element from the persistent storage
     */
    public byte[] poll() {
        DatabaseEntry k = new DatabaseEntry();
        DatabaseEntry v = new DatabaseEntry();
        Cursor cursor = database.openCursor(null, null);
        cursor.getFirst(k, v, LockMode.RMW);
        if (v.getData() == null) {
            return null;
        }
        byte[] res = v.getData();
        cursor.delete();

        if (++counter >= size) {
            database.sync();
            counter = 0;
        }
        cursor.close();
        return res;
    }

    /**
     * Adding new element to the queue
     */
    public synchronized void offer(final byte[] element) {
        DatabaseEntry k = new DatabaseEntry();
        DatabaseEntry v = new DatabaseEntry();
        Cursor cursor = database.openCursor(null, null);
        cursor.getLast(k, v, LockMode.RMW);

        BigInteger prevKeyValue;
        if (k.getData() == null) {
            prevKeyValue = BigInteger.valueOf(-1);
        } else {
            prevKeyValue = new BigInteger(k.getData());
        }
        BigInteger newKeyValue = prevKeyValue.add(BigInteger.ONE);
        final DatabaseEntry nk = new DatabaseEntry(newKeyValue.toByteArray());
        final DatabaseEntry nv = new DatabaseEntry(element);
        database.put(null, nk, nv);
        if (++counter >= size) {
            database.sync();
            counter = 0;
        }
        cursor.close();
    }

    public long size() {
        return database.count();
    }

    public String getName() {
        return name;
    }

    public void close() {
        database.sync();
        database.close();
        environment.close();
    }
}
