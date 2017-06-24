package com.alibaba.middleware.race.sync.database;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by xiefan on 6/24/17.
 */
public class Schema {
    private String name;
    private Map<String, Table> hashMap = new ConcurrentHashMap<>();

    public Schema(String name) {
        this.name = name;
    }

    public Table getTable(String tableName) {
        return hashMap.get(tableName);
    }

    public synchronized Table addTable(String tableName) {
        Table tmp = hashMap.get(tableName);
        if (tmp != null) return tmp;

        tmp = new Table(tableName);
        hashMap.put(tableName, tmp);
        return tmp;
    }
}
