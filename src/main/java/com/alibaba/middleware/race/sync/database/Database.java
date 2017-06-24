package com.alibaba.middleware.race.sync.database;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
/**
 * Created by xiefan on 6/24/17.
 */
public class Database {
    private Map<String, Schema> hashMap = new ConcurrentHashMap<>();
    public Schema getSchema(String schema) {
        return hashMap.get(schema);
    }

    public synchronized Schema addSchema(String schema) {
        Schema tmp = hashMap.get(schema);
        if (tmp != null) return tmp;
        tmp = new Schema(schema);
        hashMap.put(schema, tmp);
        return tmp;
    }
}
