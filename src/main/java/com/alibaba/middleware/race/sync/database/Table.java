package com.alibaba.middleware.race.sync.database;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by xiefan on 6/24/17.
 */
public class Table {
    private String name;
    private ConcurrentHashMap<String, Column> hashMap = new ConcurrentHashMap<>();
    private Map<String, Column> fixMap = new HashMap<>();
    private int columnIdCounter = 0;
    private volatile boolean fixed = false;
    private List<Column> columns = new ArrayList<>();
    private volatile Column pk = null;
    private Map<Integer, String> idToName = new HashMap<>();


    public Table(String name) {
        this.name = name;
    }

    public Column getColumn(String columnName) {
        return hashMap.get(columnName);
    }

    public void setPk(String column, boolean isInt) {
        if (pk != null) return;
        pk = hashMap.putIfAbsent(column, new Column(0, isInt));
    }

    public synchronized Column addColumn(String columnName, boolean isInt) {
        Column tmp = hashMap.get(columnName);
        if (tmp != null) return tmp;

        columnIdCounter++;
        tmp = new Column(columnIdCounter, isInt);
        hashMap.put(columnName, tmp);
        return tmp;
    }

    public boolean isFixed() {
        return fixed;
    }

    public Column getPk() {
        return pk;
    }

    // 一旦找到一条insert记录就可以完全固定table的各列
    public synchronized void fix(List<String> columnNames) {
        if (fixed) return;
        fixed = true;
        for (String c : columnNames) {
            Column column = getColumn(c);
            columns.add(column);
            idToName.put(column.getId(), c);
        }
        fixMap.putAll(hashMap);
//        for ()
    }

    public List<Column> getColumns() {
        return columns;
    }

    public String getName(Integer id) {
        return idToName.get(id);
    }

    public Column quickGetColumn(String columnName) {
        return fixMap.get(columnName);
    }
}
