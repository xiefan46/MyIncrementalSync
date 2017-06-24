package com.alibaba.middleware.race.sync.database;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by xiefan on 6/24/17.
 */
public class TableInfo {
    private ConcurrentHashMap<String, Column> map = new ConcurrentHashMap<>();
    private int columnIdCounter = 0;
    private List<Column> columnList = new ArrayList<>();
    private volatile boolean fixed = false;

    public Column get(String column) {
        return map.get(column);
    }

    public synchronized Column add(String column, boolean isInt) {
        Column ret = map.get(column);
        if (ret != null) return ret;
        columnIdCounter++;
        ret = new Column(columnIdCounter, isInt);
        map.put(column, ret);
        return ret;
    }

    public boolean isFixed() {
        return fixed;
    }

//    public synchronized void fix(List<String> columns) {
//        if (fixed) return;
//        fixed = true;
//        for (String column : columns) {
//            columnList.add(map.get(column));
//        }
//    }

    public synchronized void fix(List<Column> columns) {
        if (fixed) return;
        fixed = true;
        columnList.addAll(columns);
    }

    public List<Column> getColumnList() {
        return columnList;
    }
}
