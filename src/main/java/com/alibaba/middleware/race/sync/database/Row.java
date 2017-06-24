package com.alibaba.middleware.race.sync.database;

/**
 * Created by xiefan on 6/24/17.
 */
public class Row {
    private long[] vals;
    public Row(int columnNum) {
        vals = new long[columnNum];
    }

    public void update(int columnId, long offset) {
        vals[columnId] = offset;
    }

    public long get(int columnId) {
        return vals[columnId];
    }
}
