package com.alibaba.middleware.race.sync.database;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by xiefan on 6/24/17.
 */
public class Record {
    private long ts = -1;
    private boolean delete = false;
    private Map<Integer, byte[]> data = new HashMap<>();

    public synchronized void update(int columnId, long offset, long ts) {
        if (this.ts > ts) return;
        byte[] buf = data.get(columnId);
        if (buf == null) {
            buf = new byte[16];
            data.put(columnId, buf);
        }
        ByteBuffer tmp = ByteBuffer.wrap(buf);
        long lastupdate = tmp.getLong(0);
        if (lastupdate > ts) return;
        tmp.putLong(0, ts);
        tmp.putLong(8, offset);
    }

    public synchronized void setTs(long ts) {
        if (this.ts > ts) return;
        this.ts = ts;
        delete = false;
    }

    public synchronized void delete(long ts) {
        if (ts < this.ts) return;
        this.ts = ts;
        delete = true;
    }

    public void merge(Record record) {
        for (Map.Entry<Integer, byte[]> entry : record.data.entrySet()) {
            int columnId = entry.getKey();
            ByteBuffer buffer = ByteBuffer.wrap(entry.getValue());
            long ts = buffer.getLong(0);

            byte[] tmp = data.get(columnId);
            if (tmp == null || ByteBuffer.wrap(tmp).getLong(0) < ts) {
                data.put(columnId, buffer.array());
//                delete = false;
            }
        }
    }

    public long get(int columnId) {
        byte[] buf = data.get(columnId);
        return ByteBuffer.wrap(buf).getLong(8);
    }

    public boolean isDelete() {
        return delete;
    }
}
