package com.alibaba.middleware.race.sync;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.middleware.race.sync.model.Record;

/**
 * @author wangkai
 */
public class Context {

    private ReadChannel channel;
    private long endId;
    private RecordLogReceiver receiver;
    private Map<Long, Record> records = new HashMap<>();
    private long startId;
    private String tableSchema;

    public Context(ReadChannel channel, long endId, RecordLogReceiver receiver, long startId,
                   String tableSchema) {
        this.channel = channel;
        this.endId = endId;
        this.receiver = receiver;
        this.startId = startId;
        this.tableSchema = tableSchema;
    }

    public ReadChannel getChannel() {
        return channel;
    }


    public RecordLogReceiver getReceiver() {
        return receiver;
    }

    public Map<Long, Record> getRecords() {
        return records;
    }


    public String getTableSchema() {
        return tableSchema;
    }

    public void initialize() {

    }

    public void setChannel(ReadChannel channel) {
        this.channel = channel;
    }


    public void setReceiver(RecordLogReceiver receiver) {
        this.receiver = receiver;
    }

    public void setRecords(Map<Long, Record> records) {
        this.records = records;
    }


    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public long getEndId() {
        return endId;
    }

    public void setEndId(long endId) {
        this.endId = endId;
    }

    public long getStartId() {
        return startId;
    }

    public void setStartId(long startId) {
        this.startId = startId;
    }
}
