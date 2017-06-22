package com.alibaba.middleware.race.sync.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiefan on 6/23/17.
 */
public class RecordLogs {

    private int id;

    private List<RecordLog> logs = new ArrayList<>();

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public List<RecordLog> getLogs() {
        return logs;
    }

    public void setLogs(List<RecordLog> logs) {
        this.logs = logs;
    }
}
