package com.alibaba.middleware.race.sync.model.result;

import com.alibaba.middleware.race.sync.map.ArrayHashMap2;

/**
 * Created by xiefan on 6/24/17.
 */
public class CalculateResult {

    private ArrayHashMap2 recordMap;

    private boolean end = false;

    private int threadId;

    public CalculateResult(ArrayHashMap2 recordMap, int threadId) {
        this.recordMap = recordMap;
        this.threadId = threadId;
    }

    private CalculateResult() {
        end = true;
    }

    public boolean isEnd() {
        return end;
    }


    public static final CalculateResult END_TASK = new CalculateResult();

    public ArrayHashMap2 getRecordMap() {
        return recordMap;
    }

    public void setRecordMap(ArrayHashMap2 recordMap) {
        this.recordMap = recordMap;
    }

    public int getThreadId() {
        return threadId;
    }

    public void setThreadId(int threadId) {
        this.threadId = threadId;
    }
}
