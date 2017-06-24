package com.alibaba.middleware.race.sync.model;

/**
 * Created by xiefan on 6/24/17.
 */
public class Column {
    private int id;
    private boolean isInt;

    public Column(int id, boolean isInt) {
        this.id = id;
        this.isInt = isInt;
    }

    public int getId() {
        return id;
    }

    public boolean isInt() {
        return isInt;
    }

}
