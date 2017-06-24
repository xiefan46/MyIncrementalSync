package com.alibaba.middleware.race.sync;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.middleware.race.sync.database.Column;

/**
 * Created by xiefan on 6/24/17.
 */
public class ClientContext {

    private List<Column> columnList = new ArrayList<>();


    public ClientContext() {
        columnList.add(new Column(0, true));
        columnList.add(new Column(1, false));
        columnList.add(new Column(2, false));
        columnList.add(new Column(3, false));
        columnList.add(new Column(4, true));
        if (!Config.LOCAL_TEST)
            columnList.add(new Column(5, true));

    }


    public List<Column> getColumnList() {
        return columnList;
    }
}
