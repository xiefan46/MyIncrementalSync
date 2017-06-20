package com.alibaba.middleware.race.sync.model;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.middleware.race.sync.Constants;

/**
 * 保存数据变更信息
 * Created by wanshao on 2017/5/8.
 */
public class ChangeRecord {

    /**
     * 唯一的字符串编号
     */
    private String id;

    /**
     * 数据变更发生的时间戳,毫秒精度,例子:1489133349000
     */
    private String timestamp;

    /**
     * 数据变更对应的库名
     */
    private String schema;

    /**
     * 数据变更对应的表名
     */
    private String table;

    /**
     * 变更类型(主要分为I/U/D)
     */
    private String type;

    /**
     * 列的变更信息的集合
     */
    private List<ColumnChangeData> changeDataList;


    public ChangeRecord(String id, String timestamp, String schema, String table, String type,
                        List<ColumnChangeData> changeDataList) {
        this.id = id;
        this.timestamp = timestamp;
        this.schema = schema;
        this.table = table;
        this.type = type;
        this.changeDataList = changeDataList;
    }

    public ChangeRecord() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<ColumnChangeData> getChangeDataList() {
        return changeDataList;
    }

    public void setChangeDataList(ArrayList<ColumnChangeData> changeDataList) {
        this.changeDataList = changeDataList;
    }

    @Override
    public String toString() {
        String SEP = Constants.SEP;
        //获取列信息list的输出
        String changeDataListStr = "";
        for (int i = 0; i < changeDataList.size(); i++) {
            changeDataListStr += changeDataList.get(i).toString();
        }

        return SEP + id + SEP + timestamp + SEP + table + SEP + type + SEP + changeDataListStr;
    }

    /**
     * 测试下toString的效果
     *
     * @param args
     */
    public static void main(String[] args) {
        ArrayList<ColumnChangeData> columnChangeDataArrayList = new ArrayList<ColumnChangeData>();
        ColumnChangeData data1 = new ColumnChangeData("id", "1", "1", "NULL", "102");
        ColumnChangeData data2 = new ColumnChangeData("name", "1", "0", "NULL", "wanshao");
        //测试下ColumnChangeData的toString方法
        columnChangeDataArrayList.add(data1);
        columnChangeDataArrayList.add(data2);
        ChangeRecord changeRecord = new ChangeRecord("1", "1489133349000", "test", "user", "I",
                columnChangeDataArrayList);
        //测试下ChangeRecord的toString方法
        System.out.println(changeRecord);
    }
}

