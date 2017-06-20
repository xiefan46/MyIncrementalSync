package com.alibaba.middleware.race.sync.model;

import com.alibaba.middleware.race.sync.Constants;

/**
 * 具体某一列变更的情况
 */
public class ColumnChangeData {
    /**
     * 变更的列的名字
     */
    private String columnName;
    /**
     * 变更的列的类型
     */
    private String columnType;
    /**
     * 是否是主键
     */
    private String isPrimaryKey;

    /**
     * 变更前的值
     */
    private String oldValue;

    /**
     * 变更后的值
     */
    private String newValue;

    public ColumnChangeData() {
        this.columnName = "";
        this.columnType = "";
        this.isPrimaryKey = "";
        this.oldValue = "";
        this.newValue = "";
    }

    public ColumnChangeData(String columnName, String columnType, String isPrimaryKey, String oldValue,
                            String newValue) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.isPrimaryKey = isPrimaryKey;
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public String isPrimaryKey() {
        return isPrimaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        isPrimaryKey = primaryKey;
    }

    public String getOldValue() {
        return oldValue;
    }

    public void setOldValue(String oldValue) {
        this.oldValue = oldValue;
    }

    public String getNewValue() {
        return newValue;
    }

    public void setNewValue(String newValue) {
        this.newValue = newValue;
    }

    /**
     * @return 按照  "列名:类型:是否主键"这样的格式来返回
     */
    @Override
    public String toString() {
        String SEP = Constants.SEP;
        //如果没更新过，就别输出了
        // if (oldValue.equals(newValue)) {
        // return "";
        // }
        return columnName + ":" + columnType + ":" + isPrimaryKey + SEP + oldValue + SEP + newValue + SEP;
    }
}