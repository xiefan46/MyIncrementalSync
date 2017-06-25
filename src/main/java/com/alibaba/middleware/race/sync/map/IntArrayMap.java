package com.alibaba.middleware.race.sync.map;

/**
 * Created by Rudy Steiner on 2017/6/25.
 */
public interface IntArrayMap {

    /*  move oldId correspond array to newId
     *  @param oldId  source id of array
     *  @param newId
     * */
     void move(int oldId,int  newId);
    /*
     *  set first byte of array be 0 indicate null array
     * */
     void removeMark(int id);
    /*
     *  set first byte of array be 1 indicate used array
     * */
     void insertMark(int id);
    /*
     *  set index column value of id array
     * */
     void setColumn(int id, byte index, byte[] bytes, int off, int len);
}
