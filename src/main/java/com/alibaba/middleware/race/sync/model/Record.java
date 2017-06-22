package com.alibaba.middleware.race.sync.model;

import com.alibaba.middleware.race.sync.AllLog;

/**
 * @author wangkai
 */
public class Record {

	private long [] columns;

	private byte		count;

	private Integer finalPk;

	private byte alterType = 0;

	public void countDown() {
		count--;
	}

	public boolean dealOver() {
		return count <= 0;
	}

	public Integer getFinalPk() {
		return finalPk;
	}

	public void setFinalPk(Integer finalPk) {
		this.finalPk = finalPk;
	}

	public byte getAlterType() {
		return alterType;
	}
	
	public long[] getColumns() {
		return columns;
	}
	
	public void setColumns(long[] columns) {
		this.columns = columns;
	}
	
	public long getColumn(int index) {
		return columns[index];
	}
	
	public void setColumn(long off,long len,int i) {
		this.columns[i] = (off << 32) | len;
	}

	public void setAlterType(byte alterType) {
		this.alterType = alterType;
	}
	
	public void newCols(int cols){
		this.columns = new long[cols];
		this.count = (byte) cols;
	}
	
	public boolean isDelete(){
		return alterType == AllLog.DELETE;
	}
	
}
