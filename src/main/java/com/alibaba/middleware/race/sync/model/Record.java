package com.alibaba.middleware.race.sync.model;

/**
 * @author wangkai
 */
public class Record {

	private long [] columns;

	public Record(int cols) {
		this.columns = new long[cols];
	}

	public void setColum(int index, long data) {
		columns[index] = data;
	}

	public long[] getColumns() {
		return columns;
	}

	private byte	alterType;

	private int	newId;

	public byte getAlterType() {
		return alterType;
	}

	public void setAlterType(byte alterType) {
		this.alterType = alterType;
	}

	public int getNewId() {
		return newId;
	}

	public void setNewId(int newId) {
		this.newId = newId;
	}

	public void setColumn(int index, long value) {
		columns[index] = value;
	}
}
