package com.alibaba.middleware.race.sync.model;

/**
 * @author wangkai
 */
public class Record {

	private byte[][] columns;

	public Record(int cols) {
		this.columns = new byte[cols][];
	}

	public void setColum(int index, byte[] data) {
		columns[index] = data;
	}

	public byte[][] getColumns() {
		return columns;
	}

	private byte	alterType;

	private int	newId;

	public void setColumns(byte[][] columns) {
		this.columns = columns;
	}

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

	public void setColumn(int index, byte[] value) {
		columns[index] = value;
	}
}
