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

}
