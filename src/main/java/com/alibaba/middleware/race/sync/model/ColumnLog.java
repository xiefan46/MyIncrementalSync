package com.alibaba.middleware.race.sync.model;

/**
 * @author wangkai
 *
 */
public class ColumnLog {

	private boolean isNumberCol;

	private byte nameIndex;

	public byte getNameIndex() {
		return nameIndex;
	}

	public void setNameIndex(byte nameIndex) {
		this.nameIndex = nameIndex;
	}

	public boolean isNumberCol() {
		return isNumberCol;
	}

	public void setNumberCol(boolean numberCol) {
		isNumberCol = numberCol;
	}
}
