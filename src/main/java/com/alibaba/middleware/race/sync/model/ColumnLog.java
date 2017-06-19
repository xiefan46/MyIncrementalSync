package com.alibaba.middleware.race.sync.model;

/**
 * @author wangkai
 *
 */
public class ColumnLog {


	private byte nameIndex;

	private short value;



	public byte getNameIndex() {
		return nameIndex;
	}

	public void setNameIndex(byte nameIndex) {
		this.nameIndex = nameIndex;
	}

	public short getValue() {
		return value;
	}

	public void setValue(short value) {
		this.value = value;
	}
}
