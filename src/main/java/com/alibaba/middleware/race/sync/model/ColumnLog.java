package com.alibaba.middleware.race.sync.model;

/**
 * @author wangkai
 *
 */
public class ColumnLog {

	private byte[]		name;

	private byte[]		value;

	private boolean	update;

	public void setName(byte[] bytes, int off, int len) {
		byte[] array = new byte[len];
		System.arraycopy(bytes, off, array, 0, len);
		this.name = array;
	}

	public void setValue(byte[] bytes, int off, int len) {
		byte[] array = new byte[len];
		System.arraycopy(bytes, off, array, 0, len);
		this.value = array;
	}

	public byte[] getName() {
		return name;
	}

	public byte[] getValue() {
		return value;
	}

	public boolean isUpdate() {
		return update;
	}

	public void setUpdate(boolean update) {
		this.update = update;
	}

}
