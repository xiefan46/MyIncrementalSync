package com.alibaba.middleware.race.sync.model;

import com.alibaba.middleware.race.sync.codec.ByteArray;

/**
 * @author wangkai
 *
 */
public class ColumnLog {

	private ByteArray	name;

	private byte[]	value;

	public void setName(byte[] bytes, int off, int len) {
		byte[] array = new byte[len];
		System.arraycopy(bytes, off, array, 0, len);
		this.name = new ByteArray(array);
	}

	public void setValue(byte[] bytes, int off, int len) {
		byte[] array = new byte[len];
		System.arraycopy(bytes, off, array, 0, len);
		this.value = array;
	}

	public ByteArray getName() {
		return name;
	}

	public byte[] getValue() {
		return value;
	}

}
