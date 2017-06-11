package com.alibaba.middleware.race.sync.model;

/**
 * @author wangkai
 *
 */
public class Column {

	private byte[]	value;

	public void setValue(byte[] bytes) {
		this.value = bytes;
	}

	public byte[] getValue() {
		return value;
	}
}
