package com.alibaba.middleware.race.sync.model;

import com.alibaba.middleware.race.sync.codec.ByteArray2;

/**
 * @author wangkai
 *
 */
public class ColumnLog {

	//	static ByteArrayCache byteArrayCache = ByteArrayCache.get();

	static ByteArray2	byteArray2	= new ByteArray2(null, 0, 0);

	private byte		name;

	private byte[]		value;

	private int		valueOff;

	private byte		valueLen;

	public void setName(Table table, byte[] bytes, int off, int len) {
		this.name = table.getIndex(byteArray2.reset(bytes, off, len));
	}

	public void setName(byte name) {
		this.name = name;
	}

	public void setValue(byte[] bytes, int off, int len) {
		this.value = bytes;
		this.valueOff = off;
		this.valueLen = (byte)len;
	}

	public int getName() {
		return name;
	}

	public byte[] getValue() {
		return value;
	}

	public int getValueOff() {
		return valueOff;
	}

	public byte getValueLen() {
		return valueLen;
	}

	public void setValue(byte[] value) {
		this.value = value;
	}

	public void setValueOff(int valueOff) {
		this.valueOff = valueOff;
	}

	public void setValueLen(byte valueLen) {
		this.valueLen = valueLen;
	}

}
