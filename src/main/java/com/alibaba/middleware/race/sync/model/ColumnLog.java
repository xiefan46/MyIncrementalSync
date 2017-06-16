package com.alibaba.middleware.race.sync.model;

import com.alibaba.middleware.race.sync.codec.ByteArray2;

/**
 * @author wangkai
 *
 */
public class ColumnLog {

	private ByteArray2		name = new ByteArray2(null, 0, 0);

	private byte[]		value;

	public void setName(byte[] bytes, int off, int len) {
		this.name.reset(bytes, off, len);
	}

	public void setValue(byte[] bytes, int off, int len) {
		byte[] array = new byte[len];
		System.arraycopy(bytes, off, array, 0, len);
		this.value = array;
	}

	public ByteArray2 getName() {
		return name;
	}

	public byte[] getValue() {
		return value;
	}
	
	public byte[] getNameByte(){
		ByteArray2 name = this.name;
		byte [] array = new byte[name.getLen()];
		System.arraycopy(name.getArray(), name.getOff(), array, 0, name.getLen());
		return array;
	}

}
