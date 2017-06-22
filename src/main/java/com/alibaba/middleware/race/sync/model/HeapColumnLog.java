package com.alibaba.middleware.race.sync.model;

import com.alibaba.middleware.race.sync.codec.ByteArray2;

public class HeapColumnLog {

//	static ByteArrayCache byteArrayCache = ByteArrayCache.get();
	
	static ByteArray2 byteArray2 = new ByteArray2(null, 0, 0);
	
	private int	name;

	private long	value;

	public void setName(int name) {
		this.name = name;
	}

	public void setValue(int off, int len) {
		this.value = (off << 32) | len;
	}

	public void setValue(long value) {
		this.value = value;
	}

	public int getName() {
		return name;
	}
	
	public long getValue() {
		return value;
	}
	
	public int getValueOff(){
		return (int) (value >> 32);
	}
	
	public int getValueLen(){
		return (int) (value & 0xffffffff);
	}
	
}
