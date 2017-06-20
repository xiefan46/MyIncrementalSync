package com.alibaba.middleware.race.sync.model;

import com.alibaba.middleware.race.sync.codec.ByteArray2;
import com.alibaba.middleware.race.sync.codec.ByteArrayCache;

/**
 * @author wangkai
 *
 */
public class ColumnLog {
	

	static ByteArrayCache byteArrayCache = ByteArrayCache.get();
	
	static ByteArray2 byteArray2 = new ByteArray2(null, 0, 0);
	
	private int	name;

	private byte[]	value;

	public void setName(Table table,byte[] bytes, int off, int len) {
		this.name = table.getIndex(byteArray2.reset(bytes, off, len));
	}

	public void setName(int name) {
		this.name = name;
	}

	public void setValue(byte[] bytes, int off, int len) {
		this.value = byteArrayCache.get(byteArray2.reset(bytes, off, len));
	}

	public void setValue(byte[] bytes) {
		this.value = bytes;
	}

	public int getName() {
		return name;
	}

	public byte[] getValue() {
		return value;
	}

}
