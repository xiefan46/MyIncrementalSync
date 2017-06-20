package com.alibaba.middleware.race.sync.model;

import com.alibaba.middleware.race.sync.codec.ByteArray2;

/**
 * @author wangkai
 *
 */
public class ColumnLog {

//	static ByteArrayCache byteArrayCache = ByteArrayCache.get();
	
	static ByteArray2 byteArray2 = new ByteArray2(null, 0, 0);
	
	private int	name;

	private long	value;

	public void setName(Table table,byte[] bytes, int off, int len) {
		this.name = table.getIndex(byteArray2.reset(bytes, off, len));
	}

	public void setName(int name) {
		this.name = name;
	}

	public void setValue(byte[] bytes, int off, int len) {
		if (len > 7) {
			throw new RuntimeException("len");
		}
		int end = off + len;
		long res = 0;
		for (int i = off; i < end; i++) {
			res = (res << 8) | (bytes[i] & 0xff);
		}
		this.value = res|((len * 1l) << (8 * 7));
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

	public static byte[] getByteValue(long value) {
		long v = value;
		int len = (int) (v >> (8 * 7));
		byte [] res = new byte [len];
		for (int i = len - 1; i >= 0; i--) {
			res[i] = (byte) v;
			v = v >> 8;
		}
		return res;
	}
	
	public static void main(String[] args) {
		
		
		ColumnLog c = new ColumnLog();
		
		c.setValue("test".getBytes(), 0, 4);
		
		byte [] bb = getByteValue(c.getValue());
		
		System.out.println(new String(bb));
		
	}

}
