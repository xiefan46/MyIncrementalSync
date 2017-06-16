package com.alibaba.middleware.race.sync.codec;

/**
 * @author wangkai
 *
 */
public class ByteArray2 {

	private int	off;

	private int	len;

	private byte[]	array;

	public ByteArray2(byte[] array, int off, int len) {
		this.array = array;
		this.off = off;
		this.len = len;
	}

	public byte[] getArray() {
		return array;
	}

	public ByteArray2 reset(byte[] array, int off, int len) {
		this.array = array;
		this.off = off;
		this.len = len;
		return this;
	}

	@Override
	public boolean equals(Object obj) {
		return ByteArray.equals((ByteArray) obj, this);
	}

	public int getLen() {
		return len;
	}

	public int getOff() {
		return off;
	}

	@Override
	public int hashCode() {
		int h = 0;
		byte val[] = array;
		int end = off + len;
		for (int i = off; i < end; i++) {
			h = 31 * h + val[i];
		}
		return h;
	}

}
