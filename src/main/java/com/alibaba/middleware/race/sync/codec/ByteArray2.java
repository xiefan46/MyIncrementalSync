package com.alibaba.middleware.race.sync.codec;

/**
 * @author wangkai
 *
 */
public class ByteArray2 {

	private int	hash;

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
		this.hash = 0;
		this.off = off;
		this.len = len;
		return this;
	}

	public byte[] copy(){
		byte[] bytes = new byte[len];
		System.arraycopy(array,off,bytes,0,len);
		return bytes;
	}

	@Override
	public boolean equals(Object obj) {
		return compare((ByteArray2) obj);
	}
	
	
	public boolean compare(ByteArray2 other){
		byte[] thisArray = this.array;
		byte[] otherArray = other.getArray();

		if (getLen() != otherArray.length) {
			return false;
		}
		
		int thisOff = getOff();

		for (int i = 0; i < len; i++) {
			if (otherArray[i] != thisArray[thisOff + i]) {
				return false;
			}
		}
		return true;
	}

	public int getLen() {
		return len;
	}

	public int getOff() {
		return off;
	}

	@Override
	public int hashCode() {
		int h = hash;
		if (h == 0 && array.length > 0) {
			byte val[] = array;
			int end = off + len;
			for (int i = off; i < end; i++) {
				h = 31 * h + val[i];
			}
			hash = h;
		}
		return h;
	}

}
