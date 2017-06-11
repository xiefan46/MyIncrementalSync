package com.alibaba.middleware.race.sync.codec;

/**
 * @author wangkai
 *
 */
public class ByteArray {

	private int	hash;

	private byte[]	array;

	public ByteArray(byte[] array) {
		this.array = array;
	}

	public byte[] getArray() {
		return array;
	}
	
	public ByteArray reset(byte [] array){
		this.array = array;
		this.hash = 0;
		return this;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}

		if (obj == this) {
			return true;
		}

		byte[] other = ((ByteArray) obj).getArray();
		byte[] _this = this.array;

		if (other.length != _this.length) {
			return false;
		}

		for (int i = 0; i < _this.length; i++) {
			if (other[i] != _this[i]) {
				return false;
			}
		}

		return true;
	}

	@Override
	public int hashCode() {
		int h = hash;
		if (h == 0 && array.length > 0) {
			byte val[] = array;
			for (int i = 0; i < array.length; i++) {
				h = 31 * h + val[i];
			}
			hash = h;
		}
		return h;
	}

}
