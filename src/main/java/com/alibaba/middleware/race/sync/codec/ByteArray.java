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
		return equals(this, (ByteArray2) obj);
	}
	
	public static boolean equals(ByteArray array,ByteArray2 array2){
		byte[] arrayArray = array.array;
		byte[] array2Array = array2.getArray();
		int len = array2.getLen();
		if (len != arrayArray.length) {
			return false;
		}
		int off = array2.getOff();
		for (int i = 0; i < len; i++) {
			if (arrayArray[i] != array2Array[off + i]) {
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
