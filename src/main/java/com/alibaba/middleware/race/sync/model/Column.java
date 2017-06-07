package com.alibaba.middleware.race.sync.model;

/**
 * @author wangkai
 *
 */
public class Column {

	private static final byte	NUMBER_FLAG	= 0b01000000;
	private static final byte	PRIMARY_FLAG	= (byte) 0b10000000;
	// 列名
	private String				name;
	// 类型 类型主要分为1和2 1代表为数字类型，
	// 数字类型范围为0<= x <= 2^64-1 
	// 2代表为字符串类型，0<= len <= 65536
	// 类型:是否主键 是否主键:0或者1 (0代表否，1代表是) 
	private byte				flag;
	// 列值
	private Object				value;
	
	private byte []			nameBytes;
	
	private byte []			valueBytes;

	public String getName() {
		if (name == null) {
			name = new String(nameBytes);
		}
		return name;
	}

	public void setName(byte [] bytes,int off,int len) {
		byte [] array = new byte[len];
		System.arraycopy(bytes, off, array, 0, len);
		this.nameBytes = array;
	}

	public boolean isNumber() {
		return (flag & NUMBER_FLAG) != 0;
	}

	public void setNumber(boolean number) {
		if (number) {
			flag = (byte) (flag | NUMBER_FLAG);
		}
	}

	public boolean isPrimary() {
		return (flag & PRIMARY_FLAG) != 0;
	}

	public void setPrimary(boolean primary) {
		if (primary) {
			flag = (byte) (flag | PRIMARY_FLAG);
		}
	}

	public Object getValue() {
		if (!isNumber()) {
			if (value == null) {
				value = new String(valueBytes);
			}
		}
		return value;
	}
	
	public void setValue(byte [] bytes,int off,int len) {
		byte [] array = new byte[len];
		System.arraycopy(bytes, off, array, 0, len);
		this.valueBytes = array;
	}

	public void setValue(byte [] bytes,int off,int len,long value) {
		this.setValue(bytes, off, len);
		this.value = value;
	}

	public byte getFlag() {
		return flag;
	}

	public byte[] getNameBytes() {
		return nameBytes;
	}

	public byte[] getValueBytes() {
		return valueBytes;
	}
	
}
