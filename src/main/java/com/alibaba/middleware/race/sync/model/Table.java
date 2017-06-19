package com.alibaba.middleware.race.sync.model;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.middleware.race.sync.codec.ByteArray2;

/**
 * @author wangkai
 *
 */
public class Table {

	private final ThreadLocal<ByteArray2>	arrayLocal	= new ThreadLocal<ByteArray2>() {
													@Override
													protected ByteArray2 initialValue() {
														return new ByteArray2(null, 0,
																0);
													}
												};

	private Map<ByteArray2, Byte>			colNameToId	= new HashMap<>();

	private static short				globalId		= 0;

	private Map<ByteArray2, Short>		colValueToId	= new HashMap<>();

	private Map<Short, ByteArray2>		idToColValue	= new HashMap<>();

	private byte						colSize		= 0;

	public Table() {

	}

	public void addCol(byte[] buffer, int offset, int length) {
		byte[] bytes = new byte[length];
		System.arraycopy(buffer, offset, bytes, 0, length);
		ByteArray2 array = new ByteArray2(bytes, 0, bytes.length);
		colNameToId.put(array, colSize++);
	}

	public byte getColNameId(byte[] array, int offset, int length) {
		ByteArray2 array2 = arrayLocal.get().reset(array, offset, length);
		Byte id = colNameToId.get(array2);
		if (id == null) {
			throw new RuntimeException("Fail to encode col name");
		}
		return id;
	}

	public short getColValueId(byte[] array, int offset, int length) {
		ByteArray2 array2 = arrayLocal.get().reset(array, offset, length);
		Short id = colValueToId.get(array2);
		if (id == null) {
			id = globalId++;
			byte[] bytes = new byte[length];
			System.arraycopy(array, offset, bytes, 0, length);
			colValueToId.put(new ByteArray2(bytes, 0, bytes.length), id);
			idToColValue.put(id, new ByteArray2(bytes, 0, bytes.length));
		}
		return id;
	}

	public byte getColSize() {
		return colSize;
	}

	public ByteArray2 getColArrayById(short id) {
		return idToColValue.get(id);
	}

	public short[] newRecord() {
		return new short[colSize];
	}

	public Map<ByteArray2, Byte> getColNameToId() {
		return colNameToId;
	}

	public void setColNameToId(Map<ByteArray2, Byte> colNameToId) {
		this.colNameToId = colNameToId;
	}

	public Map<ByteArray2, Short> getColValueToId() {
		return colValueToId;
	}

	public void setColValueToId(Map<ByteArray2, Short> colValueToId) {
		this.colValueToId = colValueToId;
	}
}
