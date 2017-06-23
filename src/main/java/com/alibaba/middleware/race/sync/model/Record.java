package com.alibaba.middleware.race.sync.model;

/**
 * @author wangkai
 */
public class Record {

	private byte		alterType;

	private int		rootPk	= -1;
	
	private boolean	inserted;
	
	private byte		insert;
	
	private byte		delete;

	private long[]		columns;

	public Record(int cols) {
		this.columns = new long[cols];
	}

	public void setColumn(int index, long data) {
		columns[index] = data;
	}

	public long[] getColumns() {
		return columns;
	}

	public byte getAlterType() {
		return alterType;
	}

	public void setAlterType(byte alterType) {
		this.alterType = alterType;
	}

	public void setColumns(long[] columns) {
		this.columns = columns;
	}

	public int getRootPk() {
		return rootPk;
	}

	public void setRootPk(int rootPk) {
		this.rootPk = rootPk;
	}

	public boolean canDelete(){
		return delete > 0 && delete == insert;
	}
	
	public void insert(){
		insert++;
	}
	
	public void delete(){
		delete++;
	}
	
	public void clear(){
		alterType = 0;
		inserted = false;
		insert = 0;
		delete = 0;
		rootPk = -1;
		for (int i = 0; i < columns.length; i++) {
			columns[i] = 0;
		}
	}

	public boolean isInserted() {
		return inserted;
	}

	public void setInserted(boolean inserted) {
		this.inserted = inserted;
	}
	

}
