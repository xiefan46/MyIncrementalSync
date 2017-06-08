package com.alibaba.middleware.race.sync.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author wangkai
 */
public class Record {

	private Map<byte[], Column>	columns;

	private PrimaryColumn		primaryColumn;
	
	private boolean			deleted;
	
	private long				lastUpdate;

	public void newColumns() {
		this.columns = new LinkedHashMap<>();
	}

	public Map<byte[], Column> getColumns() {
		return columns;
	}

	public PrimaryColumn getPrimaryColumn() {
		return primaryColumn;
	}

	public void setPrimaryColumn(PrimaryColumn primaryColumn) {
		this.primaryColumn = primaryColumn;
	}

	public void putColumn(byte [] key,Column column){
		columns.put(key, column);
	}

	public boolean isDeleted() {
		return deleted;
	}

	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}

	public long getLastUpdate() {
		return lastUpdate;
	}

	public void setLastUpdate(long lastUpdate) {
		this.lastUpdate = lastUpdate;
	}
	
}
