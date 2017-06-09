package com.alibaba.middleware.race.sync.model;

import java.util.LinkedHashMap;
import java.util.Map;

import com.alibaba.middleware.race.sync.codec.ByteArray;

/**
 * @author wangkai
 */
public class Record {

	private Map<ByteArray, Column>	columns;

	private PrimaryColumn		primaryColumn;
	
	public void newColumns() {
		this.columns = new LinkedHashMap<>();
	}

	public Map<ByteArray, Column> getColumns() {
		return columns;
	}

	public PrimaryColumn getPrimaryColumn() {
		return primaryColumn;
	}

	public void setPrimaryColumn(PrimaryColumn primaryColumn) {
		this.primaryColumn = primaryColumn;
	}

	public void putColumn(ByteArray key,Column column){
		columns.put(key, column);
	}

}
