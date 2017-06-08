package com.alibaba.middleware.race.sync.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author wangkai
 */
public class Record {

	private Map<byte[], Column>	columns;

	private PrimaryColumn		primaryColumn;

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

}
