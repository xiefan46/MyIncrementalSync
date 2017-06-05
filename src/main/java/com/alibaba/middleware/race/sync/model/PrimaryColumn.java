package com.alibaba.middleware.race.sync.model;

/**
 * @author wangkai
 */
public class PrimaryColumn extends Column {

	private Object beforeValue;

	public Object getBeforeValue() {
		return beforeValue;
	}

	public void setBeforeValue(Object beforeValue) {
		this.beforeValue = beforeValue;
	}

	public boolean IsPkChange() {
		return getValue().equals(beforeValue) ? false : true;
	}

}
