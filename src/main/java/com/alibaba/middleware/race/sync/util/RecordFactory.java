package com.alibaba.middleware.race.sync.util;

import com.alibaba.middleware.race.sync.model.Record;

public class RecordFactory implements VFactory<Record>{

	private int cols;
	
	public RecordFactory(int cols) {
		this.cols = cols;
	}

	@Override
	public Record newInstance() {
		return new Record(cols);
	}

	@Override
	public void clean(Record v) {
		v.clear();
	}

	
}
