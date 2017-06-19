package com.alibaba.middleware.race.sync.dis;

import com.lmax.disruptor.EventFactory;

public class RecordLogEventFactory implements EventFactory<RecordLogEvent> {
	public RecordLogEvent newInstance() {
		return new RecordLogEvent();
	}
}
