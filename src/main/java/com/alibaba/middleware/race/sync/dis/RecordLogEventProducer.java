package com.alibaba.middleware.race.sync.dis;

import com.alibaba.middleware.race.sync.model.RecordLog;
import com.lmax.disruptor.RingBuffer;

public class RecordLogEventProducer {

	private final RingBuffer<RecordLogEvent> ringBuffer;

	public RecordLogEventProducer(RingBuffer<RecordLogEvent> ringBuffer) {
		this.ringBuffer = ringBuffer;
	}

	public void publish(RecordLog r) {
		long sequence = ringBuffer.next(); // Grab the next sequence
		try {
			RecordLogEvent event = ringBuffer.get(sequence); // Get the entry in
												// the Disruptor
												// for the sequence
			event.setRecordLog(r); // Fill with data
		} finally {
			ringBuffer.publish(sequence);
		}
	}

}
